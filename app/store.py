"""SQLite persistence: frozen decisions, stages, leases, artifacts, journal, faults.

All multi-statement mutations run inside a single IMMEDIATE transaction so a
submission decision (normalized input + rules snapshot + journal) is persisted
atomically. Connections run in autocommit mode; use `immediate()` for groups.
"""
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from . import config
from .canonical import canonical, digest_of

STAGES = ("RECEIVED", "PROCESSING", "STAGED", "PUBLISHED")
_STAGE_RANK = {stage: rank for rank, stage in enumerate(STAGES)}

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  version INTEGER NOT NULL,
  body TEXT NOT NULL,
  digest TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS exports (
  export_id TEXT PRIMARY KEY,
  decision_hash TEXT NOT NULL,
  input_digest TEXT NOT NULL,
  rules_digest TEXT NOT NULL,
  rules_version INTEGER NOT NULL,
  records TEXT NOT NULL,
  rules_snapshot TEXT NOT NULL,
  receipt_id TEXT NOT NULL,
  received_at TEXT NOT NULL,
  stage TEXT NOT NULL CHECK (stage IN ('RECEIVED','PROCESSING','STAGED','PUBLISHED')),
  attempts INTEGER NOT NULL DEFAULT 0,
  artifact_digest TEXT,
  artifact_path TEXT,
  published_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  export_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('staged','published','aborted')),
  path TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_artifacts_published
  ON artifacts(export_id) WHERE kind = 'published';
CREATE TABLE IF NOT EXISTS leases (
  resource TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  fencing INTEGER NOT NULL,
  expires_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  export_id TEXT,
  actor TEXT NOT NULL,
  event TEXT NOT NULL,
  detail TEXT,
  ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_journal_export ON journal(export_id);
CREATE TABLE IF NOT EXISTS faults (
  export_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL
);
"""

DEFAULT_RULES = {"rules": [{"field": "vessel_id", "action": "hash", "length": 12}]}

FAULT_MODES = ("crash_partial_write", "crash_after_staged", "pause_before_tmp_write")


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def connect(db_path=None):
    path = db_path or config.db_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None  # autocommit; explicit transactions via immediate()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def immediate(conn):
    """Single atomic IMMEDIATE transaction (the 'persistent decision' unit)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db(conn):
    conn.executescript(SCHEMA)
    body = canonical(DEFAULT_RULES)
    conn.execute(
        "INSERT OR IGNORE INTO rules(id, version, body, digest, updated_at) VALUES (1, 1, ?, ?, ?)",
        (body, digest_of(body), utcnow()),
    )
    conn.commit()


def journal(conn, export_id, actor, event, detail=None):
    conn.execute(
        "INSERT INTO journal(export_id, actor, event, detail, ts) VALUES (?,?,?,?,?)",
        (export_id, actor, event, detail, utcnow()),
    )


# ---------------------------------------------------------------- rules

def get_rules(conn):
    row = conn.execute("SELECT version, body, digest, updated_at FROM rules WHERE id = 1").fetchone()
    return dict(row)


def set_rules(conn, rules_doc, actor="api"):
    body = canonical(rules_doc)
    digest = digest_of(body)
    with immediate(conn):
        conn.execute(
            "UPDATE rules SET version = version + 1, body = ?, digest = ?, updated_at = ? WHERE id = 1",
            (body, digest, utcnow()),
        )
        row = conn.execute("SELECT version, body, digest, updated_at FROM rules WHERE id = 1").fetchone()
        journal(conn, None, actor, "rules_updated", "version=%d digest=%s" % (row["version"], digest))
    return dict(row)


# ---------------------------------------------------------------- exports

def _receipt(row, replay):
    return {
        "export_id": row["export_id"],
        "receipt_id": row["receipt_id"],
        "received_at": row["received_at"],
        "stage": row["stage"],
        "input_digest": row["input_digest"],
        "rules_digest": row["rules_digest"],
        "rules_version": row["rules_version"],
        "artifact_digest": row["artifact_digest"],
        "replay": replay,
    }


def submit_export(conn, export_id, records_obj, actor="api"):
    """Freeze normalized input + current rules snapshot in one transaction.

    Returns (status, payload): 201 new decision, 200 idempotent replay of the
    first receipt, 409 conflict (original evidence preserved untouched).
    """
    records_canon = canonical(records_obj)
    input_digest = digest_of(records_canon)
    now = utcnow()
    receipt_id = "rcpt-" + uuid.uuid4().hex[:16]
    inserted = False
    with immediate(conn):
        # Read the current rules and freeze the decision under the same write
        # lock, so a concurrent rules update serializes before or after the
        # whole adjudication, never in the middle of it.
        rules = get_rules(conn)
        decision_hash = digest_of(input_digest + ":" + rules["digest"])
        cur = conn.execute(
            """INSERT INTO exports(export_id, decision_hash, input_digest, rules_digest,
                   rules_version, records, rules_snapshot, receipt_id, received_at,
                   stage, attempts, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,'RECEIVED',0,?,?)
               ON CONFLICT(export_id) DO NOTHING""",
            (
                export_id, decision_hash, input_digest, rules["digest"], rules["version"],
                records_canon, rules["body"], receipt_id, now, now, now,
            ),
        )
        if cur.rowcount == 1:
            journal(
                conn, export_id, actor, "received",
                "input_digest=%s rules_digest=%s rules_version=%d"
                % (input_digest, rules["digest"], rules["version"]),
            )
            inserted = True
    if inserted:
        return 201, _receipt(get_export(conn, export_id), replay=False)

    existing = get_export(conn, export_id)
    if existing["decision_hash"] == decision_hash:
        return 200, _receipt(existing, replay=True)

    with immediate(conn):
        journal(
            conn, export_id, actor, "conflict_rejected",
            "submitted_input=%s submitted_rules=%s" % (input_digest, rules["digest"]),
        )
    return 409, {
        "error": "conflict",
        "message": "export_id is already frozen with a different decision; original evidence preserved",
        "export_id": export_id,
        "submitted": {"input_digest": input_digest, "rules_digest": rules["digest"]},
        "existing": {
            "input_digest": existing["input_digest"],
            "rules_digest": existing["rules_digest"],
            "receipt_id": existing["receipt_id"],
            "received_at": existing["received_at"],
            "stage": existing["stage"],
        },
    }


def get_export(conn, export_id):
    row = conn.execute("SELECT * FROM exports WHERE export_id = ?", (export_id,)).fetchone()
    return dict(row) if row else None


def list_exports(conn, limit=200):
    rows = conn.execute(
        "SELECT * FROM exports ORDER BY created_at DESC, export_id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(row) for row in rows]


def export_events(conn, export_id, limit=50):
    rows = conn.execute(
        "SELECT ts, actor, event, detail FROM journal WHERE export_id = ? ORDER BY id DESC LIMIT ?",
        (export_id, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def get_lease(conn, resource):
    row = conn.execute("SELECT resource, owner, fencing, expires_at FROM leases WHERE resource = ?",
                       (resource,)).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------- stages

def cas_stage(conn, export_id, new_stage, allowed_from):
    """Compare-and-swap a stage. Forward-only; PUBLISHED is terminal and must
    never appear in allowed_from, so a published export can never regress."""
    assert new_stage in _STAGE_RANK
    assert "PUBLISHED" not in allowed_from
    assert all(_STAGE_RANK[new_stage] > _STAGE_RANK[stage] for stage in allowed_from)
    placeholders = ",".join("?" for _ in allowed_from)
    cur = conn.execute(
        "UPDATE exports SET stage = ?, updated_at = ? WHERE export_id = ? AND stage IN (%s)" % placeholders,
        (new_stage, utcnow(), export_id, *allowed_from),
    )
    return cur.rowcount == 1


def requeue(conn, export_id, actor, reason):
    """Recovery-only reset of an unfinished export back to RECEIVED."""
    cur = conn.execute(
        "UPDATE exports SET stage = 'RECEIVED', attempts = attempts + 1, updated_at = ? "
        "WHERE export_id = ? AND stage IN ('PROCESSING','STAGED')",
        (utcnow(), export_id),
    )
    if cur.rowcount == 1:
        journal(conn, export_id, actor, "requeued", reason)
        return True
    return False


def mark_published(conn, export_id, digest, path, actor, via):
    """Terminal transition; returns False when already published (no regression)."""
    if not cas_stage(conn, export_id, "PUBLISHED", ("RECEIVED", "PROCESSING", "STAGED")):
        journal(conn, export_id, actor, "publish_skipped", "already published")
        return False
    conn.execute(
        "UPDATE exports SET artifact_digest = ?, artifact_path = ?, published_at = ? WHERE export_id = ?",
        (digest, path, utcnow(), export_id),
    )
    journal(conn, export_id, actor, "published", "digest=%s via=%s" % (digest, via))
    return True


# ---------------------------------------------------------------- leases

def acquire_lease(conn, resource, owner, ttl_seconds):
    """Take a lease or steal an expired one. Returns the fencing token or None."""
    now = time.time()
    with immediate(conn):
        conn.execute(
            """INSERT INTO leases(resource, owner, fencing, expires_at) VALUES (?,?,1,?)
               ON CONFLICT(resource) DO UPDATE
                 SET owner = excluded.owner, fencing = leases.fencing + 1,
                     expires_at = excluded.expires_at
               WHERE leases.expires_at < ?""",
            (resource, owner, now + ttl_seconds, now),
        )
        row = conn.execute("SELECT owner, fencing FROM leases WHERE resource = ?", (resource,)).fetchone()
    if row and row["owner"] == owner:
        return row["fencing"]
    return None


def check_lease(conn, resource, owner, fencing):
    row = conn.execute(
        "SELECT 1 FROM leases WHERE resource = ? AND owner = ? AND fencing = ? AND expires_at > ?",
        (resource, owner, fencing, time.time()),
    ).fetchone()
    return row is not None


def release_lease(conn, resource, owner, fencing):
    with immediate(conn):
        conn.execute(
            "DELETE FROM leases WHERE resource = ? AND owner = ? AND fencing = ?",
            (resource, owner, fencing),
        )


# ---------------------------------------------------------------- artifacts

def record_artifact(conn, export_id, kind, path, digest):
    """Record an artifact. A second 'published' row per export is ignored
    (partial unique index) so an export can be published at most once."""
    cur = conn.execute(
        """INSERT INTO artifacts(export_id, kind, path, digest, created_at)
           VALUES (?,?,?,?,?)
           ON CONFLICT(export_id) WHERE kind = 'published' DO NOTHING""",
        (export_id, kind, path, digest, utcnow()),
    )
    return cur.rowcount == 1


def staged_artifacts(conn, export_id):
    rows = conn.execute(
        "SELECT * FROM artifacts WHERE export_id = ? AND kind = 'staged' ORDER BY id DESC",
        (export_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def published_artifacts(conn, export_id):
    rows = conn.execute(
        "SELECT * FROM artifacts WHERE export_id = ? AND kind = 'published'", (export_id,)
    ).fetchall()
    return [dict(row) for row in rows]


def abort_artifact(conn, artifact_id):
    conn.execute("UPDATE artifacts SET kind = 'aborted' WHERE id = ? AND kind = 'staged'", (artifact_id,))


# ---------------------------------------------------------------- work queues

def stuck_exports(conn):
    rows = conn.execute(
        "SELECT export_id, stage FROM exports WHERE stage IN ('PROCESSING','STAGED') ORDER BY updated_at"
    ).fetchall()
    return [dict(row) for row in rows]


def next_received(conn):
    row = conn.execute(
        "SELECT export_id FROM exports WHERE stage = 'RECEIVED' ORDER BY created_at, export_id LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------- faults (test hooks)

def set_fault(conn, export_id, mode, actor="api"):
    assert mode in FAULT_MODES
    with immediate(conn):
        conn.execute(
            "INSERT INTO faults(export_id, mode) VALUES (?,?) ON CONFLICT(export_id) DO UPDATE SET mode = excluded.mode",
            (export_id, mode),
        )
        journal(conn, export_id, actor, "fault_armed", mode)


def pop_fault(conn, export_id, mode):
    """One-shot: consume the fault only when it matches the requested mode."""
    with immediate(conn):
        row = conn.execute("SELECT mode FROM faults WHERE export_id = ?", (export_id,)).fetchone()
        if row and row["mode"] == mode:
            conn.execute("DELETE FROM faults WHERE export_id = ?", (export_id,))
            return True
    return False
