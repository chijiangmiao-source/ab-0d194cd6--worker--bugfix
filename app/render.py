"""Deterministic artifact rendering from the frozen decision.

The artifact bytes depend only on the frozen decision (normalized records,
rules snapshot, receipt timestamp), never on wall-clock time, so recovery can
recompute the expected digest and converge to the very same artifact.
"""
import json

from .canonical import canonical
from .masking import apply_rules

GENERATOR = "track-export/1.0"


def render_artifact_bytes(export_row):
    records = json.loads(export_row["records"])
    rules_doc = json.loads(export_row["rules_snapshot"])
    masked = apply_rules(records, rules_doc)
    doc = {
        "export_id": export_row["export_id"],
        "received_at": export_row["received_at"],
        "input_digest": export_row["input_digest"],
        "rules_digest": export_row["rules_digest"],
        "rules_version": export_row["rules_version"],
        "generator": GENERATOR,
        "record_count": len(masked),
        "records": masked,
    }
    return (canonical(doc) + "\n").encode("utf-8")
