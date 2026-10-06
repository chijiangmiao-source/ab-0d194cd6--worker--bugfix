"""Runtime configuration, read from the environment on every access (test-friendly)."""
import os


def data_dir():
    return os.environ.get("DATA_DIR", "./data")


def db_path():
    return os.path.join(data_dir(), "app.db")


def artifacts_dir():
    return os.path.join(data_dir(), "artifacts")


def tmp_dir():
    return os.path.join(artifacts_dir(), "tmp")


def published_dir():
    return os.path.join(artifacts_dir(), "published")


def quarantine_dir():
    return os.path.join(artifacts_dir(), "quarantine")


def port():
    return int(os.environ.get("PORT", "8080"))


def lease_ttl():
    return float(os.environ.get("LEASE_TTL_SECONDS", "10"))


def poll_interval():
    return float(os.environ.get("POLL_INTERVAL_SECONDS", "0.5"))


def test_hooks():
    """Fault-injection endpoints are only available when explicitly enabled."""
    return os.environ.get("TEST_HOOKS", "") == "1"


def fault_pause_timeout_seconds():
    """Bound for the pause_before_tmp_write fault: how long the frozen worker
    waits to observe the lease handover. Default comfortably exceeds the
    takeover window (poll interval + recovery)."""
    return float(os.environ.get("FAULT_PAUSE_TIMEOUT_SECONDS", "60"))


def ensure_dirs():
    for path in (data_dir(), tmp_dir(), published_dir(), quarantine_dir()):
        os.makedirs(path, exist_ok=True)
