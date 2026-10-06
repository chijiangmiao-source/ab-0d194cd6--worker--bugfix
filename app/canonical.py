"""Canonical JSON normalization.

Business-equivalent payloads (key order, whitespace, integral floats, unicode
composition) must collapse to the same canonical form so that dedup compares
meaning, not bytes.
"""
import hashlib
import json
import unicodedata


def _normalize_str(value):
    return unicodedata.normalize("NFC", value)


def _normalize(obj):
    if isinstance(obj, dict):
        return {_normalize_str(k): _normalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize(v) for v in obj]
    if isinstance(obj, str):
        return _normalize_str(obj)
    if isinstance(obj, bool) or obj is None or isinstance(obj, int):
        return obj
    if isinstance(obj, float):
        if obj != obj or obj in (float("inf"), float("-inf")):
            raise ValueError("non-finite numbers are not allowed")
        if obj.is_integer():
            return int(obj)
        return obj
    raise TypeError("unsupported type in payload: %r" % type(obj).__name__)


def canonical(obj):
    """Deterministic canonical JSON string for a JSON-like object."""
    return json.dumps(
        _normalize(obj),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def digest_of(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
