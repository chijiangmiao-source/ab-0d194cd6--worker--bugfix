"""Field masking rules engine.

A rules document looks like:

    {"rules": [
        {"field": "depth_m",   "action": "redact", "replacement": "***"},
        {"field": "lat",       "action": "round",  "precision": 2},
        {"field": "vessel_id", "action": "hash",   "length": 12},
        {"field": "note",      "action": "drop"}
    ]}

Fields are dot-separated paths into each submitted record.
"""
import copy
import hashlib
import re

from .canonical import canonical

ACTIONS = ("redact", "round", "hash", "drop")
MAX_RULES = 50

_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


def validate_rules(doc):
    """Return a list of validation errors (empty when the document is valid)."""
    errors = []
    if not isinstance(doc, dict):
        return ["rules document must be a JSON object"]
    rules = doc.get("rules")
    if not isinstance(rules, list) or not rules:
        return ["'rules' must be a non-empty array"]
    if len(rules) > MAX_RULES:
        return ["at most %d rules are allowed" % MAX_RULES]
    seen = set()
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            errors.append("rule[%d] must be an object" % i)
            continue
        field = rule.get("field")
        action = rule.get("action")
        if not isinstance(field, str) or not _FIELD_RE.match(field):
            errors.append("rule[%d].field must be a dot path of identifiers" % i)
        elif field in seen:
            errors.append("rule[%d].field %r is duplicated" % (i, field))
        else:
            seen.add(field)
        if action not in ACTIONS:
            errors.append("rule[%d].action must be one of %s" % (i, "/".join(ACTIONS)))
            continue
        if action == "round":
            precision = rule.get("precision", 2)
            if not isinstance(precision, int) or isinstance(precision, bool) or not 0 <= precision <= 6:
                errors.append("rule[%d].precision must be an integer in 0..6" % i)
        elif action == "hash":
            length = rule.get("length", 12)
            if not isinstance(length, int) or isinstance(length, bool) or not 4 <= length <= 64:
                errors.append("rule[%d].length must be an integer in 4..64" % i)
        elif action == "redact":
            if not isinstance(rule.get("replacement", "***"), str):
                errors.append("rule[%d].replacement must be a string" % i)
    return errors


def apply_rules(records, rules_doc):
    """Return masked copies of records; inputs are never mutated."""
    rules = rules_doc.get("rules", []) if isinstance(rules_doc, dict) else []
    out = []
    for record in records:
        masked = copy.deepcopy(record)
        for rule in rules:
            _apply_one(masked, rule)
        out.append(masked)
    return out


def _apply_one(record, rule):
    path = rule["field"].split(".")
    node = record
    for segment in path[:-1]:
        if not isinstance(node, dict) or segment not in node:
            return
        node = node[segment]
    if not isinstance(node, dict):
        return
    key = path[-1]
    if key not in node:
        return
    action = rule["action"]
    if action == "drop":
        del node[key]
    elif action == "redact":
        node[key] = rule.get("replacement", "***")
    elif action == "round":
        value = node[key]
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            node[key] = round(float(value), rule.get("precision", 2))
    elif action == "hash":
        length = rule.get("length", 12)
        node[key] = hashlib.sha256(canonical(node[key]).encode("utf-8")).hexdigest()[:length]
