import logging
import importlib
import re
from typing import Any

logger = logging.getLogger(__name__)


TYPE_CHECKERS = {
    "str": lambda value: isinstance(value, str),
    "bool": lambda value: isinstance(value, bool),
    "int": lambda value: isinstance(value, int) and not isinstance(value, bool),
    "dict": lambda value: isinstance(value, dict),
    "str_list": lambda value: isinstance(value, list) and all(isinstance(item, str) for item in value),
}


CONFIG_SCHEMA: dict[str, Any] = {
    "required": ["id", "mail_connection_id", "requirements", "action"],
    "optional": ["description", "enabled", "mailbox", "match_mode", "safety"],
    "types": {
        "id": "str",
        "description": "str",
        "enabled": "bool",
        "mail_connection_id": "str",
        "mailbox": "str",
        "match_mode": "str",
        "requirements": "dict",
        "action": "dict",
        "safety": "dict",
    },
    "enum": {"match_mode": ["all", "any"]},
    "children": {
        "requirements": "requirements",
        "action": "action",
        "safety": "safety",
    },
}


SCHEMA_REGISTRY: dict[str, dict[str, Any]] = {
    "requirements": {
        "required": [],
        "optional": ["subject", "from", "flags", "age", "attachments"],
        "types": {
            "subject": "dict",
            "from": "dict",
            "flags": "dict",
            "age": "dict",
            "attachments": "dict",
        },
        "at_least_one": ["subject", "from", "flags", "age", "attachments"],
        "children": {
            "subject": "subject",
            "from": "from",
            "flags": "flags",
            "age": "age",
            "attachments": "attachments",
        },
    },
    "subject": {
        "required": [],
        "optional": ["contains_any", "contains_all", "regex"],
        "types": {
            "contains_any": "str_list",
            "contains_all": "str_list",
            "regex": "str_list",
        },
        "at_least_one": ["contains_any", "contains_all", "regex"],
        "mutually_exclusive": [["contains_any", "regex"]],
    },
    "from": {
        "required": [],
        "optional": ["match", "not_match", "regex"],
        "types": {
            "match": "str_list",
            "not_match": "str_list",
            "regex": "str_list",
        },
        "at_least_one": ["match", "not_match", "regex"],
        "mutually_exclusive": [["match", "regex"]],
    },
    "flags": {
        "required": [],
        "optional": ["include_all", "include_any", "exclude_any", "exclude_all"],
        "types": {
            "include_all": "str_list",
            "include_any": "str_list",
            "exclude_any": "str_list",
            "exclude_all": "str_list",
        },
        "at_least_one": ["include_all", "include_any", "exclude_any", "exclude_all"],
    },
    "age": {
        "required": [],
        "optional": ["older_than_days", "older_than_hours", "newer_than_days", "newer_than_hours"],
        "types": {
            "older_than_days": "int",
            "older_than_hours": "int",
            "newer_than_days": "int",
            "newer_than_hours": "int",
        },
        "at_least_one": ["older_than_days", "older_than_hours", "newer_than_days", "newer_than_hours"],
        "mutually_exclusive": [
            ["older_than_days", "older_than_hours"],
            ["newer_than_days", "newer_than_hours"],
        ],
    },
    "attachments": {
        "required": [],
        "optional": ["has_attachments", "type", "name"],
        "types": {
            "has_attachments": "bool",
            "type": "str_list",
            "name": "dict",
        },
        "at_least_one": ["has_attachments", "type", "name"],
        "children": {"name": "attachment_name"},
    },
    "attachment_name": {
        "required": [],
        "optional": ["contains_any", "contains_all", "regex"],
        "types": {
            "contains_any": "str_list",
            "contains_all": "str_list",
            "regex": "str_list",
        },
        "at_least_one": ["contains_any", "contains_all", "regex"],
        "mutually_exclusive": [["contains_any", "regex"]],
    },
    "action": {
        "required": ["type"],
        "optional": ["target_mailbox"],
        "types": {
            "type": "str",
            "target_mailbox": "str",
        },
        "enum": {"type": ["delete", "move"]},
        "conditional_required": [
            {
                "if_key": "type",
                "if_value": "move",
                "required": ["target_mailbox"],
            }
        ],
    },
    "safety": {
        "required": [],
        "optional": ["dry_run", "max_messages_per_run", "min_age_for_delete_days"],
        "types": {
            "dry_run": "bool",
            "max_messages_per_run": "int",
            "min_age_for_delete_days": "int",
        },
    },
}


def validate_config(config: dict[str, Any]) -> tuple[bool, str]:
    """
    Validate one mailbox cleaner job configuration.
    """
    valid, msg = _strip_dict_keys_in_place(config, path="config")
    if not valid:
        return False, msg

    valid, msg = _validate_schema_node(config, CONFIG_SCHEMA, "config")
    if not valid:
        return False, msg

    valid, msg = _validate_semantic_rules(config)
    if not valid:
        return False, msg

    valid, msg = _validate_airflow_imap_config(config["mail_connection_id"])
    if not valid:
        return False, msg

    return True, "OK"


def _validate_schema_node(payload: Any, schema: dict[str, Any], path: str) -> tuple[bool, str]:
    """
    Validate one dictionary node against schema rules and recurse into child nodes.
    """
    if not isinstance(payload, dict):
        msg = f"{path} must be a dictionary, got {type(payload).__name__}"
        return False, msg

    required = set(schema.get("required", []))
    optional = set(schema.get("optional", []))
    allowed = required | optional

    missing_keys = [key for key in required if key not in payload]
    if missing_keys:
        msg = f"Missing required keys in {path}: {', '.join(sorted(missing_keys))}"
        return False, msg

    unknown_keys = [key for key in payload if key not in allowed]
    if unknown_keys:
        visible_unknown_keys = ", ".join(repr(key) for key in sorted(unknown_keys))
        msg = f"Unknown keys in {path}: {visible_unknown_keys}"
        return False, msg

    for key, type_name in schema.get("types", {}).items():
        if key not in payload:
            continue
        checker = TYPE_CHECKERS[type_name]
        if not checker(payload[key]):
            msg = f"Invalid type for {path}.{key}. Expected {type_name}"
            return False, msg

    for key, valid_values in schema.get("enum", {}).items():
        if key in payload and payload[key] not in valid_values:
            msg = f"Invalid value for {path}.{key}: {payload[key]!r}. Expected one of: {', '.join(valid_values)}"
            return False, msg

    at_least_one = schema.get("at_least_one", [])
    if at_least_one and not any(key in payload for key in at_least_one):
        msg = f"At least one of these keys is required in {path}: {', '.join(at_least_one)}"
        return False, msg

    for conflict_group in schema.get("mutually_exclusive", []):
        present = [key for key in conflict_group if key in payload]
        if len(present) > 1:
            msg = f"Conflicting keys in {path}: {', '.join(present)}"
            return False, msg

    for rule in schema.get("conditional_required", []):
        trigger_key = rule["if_key"]
        trigger_value = rule["if_value"]
        if payload.get(trigger_key) != trigger_value:
            continue

        missing = [key for key in rule["required"] if key not in payload]
        if missing:
            msg = (
                f"Missing required keys in {path} when {trigger_key}={trigger_value!r}: "
                f"{', '.join(missing)}"
            )
            return False, msg

    for key, child_name in schema.get("children", {}).items():
        if key not in payload:
            continue

        child_schema = SCHEMA_REGISTRY[child_name]
        valid, msg = _validate_schema_node(payload[key], child_schema, f"{path}.{key}")
        if not valid:
            return False, msg

    return True, "OK"


def _validate_semantic_rules(config: dict[str, Any]) -> tuple[bool, str]:
    """
    Validate cross-field rules that are easier to express outside the schema tree.
    """
    requirements = config.get("requirements", {})
    subject_regexes = requirements.get("subject", {}).get("regex", [])
    sender_regexes = requirements.get("from", {}).get("regex", [])
    filename_regexes = requirements.get("attachments", {}).get("name", {}).get("regex", [])

    valid, msg = _validate_regex_list(subject_regexes, "config.requirements.subject.regex")
    if not valid:
        return False, msg

    valid, msg = _validate_regex_list(sender_regexes, "config.requirements.from.regex")
    if not valid:
        return False, msg

    valid, msg = _validate_regex_list(filename_regexes, "config.requirements.attachments.name.regex")
    if not valid:
        return False, msg

    action = config.get("action", {})
    safety = config.get("safety", {})
    age = requirements.get("age", {})

    if action.get("type") == "delete":
        if "min_age_for_delete_days" not in safety:
            msg = "config.safety.min_age_for_delete_days is required when action.type='delete'"
            return False, msg

        min_age_days = safety["min_age_for_delete_days"]
        older_than_days = age.get("older_than_days")
        older_than_hours = age.get("older_than_hours")

        if older_than_days is None and older_than_hours is None:
            msg = "Delete action requires requirements.age.older_than_days or requirements.age.older_than_hours"
            return False, msg

        if older_than_days is not None and older_than_days < min_age_days:
            msg = (
                "requirements.age.older_than_days must be greater than or equal to "
                "safety.min_age_for_delete_days for delete action"
            )
            return False, msg

        if older_than_hours is not None and older_than_hours < min_age_days * 24:
            msg = (
                "requirements.age.older_than_hours must be greater than or equal to "
                "safety.min_age_for_delete_days * 24 for delete action"
            )
            return False, msg

    return True, "OK"


def _validate_regex_list(regex_patterns: list[str], path: str) -> tuple[bool, str]:
    """
    Validate that all regex patterns compile.
    """
    for pattern in regex_patterns:
        try:
            re.compile(pattern)
        except re.error as exc:
            msg = f"Invalid regex in {path}: {pattern!r} ({exc})"
            return False, msg

    return True, "OK"


def _validate_airflow_imap_config(connection_id: str) -> tuple[bool, str]:
    """
    Validate that the Airflow IMAP connection exists and has the required fields.
    """
    try:
        airflow_base_module = importlib.import_module("airflow.hooks.base")
        base_hook = getattr(airflow_base_module, "BaseHook")
    except Exception as exc:
        msg = f"Failed to import Airflow BaseHook for connection validation: {exc}"
        return False, msg

    try:
        conn = base_hook.get_connection(connection_id)
    except Exception as exc:
        msg = f"Failed to retrieve Airflow connection '{connection_id}': {exc}"
        return False, msg

    if not conn.host or not conn.login or not conn.password:
        msg = f"Airflow connection '{connection_id}' is missing required fields (host, login, password)"
        return False, msg

    return True, "OK"


def _strip_dict_keys_in_place(payload: Any, path: str) -> tuple[bool, str]:
    """
    Strip surrounding whitespace from all dictionary keys recursively.

    This prevents subtle key mismatches caused by copied JSON with invisible
    whitespace. If two different keys collapse into the same stripped key,
    validation fails with a conflict error.
    """
    if isinstance(payload, dict):
        normalized: dict[Any, Any] = {}

        for raw_key, value in payload.items():
            normalized_key = raw_key.strip() if isinstance(raw_key, str) else raw_key

            if normalized_key in normalized:
                msg = (
                    f"Conflicting keys in {path} after stripping whitespace: "
                    f"{raw_key!r} conflicts with {normalized_key!r}"
                )
                return False, msg

            child_path = f"{path}.{normalized_key}" if isinstance(normalized_key, str) else path
            valid, msg = _strip_dict_keys_in_place(value, path=child_path)
            if not valid:
                return False, msg

            normalized[normalized_key] = value

        payload.clear()
        payload.update(normalized)
        return True, "OK"

    if isinstance(payload, list):
        for index, item in enumerate(payload):
            valid, msg = _strip_dict_keys_in_place(item, path=f"{path}[{index}]")
            if not valid:
                return False, msg

    return True, "OK"
