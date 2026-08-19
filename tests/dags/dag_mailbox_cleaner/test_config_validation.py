from __future__ import annotations

import dag_mailbox_cleaner.config_validation as validation


def _base_config() -> dict:
    return {
        "id": "invoices_cleanup",
        "enabled": True,
        "mail_connection_id": "mailbox_cleaner_test_imap",
        "mailbox": "INBOX",
        "match_mode": "all",
        "requirements": {
            "flags": {
                "include_all": ["\\Seen"],
            },
            "age": {
                "older_than_days": 0,
            },
        },
        "action": {
            "type": "delete",
        },
        "safety": {
            "dry_run": True,
            "max_messages_per_run": 200,
            "min_age_for_delete_days": 0,
        },
    }


def test_validate_config_strips_whitespace_keys(monkeypatch) -> None:
    monkeypatch.setattr(validation, "_validate_airflow_imap_config", lambda _: (True, "OK"))

    config = _base_config()
    config["requirements"]["age"] = {"older_than_days ": 0}

    valid, msg = validation.validate_config(config)

    assert valid is True
    assert msg == "OK"
    assert config["requirements"]["age"]["older_than_days"] == 0
    assert "older_than_days " not in config["requirements"]["age"]


def test_validate_config_accepts_age_key_without_whitespace(monkeypatch) -> None:
    monkeypatch.setattr(validation, "_validate_airflow_imap_config", lambda _: (True, "OK"))

    valid, msg = validation.validate_config(_base_config())

    assert valid is True
    assert msg == "OK"


def test_validate_config_rejects_conflicting_keys_after_strip(monkeypatch) -> None:
    monkeypatch.setattr(validation, "_validate_airflow_imap_config", lambda _: (True, "OK"))

    config = _base_config()
    config["requirements"]["age"] = {
        "older_than_days": 1,
        " older_than_days ": 2,
    }

    valid, msg = validation.validate_config(config)

    assert valid is False
    assert "Conflicting keys" in msg
    assert "stripping whitespace" in msg


def test_validate_config_rejects_archive_action_type(monkeypatch) -> None:
    monkeypatch.setattr(validation, "_validate_airflow_imap_config", lambda _: (True, "OK"))

    config = _base_config()
    config["action"]["type"] = "archive"

    valid, msg = validation.validate_config(config)

    assert valid is False
    assert "Invalid value for config.action.type" in msg


def test_validate_config_accepts_safety_sort_asc(monkeypatch) -> None:
    monkeypatch.setattr(validation, "_validate_airflow_imap_config", lambda _: (True, "OK"))

    config = _base_config()
    config["safety"]["sort"] = "asc"

    valid, msg = validation.validate_config(config)

    assert valid is True
    assert msg == "OK"


def test_validate_config_rejects_invalid_safety_sort(monkeypatch) -> None:
    monkeypatch.setattr(validation, "_validate_airflow_imap_config", lambda _: (True, "OK"))

    config = _base_config()
    config["safety"]["sort"] = "middle"

    valid, msg = validation.validate_config(config)

    assert valid is False
    assert "Invalid value for config.safety.sort" in msg
