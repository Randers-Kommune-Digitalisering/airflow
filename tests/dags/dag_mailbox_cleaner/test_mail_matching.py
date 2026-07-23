from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

from dag_mailbox_cleaner.mail_matching import evaluate_email_match


def _build_message(
    subject: str,
    sender: str,
    attachments: list[tuple[str, bytes]] | None = None,
) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message.set_content("Body")

    for filename, payload in attachments or []:
        subtype = filename.rsplit(".", 1)[1] if "." in filename else "octet-stream"
        message.add_attachment(
            payload,
            maintype="application",
            subtype=subtype,
            filename=filename,
        )

    return message


def test_match_mode_all_success() -> None:
    now = datetime(2026, 7, 22, tzinfo=timezone.utc)
    message = _build_message("Invoice reminder", "sender@example.com")

    config = {
        "match_mode": "all",
        "requirements": {
            "subject": {"contains_any": ["Invoice"]},
            "from": {"regex": [".*@example\\.com"]},
            "age": {"older_than_days": 20},
        },
    }

    is_match, group_results = evaluate_email_match(
        config=config,
        message=message,
        flags={"\\Seen"},
        internal_date=now - timedelta(days=30),
        now=now,
    )

    assert is_match is True
    assert group_results == {"subject": True, "from": True, "age": True}


def test_match_mode_all_fails_when_one_group_fails() -> None:
    now = datetime(2026, 7, 22, tzinfo=timezone.utc)
    message = _build_message("Invoice reminder", "sender@example.com")

    config = {
        "match_mode": "all",
        "requirements": {
            "subject": {"contains_any": ["Invoice"]},
            "from": {"match": ["other@example.com"]},
            "age": {"older_than_days": 20},
        },
    }

    is_match, group_results = evaluate_email_match(
        config=config,
        message=message,
        flags={"\\Seen"},
        internal_date=now - timedelta(days=30),
        now=now,
    )

    assert is_match is False
    assert group_results["subject"] is True
    assert group_results["from"] is False


def test_match_mode_any_succeeds_with_single_group() -> None:
    now = datetime(2026, 7, 22, tzinfo=timezone.utc)
    message = _build_message("General notice", "sender@example.com")

    config = {
        "match_mode": "any",
        "requirements": {
            "subject": {"contains_any": ["Invoice"]},
            "from": {"regex": [".*@example\\.com"]},
        },
    }

    is_match, group_results = evaluate_email_match(
        config=config,
        message=message,
        flags=set(),
        internal_date=now,
        now=now,
    )

    assert is_match is True
    assert group_results == {"subject": False, "from": True}


def test_attachment_filters_match_expected_files() -> None:
    now = datetime(2026, 7, 22, tzinfo=timezone.utc)
    message = _build_message(
        "Payment",
        "sender@example.com",
        attachments=[("invoice_2026.pdf", b"x"), ("notes.txt", b"y")],
    )

    config = {
        "match_mode": "all",
        "requirements": {
            "attachments": {
                "has_attachments": True,
                "type": ["pdf"],
                "name": {"regex": ["invoice_.*"]},
            }
        },
    }

    is_match, group_results = evaluate_email_match(
        config=config,
        message=message,
        flags=set(),
        internal_date=now,
        now=now,
    )

    assert is_match is True
    assert group_results == {"attachments": True}


def test_flags_exclude_any_blocks_match() -> None:
    now = datetime(2026, 7, 22, tzinfo=timezone.utc)
    message = _build_message("Invoice", "sender@example.com")

    config = {
        "match_mode": "all",
        "requirements": {
            "flags": {
                "include_all": ["\\Seen"],
                "exclude_any": ["\\Flagged"],
            }
        },
    }

    is_match, group_results = evaluate_email_match(
        config=config,
        message=message,
        flags={"\\Seen", "\\Flagged"},
        internal_date=now,
        now=now,
    )

    assert is_match is False
    assert group_results == {"flags": False}


def test_flags_include_unseen_matches_when_seen_flag_absent() -> None:
    now = datetime(2026, 7, 22, tzinfo=timezone.utc)
    message = _build_message("Invoice", "sender@example.com")

    config = {
        "match_mode": "all",
        "requirements": {
            "flags": {
                "include_all": ["\\Unseen"],
            }
        },
    }

    is_match, group_results = evaluate_email_match(
        config=config,
        message=message,
        flags={"\\Flagged"},
        internal_date=now,
        now=now,
    )

    assert is_match is True
    assert group_results == {"flags": True}


def test_flags_include_unseen_fails_when_seen_flag_present() -> None:
    now = datetime(2026, 7, 22, tzinfo=timezone.utc)
    message = _build_message("Invoice", "sender@example.com")

    config = {
        "match_mode": "all",
        "requirements": {
            "flags": {
                "include_all": ["\\Unseen"],
            }
        },
    }

    is_match, group_results = evaluate_email_match(
        config=config,
        message=message,
        flags={"\\Seen"},
        internal_date=now,
        now=now,
    )

    assert is_match is False
    assert group_results == {"flags": False}
