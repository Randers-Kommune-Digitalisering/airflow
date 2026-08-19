from __future__ import annotations

from datetime import datetime, timezone
from email.message import EmailMessage

from airflow.hooks.base import BaseHook

import dag_mailbox_cleaner.process_mailbox_cleaner as process_module
from dag_mailbox_cleaner.imap_client import ImapFetchedMessage


class _FakeConnection:
    host = "imap.example.com"
    login = "mailbox@example.com"
    password = "secret"
    port = 143


class _FakeImapClient:
    def __init__(self, host: str, port: int, username: str, password: str) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.moved_uids: list[tuple[bytes, str]] = []
        self.deleted_uids: list[bytes] = []
        self.expunge_called = False
        self.search_uids_calls: list[dict[str, object]] = []

    def __enter__(self) -> "_FakeImapClient":
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:  # type: ignore[no-untyped-def]
        return None

    def select_mailbox(self, mailbox: str, readonly: bool = False) -> None:
        _ = readonly
        self.mailbox = mailbox

    def search_uids(self, criteria: str = "ALL", max_results: int | None = None, newest_first: bool = True) -> list[bytes]:
        self.search_uids_calls.append(
            {
                "criteria": criteria,
                "max_results": max_results,
                "newest_first": newest_first,
            }
        )
        return [b"101"]

    def fetch_message(self, uid: bytes) -> ImapFetchedMessage:
        message = EmailMessage()
        message["Subject"] = "Invoice follow-up"
        message["From"] = "sender@example.com"
        message.set_content("Body")
        return ImapFetchedMessage(
            uid=uid,
            message=message,
            flags={"\\Seen"},
            internal_date=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )

    def move_email(self, uid: bytes, target_mailbox: str) -> None:
        self.moved_uids.append((uid, target_mailbox))

    def delete_email(self, uid: bytes) -> None:
        self.deleted_uids.append(uid)

    def expunge_if_needed(self) -> None:
        self.expunge_called = True


def _config(dry_run: bool) -> dict:
    return {
        "id": "clean_invoices",
        "enabled": True,
        "mail_connection_id": "mailbox_cleaner_demo_imap",
        "mailbox": "INBOX",
        "match_mode": "all",
        "requirements": {
            "subject": {"contains_any": ["Invoice"]},
            "from": {"regex": [".*@example\\.com"]},
        },
        "action": {
            "type": "move",
            "target_mailbox": "Archive/Finance",
        },
        "safety": {
            "dry_run": dry_run,
            "max_messages_per_run": 10,
            "min_age_for_delete_days": 14,
        },
    }


def _patch_fake_imap_client(monkeypatch):
    created: dict[str, _FakeImapClient] = {}

    def _factory(host: str, port: int, username: str, password: str) -> _FakeImapClient:
        client = _FakeImapClient(host=host, port=port, username=username, password=password)
        created["client"] = client
        return client

    monkeypatch.setattr(process_module, "ImapClient", _factory)
    return created


def test_process_mailbox_cleaner_dry_run_has_no_writes(monkeypatch) -> None:
    monkeypatch.setattr(BaseHook, "get_connection", lambda *_args, **_kwargs: _FakeConnection())
    created = _patch_fake_imap_client(monkeypatch)

    process_module.process_mailbox_cleaner(config=_config(dry_run=True))

    fake_client = created["client"]
    assert fake_client.moved_uids == []
    assert fake_client.deleted_uids == []


def test_process_mailbox_cleaner_move_executes_write(monkeypatch) -> None:
    monkeypatch.setattr(BaseHook, "get_connection", lambda *_args, **_kwargs: _FakeConnection())
    created = _patch_fake_imap_client(monkeypatch)

    process_module.process_mailbox_cleaner(config=_config(dry_run=False))

    fake_client = created["client"]
    assert fake_client.moved_uids == [(b"101", "Archive/Finance")]


def test_process_mailbox_cleaner_delete_executes_write(monkeypatch) -> None:
    monkeypatch.setattr(BaseHook, "get_connection", lambda *_args, **_kwargs: _FakeConnection())
    created = _patch_fake_imap_client(monkeypatch)
    config = _config(dry_run=False)
    config["action"] = {"type": "delete"}
    config["requirements"]["age"] = {"older_than_days": 14}
    process_module.process_mailbox_cleaner(config=config)
    fake_client = created["client"]
    assert fake_client.deleted_uids == [b"101"]
    assert fake_client.expunge_called is True


def test_process_mailbox_cleaner_uses_desc_sort_by_default(monkeypatch) -> None:
    monkeypatch.setattr(BaseHook, "get_connection", lambda *_args, **_kwargs: _FakeConnection())
    created = _patch_fake_imap_client(monkeypatch)

    process_module.process_mailbox_cleaner(config=_config(dry_run=True))

    fake_client = created["client"]
    assert fake_client.search_uids_calls[0]["newest_first"] is True


def test_process_mailbox_cleaner_uses_asc_sort_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(BaseHook, "get_connection", lambda *_args, **_kwargs: _FakeConnection())
    created = _patch_fake_imap_client(monkeypatch)
    config = _config(dry_run=True)
    config["safety"]["sort"] = "asc"

    process_module.process_mailbox_cleaner(config=config)

    fake_client = created["client"]
    assert fake_client.search_uids_calls[0]["newest_first"] is False
