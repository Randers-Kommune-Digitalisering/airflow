from __future__ import annotations

from dag_mailbox_cleaner.imap_client import ImapClient


class _FakeImap4:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.capabilities = ()
        self.uid_calls: list[tuple[str, str, str]] = []

    def starttls(self) -> None:
        return None

    def login(self, username: str, password: str):
        _ = username
        _ = password
        return "OK", [b"logged in"]

    def select(self, mailbox: str, readonly: bool = False):
        _ = mailbox
        _ = readonly
        return "OK", [b"1"]

    def uid(self, command: str, uid_arg: str, fetch_spec: str):
        self.uid_calls.append((command, uid_arg, fetch_spec))

        if command == "FETCH" and fetch_spec == "(BODY.PEEK[] FLAGS INTERNALDATE)":
            return "OK", [(b'1 (FLAGS () INTERNALDATE "23-Jul-2026 08:20:00 +0000" BODY[] {0}', b"")]

        if command == "FETCH" and fetch_spec == "(FLAGS INTERNALDATE)":
            return "OK", [(b'1 (FLAGS () INTERNALDATE "23-Jul-2026 08:20:00 +0000")', b"")]

        return "OK", [b""]

    def logout(self) -> None:
        return None


def test_fetch_message_uses_body_peek(monkeypatch) -> None:
    fake_conn = _FakeImap4(host="imap.example.com", port=143)

    monkeypatch.setattr("imaplib.IMAP4", lambda host, port: fake_conn)

    client = ImapClient(
        host="imap.example.com",
        port=143,
        username="mailbox@example.com",
        password="secret",
    )

    with client:
        client.select_mailbox("INBOX")
        client.fetch_message(b"100")

    assert ("FETCH", "100", "(BODY.PEEK[] FLAGS INTERNALDATE)") in fake_conn.uid_calls
