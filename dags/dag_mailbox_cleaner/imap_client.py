from __future__ import annotations

import imaplib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import parsedate_to_datetime


@dataclass
class ImapFetchedMessage:
    uid: bytes
    message: EmailMessage
    flags: set[str]
    internal_date: datetime | None


class ImapClient:
    """
    Small IMAP client wrapper focused on mailbox-cleaner operations.
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 143,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._conn: imaplib.IMAP4 | None = None
        self._mailbox: str | None = None
        self._supports_move = False
        self._has_pending_expunge = False

    def __enter__(self) -> "ImapClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def connect(self) -> None:
        if self._conn is not None:
            return

        conn = imaplib.IMAP4(host=self.host, port=self.port)
        conn.starttls()
        status, _ = conn.login(self.username, self.password)
        if status != "OK":
            raise ConnectionError("IMAP login failed")

        self._conn = conn
        capabilities = conn.capabilities or ()
        normalized_caps = {
            cap.decode("utf-8", errors="ignore").upper() if isinstance(cap, bytes) else str(cap).upper()
            for cap in capabilities
        }
        self._supports_move = "MOVE" in normalized_caps

    def close(self) -> None:
        if self._conn is None:
            return

        try:
            self._conn.logout()
        finally:
            self._conn = None
            self._mailbox = None
            self._has_pending_expunge = False

    def select_mailbox(self, mailbox: str, readonly: bool = False) -> None:
        conn = self._ensure_connection()
        status, _ = conn.select(mailbox, readonly=readonly)
        if status != "OK":
            raise ConnectionError(f"Failed to select mailbox: {mailbox}")
        self._mailbox = mailbox

    def search_uids(
        self,
        criteria: str = "ALL",
        max_results: int | None = None,
        newest_first: bool = True,
    ) -> list[bytes]:
        conn = self._ensure_selected_mailbox()

        if not criteria or not criteria.strip():
            raise ValueError("IMAP search criteria must be a non-empty string")

        status, data = conn.uid("SEARCH", None, criteria)
        if status != "OK":
            raise ConnectionError(f"Failed to search mailbox with criteria: {criteria}")

        if not data or not data[0]:
            return []

        uids = data[0].split()
        if newest_first:
            uids.reverse()

        if max_results is not None:
            uids = uids[:max_results]

        return uids

    def fetch_message(self, uid: bytes) -> ImapFetchedMessage:
        conn = self._ensure_selected_mailbox()

        uid_arg = _uid_to_str(uid)
        # BODY.PEEK[] avoids changing \Seen state while still returning full message bytes.
        status, data = conn.uid("FETCH", uid_arg, "(BODY.PEEK[] FLAGS INTERNALDATE)")
        if status != "OK":
            raise ConnectionError(f"Failed to fetch message UID={uid_arg}")

        raw_message: bytes | None = None

        for item in data or []:
            if isinstance(item, tuple) and len(item) >= 2:
                raw_message = item[1] if isinstance(item[1], bytes) else b""
                break

        if raw_message is None:
            raise ConnectionError(f"Message payload missing for UID={uid_arg}")

        parsed_message = BytesParser(policy=policy.default).parsebytes(raw_message)
        parsed_message.uid = uid

        metadata = _collect_fetch_metadata_bytes(data)
        flags = _parse_flags_from_fetch_metadata(metadata)
        internal_date = _parse_internal_date_from_fetch_metadata(metadata)

        # Some IMAP servers split FLAGS/INTERNALDATE outside the first FETCH tuple.
        # If metadata is still missing, do a lightweight follow-up FETCH for metadata only.
        if not flags or internal_date is None:
            status, metadata_only_data = conn.uid("FETCH", uid_arg, "(FLAGS INTERNALDATE)")
            if status == "OK":
                metadata_only = _collect_fetch_metadata_bytes(metadata_only_data)
                if not flags:
                    flags = _parse_flags_from_fetch_metadata(metadata_only)
                if internal_date is None:
                    internal_date = _parse_internal_date_from_fetch_metadata(metadata_only)

        return ImapFetchedMessage(
            uid=uid,
            message=parsed_message,
            flags=flags,
            internal_date=internal_date,
        )

    def move_email(self, uid: bytes, target_mailbox: str) -> None:
        if not target_mailbox or not target_mailbox.strip():
            raise ValueError("target_mailbox must be a non-empty string")

        conn = self._ensure_selected_mailbox()
        uid_arg = _uid_to_str(uid)

        if self._supports_move:
            status, _ = conn.uid("MOVE", uid_arg, target_mailbox)
            if status == "OK":
                return

        status, _ = conn.uid("COPY", uid_arg, target_mailbox)
        if status != "OK":
            raise ConnectionError(f"Failed to copy UID={uid_arg} to mailbox '{target_mailbox}'")

        status, _ = conn.uid("STORE", uid_arg, "+FLAGS", "\\Deleted")
        if status != "OK":
            raise ConnectionError(f"Failed to mark UID={uid_arg} as deleted after copy")

        self._has_pending_expunge = True

    def delete_email(self, uid: bytes) -> None:
        conn = self._ensure_selected_mailbox()

        uid_arg = _uid_to_str(uid)
        status, _ = conn.uid("STORE", uid_arg, "+FLAGS", "\\Deleted")
        if status != "OK":
            raise ConnectionError(f"Failed to mark UID={uid_arg} as deleted")

        self._has_pending_expunge = True

    def expunge_if_needed(self) -> None:
        if not self._has_pending_expunge:
            return

        conn = self._ensure_selected_mailbox()
        status, _ = conn.expunge()
        if status != "OK":
            raise ConnectionError("Failed to expunge mailbox")

        self._has_pending_expunge = False

    def _ensure_connection(self) -> imaplib.IMAP4:
        if self._conn is None:
            raise RuntimeError("IMAP connection is not open")
        return self._conn

    def _ensure_selected_mailbox(self) -> imaplib.IMAP4:
        conn = self._ensure_connection()
        if self._mailbox is None:
            raise RuntimeError("No mailbox selected")
        return conn


def _uid_to_str(uid: bytes | str) -> str:
    if isinstance(uid, bytes):
        return uid.decode("utf-8", errors="ignore")
    return str(uid)


def _collect_fetch_metadata_bytes(fetch_data: object) -> bytes:
    """
    Collect all metadata byte fragments returned by imaplib FETCH responses.

    Different IMAP servers may return metadata partly in tuple headers and partly
    in standalone bytes entries.
    """
    fragments: list[bytes] = []

    if not isinstance(fetch_data, list):
        return b""

    for item in fetch_data:
        if isinstance(item, tuple):
            if item and isinstance(item[0], bytes):
                fragments.append(item[0])
        elif isinstance(item, bytes):
            fragments.append(item)

    return b" ".join(fragments)


def _parse_flags_from_fetch_metadata(metadata: bytes) -> set[str]:
    match = re.search(rb"FLAGS \((.*?)\)", metadata)
    if not match:
        return set()

    raw_flags = match.group(1).split()
    return {flag.decode("utf-8", errors="ignore") for flag in raw_flags}


def _parse_internal_date_from_fetch_metadata(metadata: bytes) -> datetime | None:
    match = re.search(rb'INTERNALDATE "([^"]+)"', metadata)
    if not match:
        return None

    raw_date = match.group(1).decode("utf-8", errors="ignore")

    try:
        parsed = parsedate_to_datetime(raw_date)
    except (TypeError, ValueError):
        return None

    if parsed is None:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed
