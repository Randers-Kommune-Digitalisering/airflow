
from email.header import decode_header
import re
import imaplib
import email as email_module
from airflow.hooks.base import BaseHook


_UID_RE = re.compile(rb"\bUID (?P<uid>\d+)\b")


def imap_get_emails_with_uids(
    *,
    mailbox: str = "INBOX",
    criteria: str = "ALL",
    set_flags: str | None = "\\Seen",
    del_flags: str | None = None,
    max: int | None = None,
) -> tuple[list[tuple[str, "email_module.message.Message"]], list[bytes]]:
    """Fetch emails and their IMAP UID.

    Returns:
      - list of (uid, email_message)
      - list of sequence ids (bytes) that failed to fetch

    Notes:
      - We fetch using message sequence numbers returned by SEARCH.
      - UID is extracted from FETCH response (UID RFC822).
    """
    imap_conn = BaseHook.get_connection("kantinedata_imap")
    with imaplib.IMAP4(host=imap_conn.host, port=imap_conn.port) as server:
        server.starttls()
        server.login(imap_conn.login, imap_conn.password)
        server.select(mailbox)

        status, data = server.search(None, criteria)
        if status != "OK":
            raise ConnectionError("Failed to search emails.")

        all_ids = (data[0] or b"").split()
        email_ids = all_ids[:max] if max is not None else all_ids

        emails: list[tuple[str, "email_module.message.Message"]] = []
        failed_to_fetch_ids: list[bytes] = []

        for email_id in email_ids:
            status, msg_data = server.fetch(email_id, "(UID RFC822)")
            if status != "OK":
                failed_to_fetch_ids.append(email_id)
                continue

            uid: str | None = None
            msg = None
            for part in msg_data:
                if not isinstance(part, tuple) or len(part) != 2:
                    continue
                meta, raw = part
                if isinstance(meta, bytes):
                    m = _UID_RE.search(meta)
                    if m:
                        uid = m.group("uid").decode("ascii", errors="replace")
                if isinstance(raw, (bytes, bytearray)):
                    msg = email_module.message_from_bytes(bytes(raw))
                    break

            if uid is None or msg is None:
                failed_to_fetch_ids.append(email_id)
                continue

            emails.append((uid, msg))
            if set_flags:
                server.store(email_id, "+FLAGS", set_flags)
            if del_flags:
                server.store(email_id, "-FLAGS", del_flags)

        return emails, failed_to_fetch_ids


def extract_attachments(mail) -> list[dict]:
    """Extract attachments from an EmailMessage or raw RFC822 email.

    Returns a list of dicts:
      - filename: str | None
      - content_bytes: bytes
      - content_type: str
      - content_disposition: str | None
    """
    if mail is None:
        return []

    msg = mail
    if isinstance(mail, (bytes, bytearray)):
        msg = email_module.message_from_bytes(bytes(mail))
    elif isinstance(mail, str):
        msg = email_module.message_from_string(mail)

    if not hasattr(msg, "walk"):
        raise TypeError(f"Unsupported mail type for attachment extraction: {type(mail)!r}")

    attachments: list[dict] = []
    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue

        filename = part.get_filename()
        content_disposition = part.get_content_disposition()  # 'attachment', 'inline', or None

        is_attachment = content_disposition == "attachment" or bool(filename)
        if not is_attachment:
            continue

        decoded_filename = decode_mime_word(filename) if filename else None
        content_bytes = part.get_payload(decode=True) or b""
        content_type = part.get_content_type() or "application/octet-stream"

        attachments.append(
            {
                "filename": decoded_filename,
                "content_bytes": content_bytes,
                "content_type": content_type,
                "content_disposition": content_disposition,
            }
        )

    return attachments


def decode_mime_word(value: str | None) -> str | None:
    if value is None:
        return None
    decoded_parts: list[str] = []
    for part, charset in decode_header(value):
        if isinstance(part, bytes):
            decoded_parts.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded_parts.append(part)
    return "".join(decoded_parts)
