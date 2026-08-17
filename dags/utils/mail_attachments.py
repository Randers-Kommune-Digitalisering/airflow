from typing import Iterable, Sequence
import logging
from rkdigi.email_handling import EmailReader

logger = logging.getLogger(__name__)


def find_latest_attachment(
    email_reader: EmailReader,
    mailbox: str = "INBOX",
    criteria: str = "UNSEEN",
    extensions: Sequence[str] = (".xlsx",),
    filename_prefixes: Iterable[str] | None = None,
    max_emails: int = 50,
) -> tuple[bytes, str, bytes] | None:
    """
    Find newest attachment matching extension and optional filename prefixes.
    Returns (uid, filename, content_bytes) or None.
    """
    emails, failed = email_reader.get_emails(
        mailbox=mailbox,
        criteria=criteria,
        set_flags=None,
        max=max_emails,
        low_to_high=False,
    )

    logger.info(f"Fetched {len(emails)} email(s), {len(failed)} failed to fetch.")

    normalized_ext = tuple(ext.lower() for ext in extensions)
    normalized_prefixes = (
        tuple(p.lower() for p in filename_prefixes)
        if filename_prefixes
        else None
    )

    for msg in emails:
        uid: bytes = getattr(msg, "uid", None)

        for part in msg.iter_attachments():
            filename = part.get_filename() or ""
            filename_lc = filename.lower()

            if not filename_lc.endswith(normalized_ext):
                continue

            if normalized_prefixes and not any(
                filename_lc.startswith(prefix) for prefix in normalized_prefixes
            ):
                continue

            content = part.get_payload(decode=True)
            if content:
                return uid, filename, content

    return None
