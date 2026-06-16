import logging
import xml.etree.ElementTree as ET
from io import BytesIO

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from rkdigi.email_handling import EmailReader

logger = logging.getLogger(__name__)

_FILE_PREFIX = "EksporteredeOrdrer_"
_FILE_SLOTS = 10


def _allocate_filename(sftp_client) -> str:
    """Allocate the next available filename on the SFTP server in the format "EksporteredeOrdrer_XX.xml"."""
    existing = set(sftp_client.listdir("."))
    for i in range(1, _FILE_SLOTS + 1):
        filename = f"{_FILE_PREFIX}{i:02d}.xml"
        if filename not in existing:
            return filename
    raise RuntimeError(
        f"All {_FILE_SLOTS} filename slots are occupied on the SFTP server."
    )


def _mark_email_seen(email_reader: EmailReader, uid: bytes, mailbox: str = "INBOX") -> None:
    """Mark a single email as seen via UID."""
    uid_text = uid.decode(errors="ignore")
    if not uid_text:
        logger.warning("Skipping mark-as-seen because email UID is missing.")
        return

    marked, _ = email_reader.get_emails(
        mailbox=mailbox,
        criteria=f"UID {uid_text}",
        set_flags="\\Seen",
        max=1,
    )
    if not marked:
        logger.warning(f"Failed to mark email UID {uid!r} as seen.")


def process_kantinedata(sftp_hook: SFTPHook) -> None:
    """Fetch unseen emails, extract XML attachments, validate, and upload to SFTP."""
    imap_conn = BaseHook.get_connection("kantinedata_imap")
    imap_host = imap_conn.host
    imap_port = imap_conn.port
    imap_extra = imap_conn.extra_dejson or {}

    if not imap_host:
        raise AirflowFailException("Connection 'kantinedata_imap' is missing host. Cannot initialize EmailReader.")

    if not imap_port:
        raise AirflowFailException("Connection 'kantinedata_imap' is missing port. Cannot initialize EmailReader.")

    if imap_extra.get("use_ssl"):
        raise AirflowFailException(
            "Connection 'kantinedata_imap' has use_ssl=true. rkdigi EmailReader uses IMAP + STARTTLS and does not support IMAP SSL mode. "
            "Set use_ssl=false with STARTTLS-compatible endpoint, or switch back to Airflow ImapHook for SSL mode."
        )

    email_reader = EmailReader(
        email=imap_conn.login,
        password=imap_conn.password,
        imap_server=imap_host,
        imap_port=int(imap_port),
    )

    emails, failed = email_reader.get_emails(
        mailbox="INBOX",
        criteria="UNSEEN",
        set_flags=None,
    )

    logger.info(f"{len(emails)} email(s) found")
    if failed:
        logger.warning(f"Failed to fetch {len(failed)} email(s): {failed}")

    found_emails_without_xml_attachment = False

    if len(emails) > 0:
        with sftp_hook.get_conn() as sftp_client:
            for message in emails:
                uid = getattr(message, "uid", b"")
                all_uploaded = True
                xml_found = False

                for part in message.iter_attachments():
                    content_type = part.get_content_type()
                    if content_type not in ("application/xml", "text/xml"):
                        continue

                    xml_found = True
                    payload = part.get_payload(decode=True)
                    if not payload:
                        logger.warning(f"Skipping empty XML attachment in msg UID {uid!r}")
                        all_uploaded = False
                        continue

                    try:
                        # Verify that the payload is valid XML before uploading.
                        ET.parse(BytesIO(payload))
                    except ET.ParseError as e:
                        logger.warning(f"Skipping invalid XML in msg UID {uid!r}: {e}")
                        all_uploaded = False
                        continue

                    filename = _allocate_filename(sftp_client)
                    sftp_client.putfo(BytesIO(payload), filename)
                    logger.info(f"Uploaded {filename} to SFTP")

                if xml_found and all_uploaded:
                    # Only mark as seen when at least one XML attachment was uploaded successfully.
                    _mark_email_seen(email_reader=email_reader, uid=uid, mailbox="INBOX")
                elif not xml_found:
                    found_emails_without_xml_attachment = True

    if found_emails_without_xml_attachment:
        raise RuntimeError("Found one or more emails without valid XML attachments. Please check the email contents.")
