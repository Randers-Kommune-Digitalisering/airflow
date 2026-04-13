import email
import logging
import xml.etree.ElementTree as ET
from email.message import Message
from io import BytesIO

from airflow.providers.imap.hooks.imap import ImapHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

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


def process_kantinedata(imap_hook: ImapHook, sftp_hook: SFTPHook) -> None:
    """Main function to process Kantinedata: fetch unseen emails, extract XML attachments, validate and upload to SFTP."""
    imap_hook.get_conn()
    with imap_hook.mail_client as conn, sftp_hook.get_conn() as sftp_client:
        conn.select("INBOX")
        status, data = conn.search(None, "UNSEEN")

        if status != "OK":
            logger.warning(f"IMAP search failed with status: {status}")
            return

        msg_ids = data[0].split()
        logger.info(f"{len(msg_ids)} email(s) found")
        found_emails_without_xml_attachment = False

        for msg_id in msg_ids:
            status, msg_data = conn.fetch(msg_id, "(BODY.PEEK[])")  # Get the email without marking it as read
            if status != "OK":
                logger.warning(f"Failed to fetch email with ID {msg_id}: {status}")
                continue
            message: Message = email.message_from_bytes(msg_data[0][1])

            all_uploaded = True
            xml_found = False
            for part in message.walk():
                if part.get_content_disposition() != "attachment":
                    continue
                if part.get_content_type() not in ("application/xml", "text/xml"):
                    continue

                xml_found = True
                payload = part.get_payload(decode=True)
                try:
                    # Verify that the payload is valid XML before uploading
                    ET.parse(BytesIO(payload))
                except ET.ParseError as e:
                    logger.warning(f"Skipping invalid XML in msg {msg_id}: {e}")
                    all_uploaded = False
                    continue

                filename = _allocate_filename(sftp_client)
                sftp_client.putfo(BytesIO(payload), filename)
                logger.info(f"Uploaded {filename} to SFTP")

            if xml_found and all_uploaded:
                # Only mark the email as seen if we found at least one XML attachment and all were uploaded successfully.
                conn.store(msg_id, "+FLAGS", "\\Seen")
            elif not xml_found:
                found_emails_without_xml_attachment = True

        if found_emails_without_xml_attachment:
            # If we encountered any emails that were missing valid XML attachments, raise an error at the end.
            raise RuntimeError("Found one or more emails without valid XML attachments. Please check the email contents.")
