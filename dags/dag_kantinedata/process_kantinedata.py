import logging
import io
import errno

from airflow.providers.sftp.hooks.sftp import SFTPHook
try:
    from airflow.models import Variable
except Exception:
    Variable = None
from dag_kantinedata.mail_utils import imap_get_emails_with_uids, extract_attachments, decode_mime_word

logger = logging.getLogger(__name__)


_KANTINEDATA_FILE_COUNTER_VAR_KEY = "kantinedata_file_counter"


def _safe_int(value: object, default: int = 0) -> int:
    """
    Safely convert a value to an integer, returning a default if conversion fails.

    :param value: The value to convert (e.g. from Airflow Variable).
    :param default: The default integer to return if conversion fails.
    :return: The converted integer or the default.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _sftp_path_exists(sftp_client: SFTPHook, remote_path: str) -> bool:
    """
    Check if a remote path exists on the SFTP server.

    :param sftp_client: An active SFTP client connection.
    :param remote_path: The remote path to check (e.g. "/EksportedeOrdrer_1.xml").
    :return: True if the path exists, False otherwise.
    """
    try:
        sftp_client.stat(remote_path)
        return True
    except FileNotFoundError:
        return False
    except OSError as e:
        if getattr(e, "errno", None) in {errno.ENOENT, 2}:
            return False
        raise


def _allocate_next_filename(sftp_client: SFTPHook) -> str:
    """Allocate the next monotonic Kantinedata filename.

    Uses an Airflow Variable as the source of truth and increments it.
    Also checks the SFTP destination for collisions (e.g. if the variable was reset).

    :param sftp_client: An active SFTP client connection.
    :return: The allocated filename (e.g. "EksportedeOrdrer_1.xml").
    """
    if Variable is None:
        raise RuntimeError(
            "Airflow Variable is not available."
        )

    current_raw = Variable.get(_KANTINEDATA_FILE_COUNTER_VAR_KEY, default_var="0")
    next_number = max(1, _safe_int(current_raw, default=0) + 1)

    remote_path = f"/EksportedeOrdrer_{next_number}.xml"
    while _sftp_path_exists(sftp_client, remote_path):
        next_number += 1
        remote_path = f"/EksportedeOrdrer_{next_number}.xml"

    Variable.set(_KANTINEDATA_FILE_COUNTER_VAR_KEY, str(next_number))
    return remote_path.lstrip("/")


def process_kantinedata():
    """
    Main function to process Kantinedata emails from the INBOX.
    - Fetches flagged emails (from previous failed runs) and unseen emails (new).
    - Flags unseen emails while processing.
    - Extracts attachments and uploads XML files to SFTP.
    - Unflags successfully processed emails, and logs any failures for retry.
    - Uses Airflow Variable to maintain a monotonic counter for filenames, with collision checks against SFTP.
    - Logs detailed information about processing steps and errors.
    - Raises an exception if any email fails to process, which triggers retries.
    """
    # Fetch emails from the kantinedata INBOX
    try:
        # First, fetch flagged mails that have not finished processing (from failed runs)
        flagged_emails, flagged_failed_email_ids = imap_get_emails_with_uids(
            mailbox="INBOX",
            criteria="FLAGGED",
        )
        # Then, fetch unseen mails and flag them for processing
        unseen_emails, unseen_failed_email_ids = imap_get_emails_with_uids(
            mailbox="INBOX",
            criteria="UNSEEN",
            set_flags="\\Flagged",  # Flag while processing
        )

    except Exception as e:
        logger.error(f"Error fetching emails: {e}")
        raise

    # Combine flagged and unseen emails for processing while avoiding duplicates
    # Each email item is (uid, email_message)
    emails = []
    seen_uids = set()
    for uid, mail in flagged_emails + unseen_emails:
        if uid in seen_uids:
            continue
        seen_uids.add(uid)
        emails.append((uid, mail))

    failed_email_ids = []
    seen_failed_email_ids = set()
    for email_id in flagged_failed_email_ids + unseen_failed_email_ids:
        if email_id in seen_failed_email_ids:
            continue
        seen_failed_email_ids.add(email_id)
        failed_email_ids.append(email_id)

    if failed_email_ids:
        logger.warning(
            "Failed to fetch emails with IMAP sequence IDs: %s",
            failed_email_ids,
        )

    if not emails:
        logger.info("No new emails found in the INBOX.")
        return

    # Process each email
    successful_mail_ids: list[str] = []
    failed_mail_ids: list[str] = []
    sftp_hook: SFTPHook | None = None
    for uid, mail in emails:
        try:
            message_id = mail.get("Message-ID") if hasattr(mail, "get") else None
            subject = mail.get("Subject") if hasattr(mail, "get") else None
            logger.info(
                "Processing email UID: %s (Message-ID=%s) with subject: %s",
                uid,
                message_id,
                decode_mime_word(subject) if isinstance(subject, str) else subject,
            )

            attachments = extract_attachments(mail)

            # Mark as processed if no attachments, to avoid reprocessing
            if not attachments:
                logger.info("No attachments found for email UID: %s", uid)
                successful_mail_ids.append(str(uid))
                continue

            # Process attachments
            for attachment in attachments:
                logger.info(
                    "Found attachment: %s (type=%s, bytes=%s) for email ID: %s",
                    attachment.get("filename"),
                    attachment.get("content_type"),
                    len(attachment.get("content_bytes") or b""),
                    message_id,
                )

                # Upload attachment to SFTP if XML
                if attachment.get("content_type") in ["application/xml", "text/xml"]:
                    if sftp_hook is None:
                        sftp_hook = SFTPHook("kantinedata_sftp")
                    with sftp_hook.get_conn() as sftp_client:
                        filename = _allocate_next_filename(sftp_client)
                        remote_path = f"/{filename}"
                        sftp_client.putfo(io.BytesIO(attachment["content_bytes"]), remote_path)
                    logger.info("Uploaded attachment to SFTP: %s", remote_path)

                # Skip non-XML attachments
                else:
                    logger.info(
                        "Skipping non-XML attachment: %s (type=%s) for email ID: %s",
                        attachment.get("filename"),
                        attachment.get("content_type"),
                        message_id,
                    )

            # Mark email as successfully processed
            successful_mail_ids.append(str(uid))

        # Log any exceptions during processing, but continue with other emails
        except Exception as e:
            logger.error(
                "Error processing email (UID=%s, Message-ID=%s): %s",
                uid,
                getattr(mail, "get", lambda *_: None)("Message-ID"),
                e,
            )
            failed_mail_ids.append(str(uid))

    # Unflag successfully processed emails, and log errors for failures
    if successful_mail_ids:
        uid_set = ",".join(sorted(set(successful_mail_ids), key=int))
        _, failed_to_unflag_ids = imap_get_emails_with_uids(
            mailbox="INBOX",
            criteria=f"UID {uid_set}",
            del_flags="\\Flagged",  # Unflag
        )

        if failed_to_unflag_ids:
            logger.error("Failed to unflag email sequence IDs: %s", failed_to_unflag_ids)

    # Log failed email IDs and throw error to trigger retry
    if failed_mail_ids:
        logger.warning(
            "Failed to process %s email(s). UIDs: %s",
            len(failed_mail_ids),
            ",".join(sorted(set(failed_mail_ids), key=int)),
        )
        raise Exception(f"Failed to process {len(failed_mail_ids)} email(s). UIDs: {','.join(sorted(set(failed_mail_ids), key=int))}")

    return
