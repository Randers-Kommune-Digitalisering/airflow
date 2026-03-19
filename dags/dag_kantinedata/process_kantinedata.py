import logging

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.hooks.base import BaseHook
from dag_kantinedata.mail_utils import imap_get_emails_with_uids, extract_attachments, decode_mime_word

logger = logging.getLogger(__name__)
imap_conn = BaseHook.get_connection("kantinedata_imap")
sftp_hook = SFTPHook("kantinedata_sftp")


def process_kantinedata():
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
        return

    # Combine flagged and unseen emails for processing
    # Each email item is (uid, email_message)
    emails = flagged_emails + unseen_emails
    failed_email_ids = flagged_failed_email_ids + unseen_failed_email_ids

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
                    remote_path = f"/{attachment['filename']}"
                    # with sftp_hook.get_conn() as sftp_client:
                    #     sftp_client.putfo(io.BytesIO(attachment["content_bytes"]), remote_path)
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
