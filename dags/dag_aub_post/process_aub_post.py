import logging

from airflow.hooks.base import BaseHook
from airflow.models import Variable

try:
    from airflow.exceptions import AirflowFailException
except ImportError:
    class AirflowFailException(Exception):
        pass

from rkdigi.email_handling import EmailReader, EmailSender
from dag_aub_post.aub_post_data import (
    build_education_contact_map,
    extract_education_from_pdf,
    find_attachment_by_name,
    resolve_contact_email,
)

logger = logging.getLogger(__name__)

_AUB_POST_CONFIG_VAR = "aub_post_config"
_DEFAULT_MAILBOX = "INBOX"
_DEFAULT_SEARCH_CRITERIA = "ALL"
_TARGET_ATTACHMENT_NAME = "maindoc.pdf"


def process_aub_post() -> None:
    """
    Main processing function for the AUB post DAG.

    It performs the following steps:
    1. Fetches job configuration from Airflow Variable.
    2. Builds a mapping of educations to contact emails.
    3. Initializes email reader and sender.
    4. Fetches emails from the specified mailbox based on search criteria.
    5. For each email, it finds the target PDF attachment, extracts the education, resolves the contact email, forwards the email to the contact, and deletes the original email from the mailbox.
    Note: If any failures occur during processing, it raises an AirflowFailException and logs per-email details.
    """
    # Fetch job configuration from Airflow Variable
    config = Variable.get(_AUB_POST_CONFIG_VAR, deserialize_json=True)
    if not isinstance(config, dict):
        raise ValueError(f"Airflow Variable '{_AUB_POST_CONFIG_VAR}' must be a JSON object")

    smtp_server = config.get("smtp_server")
    sender_email = config.get("sender_email")
    contact_mappings = config.get("contacts_map")

    mailbox = config.get("mailbox", _DEFAULT_MAILBOX)  # Use default if not provided
    search_criteria = config.get("mail_search_criteria", _DEFAULT_SEARCH_CRITERIA)  # Use default if not provided

    if not isinstance(smtp_server, str) or not smtp_server.strip():
        raise ValueError("'smtp_server' must be a non-empty string")
    if not isinstance(sender_email, str) or not sender_email.strip():
        raise ValueError("'sender_email' must be a non-empty string")
    if not isinstance(mailbox, str) or not mailbox.strip():
        raise ValueError("'mailbox' must be a non-empty string")
    if not isinstance(search_criteria, str) or not search_criteria.strip():
        raise ValueError("'mail_search_criteria' must be a non-empty string")

    # Build the education to contact email map
    education_contact_map = build_education_contact_map(contact_mappings=contact_mappings)

    # Initialize email reader and sender
    aub_post_conn = BaseHook.get_connection("aub_post_imap")

    if not aub_post_conn.login or not aub_post_conn.password:
        raise ValueError(
            "Connection 'aub_post_imap' must include login and password"
        )

    email_reader = EmailReader(
        email=aub_post_conn.login,
        password=aub_post_conn.password,
    )

    email_sender = EmailSender(smtp_server=smtp_server.strip())

    # Fetch emails from the mailbox based on the search criteria
    emails, failed_ids = email_reader.get_emails(
        mailbox=mailbox.strip(),
        criteria=search_criteria.strip(),
    )

    failures: list[str] = []

    if failed_ids:
        failures.append(f"Could not fetch {len(failed_ids)} email(s) from mailbox")

    if not emails and not failed_ids:
        logger.info("No emails found in mailbox for AUB processing.")
        return

    # Process each email: find the target attachment, extract education, resolve contact, forward email, and delete original email
    for message in emails:
        uid = getattr(message, "uid", None)
        if uid is None:
            failures.append("Skipped one email without UID")
            continue

        if isinstance(uid, bytes):
            uid_text = uid.decode(errors="ignore")
        else:
            uid_text = str(uid)

        try:
            _, attachment_bytes = find_attachment_by_name(
                message=message,
                target_filename=_TARGET_ATTACHMENT_NAME,
            )
            education = extract_education_from_pdf(attachment_bytes)
            contact_email = resolve_contact_email(
                education=education,
                education_contact_map=education_contact_map,
            )

            subject = (message.get("Subject") or "").strip()
            body = ""
            if message.is_multipart():
                body_part = message.get_body(preferencelist=("plain", "html"))
                if body_part is not None:
                    body = body_part.get_content()
            else:
                payload = message.get_payload(decode=True)
                if isinstance(payload, bytes):
                    body = payload.decode(message.get_content_charset() or "utf-8", errors="replace")
                else:
                    body = payload or ""

            email_sender.send_email(
                sender=sender_email.strip(),
                recipients=[contact_email],
                subject=subject,
                body=body,
                attachments=[(_TARGET_ATTACHMENT_NAME, attachment_bytes)],
            )

            email_reader.delete_email_by_uid(
                uid=uid,
                mailbox=mailbox.strip(),
                expunge=True,
            )
            logger.info("Processed and deleted file %s mailbox email uid=%s", _TARGET_ATTACHMENT_NAME, uid_text)

        except Exception as exc:
            failures.append(f"uid={uid_text}: {exc}")
            logger.exception("AUB processing failed for filename %s uid=%s", _TARGET_ATTACHMENT_NAME, uid_text)

    if failures:
        raise AirflowFailException(
            f"AUB post processing failed for {len(failures)} email(s). "
            "See task logs for details."
        )
