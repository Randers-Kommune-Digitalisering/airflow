import logging
import json
from datetime import date

from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException

from dag_absence_post_routing_and_journalize.absence_post_routing_and_journalize_data import (
    build_department_email_map,
    extract_cpr_from_pdf,
)
from dag_sd_delta.delta_client import DeltaClient
from rkdigi.email_handling import EmailReader, EmailSender

from utils.mail_attachments import find_latest_attachment
from utils.mail_messages import get_message_body, build_safe_subject_header

logger = logging.getLogger(__name__)


def resolve_forward_body(
    subject: str | None,
    original_body: str,
    config: dict,
) -> str:
    """Return the configured body with the standard closing text appended."""
    subject_body_mapping = config.get("subject_body_mapping", {})
    if not isinstance(subject_body_mapping, dict):
        raise AirflowFailException(
            "'subject_body_mapping' in Variable 'absence_post_config' "
            "must be a JSON object"
        )

    closing_body = config.get("default_closing_body", "")
    if not isinstance(closing_body, str):
        raise AirflowFailException(
            "'default_closing_body' in Variable 'absence_post_config' "
            "must be a string"
        )

    normalized_subject = str(subject or "").casefold()
    resolved_body = original_body
    for subject_fragment, body in subject_body_mapping.items():
        if not isinstance(subject_fragment, str) or not isinstance(body, str):
            raise AirflowFailException(
                "'subject_body_mapping' keys and values must be strings"
            )
        if subject_fragment.casefold() in normalized_subject:
            resolved_body = body
            break

    if not closing_body:
        return resolved_body
    return f"{resolved_body.rstrip()}\n\n{closing_body}"


def sync_sd_org_department_mapping() -> None:
    """
    Placeholder function for processing the absence_post_routing_and_journalize data.
    """
    logger.info("Starting to process absence_post_routing_and_journalize data...")
    absence_post_imap_conn = BaseHook.get_connection("absence_post_imap")
    absence_post_config = Variable.get("absence_post_config", deserialize_json=True)

    email_reader = EmailReader(
        email=absence_post_imap_conn.login,
        password=absence_post_imap_conn.password,
        imap_server=absence_post_config.get("imap_server")
    )

    # Find the newest matching Excel attachment in the mailbox
    found = find_latest_attachment(
        email_reader=email_reader,
        filename_prefixes="SD org"
    )

    if not found:
        raise AirflowFailException("No matching attachment found in the mailbox.")

    uid, filename, content_bytes = found
    logger.info(f"Found Excel attachment in email UID {uid.decode()}: {filename} ({len(content_bytes)} bytes)")

    # Store the map for the task that processes the PDF attachments.
    department_email_map = build_department_email_map(excel_bytes=content_bytes)
    Variable.set("absence_post_mapning", json.dumps(department_email_map, ensure_ascii=False, indent=2))
    logger.info(f"Updated Airflow Variable 'absence_post_mapning' with {len(department_email_map)} department(s).")


def extract_cpr_from_maindoc_attachments() -> None:
    """Check all maindoc PDF attachments for one valid CPR number each."""
    absence_post_imap_conn = BaseHook.get_connection("absence_post_imap")

    absence_post_config = Variable.get("absence_post_config", deserialize_json=True)
    if not isinstance(absence_post_config, dict):
        raise AirflowFailException("Variable 'absence_post_config' must be a JSON object")

    sender_email = absence_post_config.get("sender_email")
    smtp_server = absence_post_config.get("smtp_server")
    imap_server = absence_post_config.get("imap_server")

    configured_recipients = Variable.get("absence_post_mapning", deserialize_json=True)
    if not isinstance(configured_recipients, dict):
        raise AirflowFailException("Variable 'absence_post_mapning' must be a JSON object")

    recipients_by_department = {
        str(department).strip().casefold(): recipients
        for department, recipients in configured_recipients.items()
        if isinstance(recipients, list)
        and all(isinstance(recipient, str) and recipient.strip() for recipient in recipients)
    }

    email_reader = EmailReader(
        email=absence_post_imap_conn.login,
        password=absence_post_imap_conn.password,
        imap_server=imap_server,
    )
    email_sender = EmailSender(smtp_server=smtp_server)
    emails, failed_ids = email_reader.get_emails(mailbox="INBOX", criteria="ALL")
    if failed_ids:
        logger.warning(f"Could not fetch {len(failed_ids)} email(s) from the mailbox.")

    processed_attachments = 0
    routed_attachments = 0
    failures: list[str] = []
    department_by_cpr: dict[str, str | None] = {}
    delta_client = DeltaClient(BaseHook.get_connection("delta_prod"))
    for message in emails:
        uid = getattr(message, "uid", None)
        uid_text = uid.decode(errors="ignore") if isinstance(uid, bytes) else str(uid)

        for attachment in message.iter_attachments():
            filename = attachment.get_filename() or ""
            normalized_filename = filename.strip().casefold()

            # Ignore unrelated files, but warn for invalid maindoc types.
            if not (
                normalized_filename.startswith("maindoc")
                and normalized_filename.endswith(".pdf")
            ):
                logger.warning(f"Skipped wrong file attachment {filename} uid={uid_text}.")
                continue

            processed_attachments += 1
            pdf_bytes = attachment.get_payload(decode=True)
            if not pdf_bytes:
                logger.warning(f"Skipped unreadable {filename} attachment uid={uid_text}.")
                continue

            try:
                cpr = extract_cpr_from_pdf(pdf_bytes=pdf_bytes)
            except ValueError as exc:
                logger.warning(f"No usable CPR found in {filename} attachment uid={uid_text}: {exc}")
                continue

            # Cache Delta lookups because a CPR can occur in multiple emails.
            if cpr not in department_by_cpr:
                department_codes = set(
                    delta_client.get_sd_unit_codes_by_cpr(cpr, date.today())
                )
                if len(department_codes) == 1:
                    department_by_cpr[cpr] = department_codes.pop()
                elif len(department_codes) > 1:
                    logger.warning(f"Multiple active SD departments: {department_codes} found for {filename} attachmentuid={uid_text}; attachment will not be forwarded.")
                    department_by_cpr[cpr] = None
                else:
                    department_by_cpr[cpr] = None

            department_code = department_by_cpr[cpr]
            recipients = recipients_by_department.get(
                department_code.casefold() if department_code else ""
            )
            # If no recipients are found for the department, log a warning and skip this attachment.
            if not recipients:
                logger.warning(f"No recipients are configured for the SD department found for {filename} attachment uid={uid_text}.")
                failures.append(uid_text)
                continue

            try:
                # Forward the original subject and the resolved body with the PDF.
                email_sender.send_email(
                    sender=sender_email,
                    recipients=recipients,
                    subject=build_safe_subject_header(message.get("Subject")),
                    body=resolve_forward_body(
                        subject=message.get("Subject"),
                        original_body=get_message_body(message),
                        config=absence_post_config,
                    ),
                    attachments=[(filename, pdf_bytes)],
                )
                # Delete the original email after successfully forwarding the attachment.
                email_reader.delete_email_by_uid(uid=uid)
                logger.info(f"Deleted {filename} email uid={uid_text} after forwarding.")

            except Exception:
                logger.exception(f"Could not forward {filename} uid={uid_text} to its department recipients: {recipients} from department: {department_code}")
                failures.append(uid_text)
                continue

            routed_attachments += 1
            logger.info(f"Forwarded {filename} PDF attachment uid={uid_text} to {recipients} from department: {department_code}")

    logger.info(f"Checked {processed_attachments} attachment(s); forwarded {routed_attachments} attachment(s).")
    if failures:
        raise AirflowFailException(f"Could not forward {len(failures)} maindoc PDF attachment(s)")
