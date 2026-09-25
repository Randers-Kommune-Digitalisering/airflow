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


def _resolve_forward_body(
    subject: str | None,
    original_body: str,
    config: dict,
) -> str:
    """
    Return the configured body with optional greeting and closing text.

    :param subject: The subject of the email.
    :param original_body: The original body of the email.
    :param config: The absence post configuration.
    :return: The resolved email body with greeting and closing text.
    """
    subject_body_mapping = config.get("subject_body_mapping", {})
    if not isinstance(subject_body_mapping, dict):
        raise AirflowFailException(
            "'subject_body_mapping' in Variable 'absence_post_config' must be a JSON object"
        )

    welcome_body = config.get("default_welcome_body", "")
    if not isinstance(welcome_body, str):
        raise AirflowFailException(
            "'default_welcome_body' in Variable 'absence_post_config' must be a string"
        )

    closing_body = config.get("default_closing_body", "")
    if not isinstance(closing_body, str):
        raise AirflowFailException(
            "'default_closing_body' in Variable 'absence_post_config' must be a string"
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

    if welcome_body:
        resolved_body = f"{welcome_body.rstrip()}\n\n{resolved_body}"
    if closing_body:
        resolved_body = f"{resolved_body.rstrip()}\n\n{closing_body}"
    return resolved_body


def _notify_multiple_active_departments(
    email_sender: EmailSender,
    sender_email: str | None,
    recipients: list[str] | None,
    multi_department_info: dict[str, dict],
) -> None:
    """
    Notify recipients about persons with multiple active SD departments with department codes and the affected PDF attachments.

    :param email_sender: EmailSender used to send the notification.
    :param sender_email: Email address used as the sender.
    :param recipients: Email addresses receiving the notification.
    :param multi_department_info: Person names, department codes, and PDF
        attachments grouped by CPR.
    :return: None.
    """
    if not recipients or not all(isinstance(recipient, str) and recipient.strip() for recipient in recipients):
        logger.warning(
            "'multi_department_notification_recipients' is not configured; "
            f"skipping notification for {len(multi_department_info)} person(s) with multiple active departments."
        )
        return

    overview_lines = []
    attachments: list[tuple[str, bytes]] = []
    for info in sorted(multi_department_info.values(), key=lambda info: info["person_name"]):
        person_name = info["person_name"]
        overview_lines.append(
            f"Navn: {person_name} - Afdelingskoder: {', '.join(sorted(info['department_codes']))}"
        )
        for attachment_filename, pdf_bytes in info["attachments"]:
            attachments.append((f"{person_name}_{attachment_filename}", pdf_bytes))

    body = (
        "Følgende personer har mere end én aktiv SD-afdelingskode, "
        "og deres vedhæftede dokument(er) er derfor ikke blevet videresendt automatisk:\n\n"
        + "\n".join(overview_lines)
    )

    email_sender.send_email(
        sender=sender_email,
        recipients=recipients,
        subject="Personer med flere aktive SD-afdelingskoder",
        body=body,
        attachments=attachments,
    )
    logger.info(f"Sent multiple-active-departments notification for {len(multi_department_info)} person(s) with {len(attachments)} attachment(s).")


def sync_sd_org_department_mapping() -> None:
    """
    Sync the mapping between SD org departments and email addresses into an Airflow Variable.
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
        filename_prefixes=("SD org",)
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
    # Replace this imap with the correct connection for the real absence_post mailbox
    absence_post_imap_conn = BaseHook.get_connection("absence_post_imap")

    absence_post_config = Variable.get("absence_post_config", deserialize_json=True)
    if not isinstance(absence_post_config, dict):
        raise AirflowFailException("Variable 'absence_post_config' must be a JSON object")

    allowed_subject_fragments = absence_post_config.get("allowed_subject_fragments")
    if (
        not isinstance(allowed_subject_fragments, list)
        or not allowed_subject_fragments
        or not all(
            isinstance(fragment, str) and fragment.strip()
            for fragment in allowed_subject_fragments
        )
    ):
        raise AirflowFailException(
            "'allowed_subject_fragments' in Variable 'absence_post_config' must be a non-empty list of strings"
        )
    normalized_subject_fragments = [
        fragment.casefold() for fragment in allowed_subject_fragments
    ]

    sender_email = absence_post_config.get("sender_email")
    smtp_server = absence_post_config.get("smtp_server")
    imap_server = absence_post_config.get("imap_server")
    multi_department_notification_recipients = absence_post_config.get("multi_department_notification_recipients")

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

    # Only fetch mails, if the subject contains a fragment from `allowed_subject_fragments`
    emails, failed_ids = email_reader.get_emails(mailbox="INBOX", criteria="ALL")
    if failed_ids:
        logger.warning(f"Could not fetch {len(failed_ids)} email(s) from the mailbox.")

    processed_attachments = 0
    routed_attachments = 0
    skipped_attachments = 0
    failures: list[str] = []
    department_by_cpr: dict[str, str | None] = {}
    multi_department_info: dict[str, dict] = {}
    delta_client = DeltaClient(BaseHook.get_connection("delta_prod"))

    for message in emails:
        subject = str(message.get("Subject") or "")
        if not any(
            fragment in subject.casefold()
            for fragment in normalized_subject_fragments
        ):
            continue

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

            try:
                cpr = extract_cpr_from_pdf(pdf_bytes=pdf_bytes)
            except ValueError as exc:
                logger.warning(f"No usable CPR found in {filename} attachment uid={uid_text}: {exc}")
                continue

            # Cache Delta lookups because a CPR can occur in multiple emails.
            if cpr not in department_by_cpr:
                engagements = delta_client.get_sd_unit_codes_by_cpr(cpr=cpr, valid_date=date.today())
                department_codes = {e["department_id"] for e in engagements if e["department_id"]}
                if len(department_codes) == 1:
                    department_by_cpr[cpr] = department_codes.pop()
                elif len(department_codes) > 1:
                    logger.warning(
                        f"Multiple active SD departments: {department_codes} found for {filename} uid={uid_text}; attachment will not be forwarded."
                    )
                    person_name = next(
                        (e["person_name"] for e in engagements if e["person_name"]),
                        None,
                    )
                    if person_name is None:
                        raise AirflowFailException("No person name found for multiple active SD departments")
                    multi_department_info[cpr] = {
                        "person_name": person_name,
                        "department_codes": department_codes,
                        "attachments": [],
                    }
                    department_by_cpr[cpr] = None
                else:
                    department_by_cpr[cpr] = None

            department_code = department_by_cpr[cpr]
            recipients = recipients_by_department.get(
                department_code.casefold() if department_code else ""
            )
            # If no recipients are found for the department (e.g. multiple active departments or an unmapped department), skip this attachment without failing the whole task.
            if not recipients:
                logger.warning(f"No recipients are configured for the SD department found for {filename} attachment uid={uid_text}.")
                skipped_attachments += 1
                if cpr in multi_department_info:
                    multi_department_info[cpr]["attachments"].append((filename, pdf_bytes))
                continue

            try:
                # Forward the original subject and the resolved body with the PDF.
                email_sender.send_email(
                    sender=sender_email,
                    recipients=recipients,
                    subject=build_safe_subject_header(message.get("Subject")),
                    body=_resolve_forward_body(
                        subject=message.get("Subject"),
                        original_body=get_message_body(message),
                        config=absence_post_config,
                    ),
                    attachments=[(filename, pdf_bytes)],
                )
                # Delete the original email after successfully forwarding the attachment.
                email_reader.delete_email_by_uid(uid=uid, mailbox="INBOX", expunge=True)
                logger.info(f"Deleted {filename} email uid={uid_text} after forwarding.")

            except Exception:
                logger.exception(f"Could not forward {filename} uid={uid_text} to its department recipients: {recipients} from department: {department_code}")
                failures.append(uid_text)
                continue

            routed_attachments += 1
            logger.info(f"Forwarded {filename} PDF attachment uid={uid_text} to {recipients} from department: {department_code}")

    # Notify about multiple active departments if any were encountered.
    if multi_department_info:
        _notify_multiple_active_departments(
            email_sender=email_sender,
            sender_email=sender_email,
            recipients=multi_department_notification_recipients,
            multi_department_info=multi_department_info,
        )

    logger.info(f"Checked {processed_attachments} attachment(s); forwarded {routed_attachments} attachment(s); skipped {skipped_attachments} attachment(s).")
    failure_messages = []
    if failed_ids:
        failure_messages.append(
            f"Could not fetch {len(failed_ids)} email(s) from the mailbox"
        )
    if failures:
        failure_messages.append(
            f"Could not forward {len(failures)} maindoc PDF attachment(s)"
        )
    if failure_messages:
        raise AirflowFailException("; ".join(failure_messages))
