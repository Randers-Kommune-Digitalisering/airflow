import io
import logging

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from rkdigi.email_handling import EmailReader, EmailSender

from dag_sd_fleksjobrefusion.sd_fleksjobrefusion_data import (
    excel_to_sd_fleksjobrefusion_config,
    run_sd_fleksjobrefusion_job,
)
from utils.mail_attachments import find_latest_attachment

logger = logging.getLogger(__name__)


def _send_failure_email(
    runtime_config: dict,
    failed_persons: list[dict[str, str]],
) -> None:
    """
    Send summary email with all failed person rows.

    :param runtime_config: Runtime config containing sender/recipients/smtp.
    :param failed_persons: Rows that failed during browser processing.
    """
    sender = runtime_config.get("sender_email")
    recipients = runtime_config.get("recipient_emails")
    smtp_server = runtime_config.get("smtp_server")

    lines = [
        "SD Fleksjobrefusion har fejl på følgende personer:",
        "",
    ]
    for person in failed_persons:
        lines.append(
            "- "
            f"{person.get('employee_number', '')} "
            f"({person.get('institution', '')}) | "
            f"Beløb={person.get('amount', '')} | "
            f"Lønart={person.get('wage_type', '')}"
        )

    body = "\n".join(lines)
    subject = (
        "SD Fleksjobrefusion: "
        f"{len(failed_persons)} person(er) fejlede"
    )

    email_sender = EmailSender(smtp_server=smtp_server)
    email_sender.send_email(
        sender=sender,
        recipients=recipients,
        subject=subject,
        body=body,
    )
    logger.info(f"Sent SD Fleksjobrefusion failure summary email for {len(failed_persons)} person(s)")


def _send_success_email(
    runtime_config: dict,
    total_persons: int,
    attachment_name: str,
) -> None:
    """
    Send success email when all persons are processed without failures.

    :param runtime_config: Runtime config containing sender/recipients/smtp.
    :param total_persons: Number of persons processed.
    :param attachment_name: Name of source attachment used for processing.
    """
    sender = runtime_config.get("sender_email")
    recipients = runtime_config.get("recipient_emails")
    smtp_server = runtime_config.get("smtp_server")

    subject = "SD Fleksjobrefusion: Kørsel gennemført uden fejl"
    body = (
        "SD Fleksjobrefusion er gennemført uden fejl.\n\n"
        f"Antal behandlede personer: {total_persons}\n"
        f"Kilde-fil: {attachment_name}"
    )

    email_sender = EmailSender(smtp_server=smtp_server)
    email_sender.send_email(
        sender=sender,
        recipients=recipients,
        subject=subject,
        body=body,
    )
    logger.info(f"Sent SD Fleksjobrefusion success email for {total_persons} processed person(s)")


def process_sd_fleksjobrefusion() -> None:
    """
    Process SD Fleksjobrefusion rows from mailbox attachment.
    """
    logger.info("Starting SD Fleksjobrefusion processing")

    sd_personaleweb = BaseHook.get_connection("sd_personaleweb")
    username = sd_personaleweb.login
    password = sd_personaleweb.password
    if not username or not password:
        raise AirflowFailException(
            "Connection 'sd_personaleweb' is missing username and password"
        )

    fleksjobrefusion_runtime_config = Variable.get("fleksjobrefusion_runtime_config", deserialize_json=True,)
    imap_server = fleksjobrefusion_runtime_config["imap_server"]

    fleksjobrefusion_imap_conn = BaseHook.get_connection("fleksjobrefusion_imap")
    email_reader = EmailReader(
        email=fleksjobrefusion_imap_conn.login,
        password=fleksjobrefusion_imap_conn.password,
        imap_server=imap_server,
    )

    try:
        found = find_latest_attachment(
            email_reader=email_reader,
            filename_prefixes="Fleksjobrefusion",
        )
        if not found:
            raise AirflowFailException("No matching Fleksjobrefusion Excel attachment found in mailbox")

        uid, attachment_name, excel_bytes = found
        logger.info(
            "Found Excel attachment in email UID %s: %s (%s bytes)",
            uid.decode() if isinstance(uid, bytes) else str(uid),
            attachment_name,
            len(excel_bytes),
        )

        try:
            df = pd.read_excel(
                io.BytesIO(excel_bytes),
                engine="openpyxl",
                dtype={"TJNR.": str},
                sheet_name=0,
            )
        except Exception as e:
            raise AirflowFailException(
                f"Failed to parse Excel attachment: {attachment_name}"
            ) from e

        persons = excel_to_sd_fleksjobrefusion_config(df=df)
        if not persons:
            raise AirflowFailException(f"No valid rows found in Excel attachment: {attachment_name}")

        success, failed_persons = run_sd_fleksjobrefusion_job(
            username=username,
            password=password,
            persons=persons,
        )

        if not success:
            logger.warning(
                "SD Fleksjobrefusion flow had %s failed person(s)",
                len(failed_persons),
            )
            try:
                _send_failure_email(
                    runtime_config=fleksjobrefusion_runtime_config,
                    failed_persons=failed_persons,
                )
            except Exception as e:
                raise AirflowFailException("Failed to send SD Fleksjobrefusion failure email") from e
        else:
            try:
                _send_success_email(
                    runtime_config=fleksjobrefusion_runtime_config,
                    total_persons=len(persons),
                    attachment_name=attachment_name,
                )
            except Exception as e:
                raise AirflowFailException("Failed to send SD Fleksjobrefusion success email") from e

        logger.info("SD Fleksjobrefusion processing completed successfully")

    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException("Error processing SD Fleksjobrefusion") from e
    finally:
        email_reader.delete_email_by_uid(uid=uid, mailbox="INBOX", expunge=True)  # Disable when testing
        logger.info(f"Deleted input email {attachment_name} with UID {uid!r} from INBOX")
