import logging
from dateutil.relativedelta import relativedelta
import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from typing import Iterable
from airflow.operators.python import get_current_context
import io
from rkdigi.email_handling import EmailSender, EmailReader

from dag_modregning.modregning_data import (
    df_to_excel_bytes,
    extract_unique_cprs,
    extract_ydelser_from_serviceplatform_response,
)

logger = logging.getLogger(__name__)


def _find_latest_modregning_excel_attachment(
    email_reader: EmailReader,
    mailbox: str = "INBOX",
    criteria: str = "ALL",
    filename_prefixes: Iterable[str] = ("Modregning", "2026", "DAKT"),
    max_emails: int = 50,
) -> tuple[bytes, str, bytes] | None:
    """
    Find the newest matching Excel attachment in an IMAP mailbox.

    :param email_reader: EmailReader used to fetch emails.
    :param mailbox: Mailbox/folder to search in (e.g. "INBOX").
    :param criteria: IMAP search criteria (e.g. "ALL", "UNSEEN").
    :param filename_prefixes: Allowed attachment filename prefixes.
    :param max_emails: Maximum number of emails to fetch and scan.
    :return: (uid, filename, content_bytes) for the first matching attachment, or None.
    """
    emails, failed = email_reader.get_emails(
        mailbox=mailbox,
        criteria=criteria,
        set_flags=None,
        max=max_emails,
        low_to_high=False,  # start with newest emails first
    )

    logger.info(f"Fetched {len(emails)} email(s), {len(failed)} failed to fetch.")

    for msg in emails:
        uid: bytes = getattr(msg, "uid", None)
        subject = msg.get("Subject", "")
        logger.info(f"Email UID: {uid}, Subject: {subject}")

        for part in msg.iter_attachments():
            filename = part.get_filename() or ""
            if not filename.lower().endswith(".xlsx"):
                continue

            if not any(filename.startswith(p) for p in filename_prefixes):
                continue

            content = part.get_payload(decode=True)  # bytes
            if not content:
                continue

            return uid, filename, content

    return None


def _resolve_date_range() -> tuple[str, str]:
    """
    Resolve start_dato/slut_dato as ISO dates (YYYY-MM-DD) based on Airflow `logical_date`.

    - `logical_date` is converted to the DAG timezone and truncated to a date.
    - `start_dato` is set to the 1st day of the previous month relative to `logical_date`.
    - `slut_dato` is set to `logical_date`.

    :return: (start_dato, slut_dato) as ISO date strings.
    """
    ctx = get_current_context()
    logical_date = ctx["logical_date"].in_timezone(ctx["dag"].timezone).date()
    start = logical_date.replace(day=1) - relativedelta(months=1)
    end = logical_date
    return start.isoformat(), end.isoformat()


def process_modregning() -> None:
    """
    1) Read newest Excel from SFTP (CPR list)
    2) Call Serviceplatform for each CPR in date range
    3) Email an Excel report
    """
    modregning_runtime_config = Variable.get("modregning_runtime_config", deserialize_json=True)

    sender = modregning_runtime_config["sender_email"]
    recipients = modregning_runtime_config["recipient_emails"]
    smtp_server = modregning_runtime_config["smtp_server"]

    modregning_imap_conn = BaseHook.get_connection("modregning_imap")

    email_reader = EmailReader(
        email=modregning_imap_conn.login,
        password=modregning_imap_conn.password,
    )

    found = _find_latest_modregning_excel_attachment(
        email_reader=email_reader,
    )

    if not found:
        raise AirflowFailException("No Modregning Excel attachment found in mailbox")

    uid, attachment_name, excel_bytes = found
    logger.info(f"Found Excel attachment in email UID {uid.decode()}: {attachment_name} ({len(excel_bytes)} bytes)")

    start_dato, slut_dato = _resolve_date_range()
    logger.info(f"Modregning date range: {start_dato} -> {slut_dato}")

    try:
        df = pd.read_excel(
            io.BytesIO(excel_bytes),
            engine="openpyxl",
        )

        cpr_list = extract_unique_cprs(df=df)
        logger.info("Extracted CPR from sftp")
        if not cpr_list:
            raise AirflowFailException("No CPR values found in the Excel file")

        logger.info("After extracting unique CPRs")

        rows: list[list[str]] = []

        from utils.kombit import TempClientCert
        from kombit_client.integrations.sf1491 import YdelseListeHentClient  # Import lazily to avoid Airflow freezing issue

        with TempClientCert() as client_cert_path:
            ydelse_client = YdelseListeHentClient(client_certificate_file_path=client_cert_path)
            for cpr in cpr_list:
                try:
                    payload = ydelse_client.effektuering_hent(cpr=cpr, start_dato=start_dato, slut_dato=slut_dato)

                    ydelser, found_any = extract_ydelser_from_serviceplatform_response(payload=payload)

                    if ydelser:
                        cell_value = ", ".join(sorted(ydelser))  # Join sorted ydelser into a single string(e.g. Forhøjet sats , Grund sats)
                    elif found_any:
                        cell_value = ""  # Only filtered ydelser -> empty cell only
                    else:
                        cell_value = "Ingen Ydelse"  # No ydelser in response -> "Ingen Ydelse"

                    rows.append([cpr, cell_value])

                except Exception as e:
                    logger.error(f"Error during processing: {e}")
                    rows.append([cpr, "Error"])

        out_df = pd.DataFrame(rows, columns=["cpr", "YdelseNavn"])
        excel_bytes = df_to_excel_bytes(df=out_df)

        ctx = get_current_context()
        logical_date = ctx["logical_date"]
        dag_tz = ctx["dag"].timezone
        report_date = logical_date.in_timezone(dag_tz).date().isoformat()

        filename = f"Modregning_{report_date}.xlsx"

        email_sender = EmailSender(smtp_server=smtp_server)
        email_sender.send_email(
            sender=sender,
            recipients=recipients,
            subject=f"Modregninger for {report_date}",
            body=f"Liste af Modregning er vedhæftet: {report_date}",
            attachments=[(filename, excel_bytes)],
        )

        logger.info("Modregning processing completed successfully (email sent).")

        # Delete the input email right after successful processing (report sent)
        email_reader.delete_email_by_uid(uid=uid, mailbox="INBOX", expunge=True)
        logger.info(f"Deleted input email UID {uid!r} from INBOX")

    except Exception as e:
        raise AirflowFailException("Error processing Modregning") from e
