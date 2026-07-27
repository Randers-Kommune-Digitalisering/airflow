import logging
import io
import pandas as pd
from dateutil.relativedelta import relativedelta
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context
from rkdigi.email_handling import EmailSender, EmailReader

from dag_modregning.modregning_data import (
    df_to_excel_bytes,
    extract_unique_cprs,
    extract_ydelser_from_serviceplatform_response,
    find_latest_modregning_excel_attachment,
)

logger = logging.getLogger(__name__)


def _resolve_month_interval() -> tuple[str, str]:
    """
    Resolve and validate month interval from DAG runtime params.

    :return: Tuple containing (start_dato, slut_dato).
    :raises AirflowFailException: If required params are missing or the interval is invalid.
    """
    ctx = get_current_context()
    start_dato = (ctx.get("params") or {}).get("start_dato")
    slut_dato = (ctx.get("params") or {}).get("slut_dato")

    if not start_dato or not slut_dato:
        raise AirflowFailException("Need to specify both start_dato and slut_dato (YYYY-MM-DD).")

    if start_dato > slut_dato:
        raise AirflowFailException("start_dato must not be after slut_dato.")

    return start_dato, slut_dato


def process_modregning() -> None:
    """
    Process CPRs from the Modregning mailbox and generate a report.

    1) Read newest Excel from Modregning Mailbox (CPR list)
    2) Call Serviceplatform for each CPR in date range
    3) Email an Excel report
    """
    modregning_runtime_config = Variable.get("modregning_runtime_config", deserialize_json=True)

    sender = modregning_runtime_config["sender_email"]
    recipients = modregning_runtime_config["recipient_emails"]
    smtp_server = modregning_runtime_config["smtp_server"]
    imap_server = modregning_runtime_config["imap_server"]

    modregning_imap_conn = BaseHook.get_connection("modregning_imap")

    email_reader = EmailReader(
        email=modregning_imap_conn.login,
        password=modregning_imap_conn.password,
        imap_server=imap_server,
    )

    found = find_latest_modregning_excel_attachment(
        email_reader=email_reader,
    )

    if not found:
        raise AirflowFailException("No Modregning Excel attachment found in mailbox")

    uid, attachment_name, excel_bytes = found
    logger.info(f"Found Excel attachment in email UID {uid.decode()}: {attachment_name} ({len(excel_bytes)} bytes)")

    start_dato, slut_dato = _resolve_month_interval()
    logger.info(f"Modregning date range: {start_dato} -> {slut_dato}")

    try:
        df = pd.read_excel(
            io.BytesIO(excel_bytes),
            engine="openpyxl",
            dtype={"ID-nummer": str},
            sheet_name=0,
        )

        cpr_list = extract_unique_cprs(df=df)
        logger.info("Extracted CPR from Modregning Excel")
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
            body=f"Liste af Modregning er vedhæftet: fra {report_date} med Startdato {start_dato} og Slutdato {slut_dato}",
            attachments=[(filename, excel_bytes)],
        )

        logger.info("Modregning processing completed successfully (email sent).")

        # Delete the input email right after successful processing (report sent)
        email_reader.delete_email_by_uid(uid=uid, mailbox="INBOX", expunge=True)
        logger.info(f"Deleted input email {attachment_name} with UID {uid!r} from INBOX")

    except Exception as e:
        raise AirflowFailException("Error processing Modregning") from e
