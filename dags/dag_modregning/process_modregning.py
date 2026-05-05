import logging
from dateutil.relativedelta import relativedelta
import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.sftp.hooks.sftp import SFTPHook
from rkdigi.email_handling import EmailSender
from dag_modregning.modregning_data import (
    df_to_excel_bytes,
    extract_unique_cprs,
    extract_ydelser_from_serviceplatform_response,
    get_latest_excel_info,
    mask_cpr,
    read_excel_from_sftp,
)

logger = logging.getLogger(__name__)


def _resolve_date_range() -> tuple[str, str]:
    """
    Resolve startDato/slutDato as ISO dates based on logical_date (default: month-to-date).
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
    modregning_config = Variable.get("modregning_config", deserialize_json=True)

    sftp_dir = modregning_config["sftp_dir"]
    sender = modregning_config["sender_email"]
    recipients = modregning_config["recipient_emails"]
    smtp_server = modregning_config["smtp_server"]

    start_dato, slut_dato = _resolve_date_range()
    logger.info(f"Modregning date range: {start_dato} -> {slut_dato}")

    sftp_hook = SFTPHook(ssh_conn_id="shared_sftp")
    latest_info = get_latest_excel_info(sftp_hook=sftp_hook, directory=sftp_dir)
    if not latest_info:
        raise AirflowFailException("No Excel file found on SFTP for Modregning")

    excel_path, modified_at_utc = latest_info
    logger.info(f"Using Excel file: {excel_path} (modified_at_utc={modified_at_utc.isoformat()})")

    try:
        df = read_excel_from_sftp(
            sftp_hook=sftp_hook,
            remote_path=excel_path,
            dtype={"ID-nummer": str},
            sheet_name=0,
        )
        cpr_list = extract_unique_cprs(df=df)
        logger.info("Extracted CPR from sftp")
        if not cpr_list:
            raise AirflowFailException("No CPR values found in the Excel file")

        logger.info("After extracting unique CPRs")

        rows: list[list[str]] = []
        
        from utils.kombit import TempClientCert
        from kombit_client.integrations.sf1491 import YdelseListeHentClient # Virker kun med Lazy import. Hvis du sætter import ved toppen så fryser hele DAG'en 
        with TempClientCert() as client_cert_path:
            for cpr in cpr_list:
                masked_cpr = mask_cpr(cpr=cpr)
                logger.info(f"Processing CPR: {masked_cpr}")
                try:
                    ydelse_client = YdelseListeHentClient(client_certificate_file_path=client_cert_path)
                    payload = ydelse_client.effektuering_hent(cpr=cpr, start_dato=start_dato, slut_dato=slut_dato)

                    ydelser, found_any = extract_ydelser_from_serviceplatform_response(payload=payload)

                    if ydelser:
                        cell_value = ", ".join(sorted(ydelser)) # Join sorted ydelser into a single string(eg. Forhøjet sats , Grund sats)
                    elif found_any:
                        cell_value = ""  # Only filtered ydelser -> empty cell Onl
                    else:
                        cell_value = "Ingen Ydelse"  # No ydelser in response -> "Ingen Ydelse"

                    rows.append([cpr, cell_value])

                except Exception as e:
                    logger.error(f"Error processing CPR {masked_cpr}: {e}")

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

    finally:
        try:
            sftp_hook.delete_file(excel_path)
            logger.info(f"Deleted SFTP file: {excel_path}")
        except Exception:
            logger.exception(f"Could not delete SFTP file: {excel_path}")
