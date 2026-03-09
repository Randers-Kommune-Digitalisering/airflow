import logging

from rkdigi.database_manager import DatabaseManager
from rkdigi.email_handling import EmailSender
from dag_affald.affald_data import (
    build_affald_excel_bytes,
    fetch_affald_registration_monthly_df,
    sheet_specs_requires_carrier
)
from airflow.operators.python import get_current_context
from airflow.models import Variable

logger = logging.getLogger(__name__)


def process_affald() -> None:

    sender = Variable.get("affald_config", default_var=None, deserialize_json=True)["sender_email"]
    recipients = Variable.get("affald_config", default_var=None, deserialize_json=True)["recipient_emails"]

    affald_db = DatabaseManager(
        profile_name="scanvaegt_db",
        db_type="mssql",
        airflow_connection_id="scanvaegt_db",
    )
    affald_engine = getattr(affald_db, "_engine")

    include_carrier = sheet_specs_requires_carrier()

    affald_df = fetch_affald_registration_monthly_df(
        affald_engine=affald_engine,
        customer_names=[],
        include_carrier=include_carrier,
    )

    excel_bytes = build_affald_excel_bytes(df=affald_df)
    ctx = get_current_context()
    logical_date = ctx["logical_date"]
    dag_tz = ctx["dag"].timezone
    report_date = logical_date.in_timezone(dag_tz).date().isoformat()

    filename = f"Affaldsterminalen_Udregning_{report_date}.xlsx"

    email_sender = EmailSender()
    email_sender.send_email(
        sender=sender,
        recipients=recipients,
        body=f"Mængde af Genbrugspladsen & Affaldsterminalen opdateret senest {report_date}.",
        attachments=[(filename, excel_bytes)],
    )

    logger.info("Affald data processing completed successfully (email sent).")
