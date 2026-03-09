import logging
from datetime import datetime

from rkdigi.database_manager import DatabaseManager
from rkdigi.email_handling import EmailSender
from dag_affald.affald_data import (
    build_affald_excel_bytes,
    fetch_affald_registration_monthly_df,
    sheet_specs_requires_carrier
)

logger = logging.getLogger(__name__)


def process_affald() -> None:

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
    today = datetime.now().date().isoformat()
    filename = f"Affaldsterminalen_Udregning_{today}.xlsx"

    email_sender = EmailSender()
    email_sender.send_email(
        sender="xx@randers.dk",
        recipients="xx@randers.dk",
        body=f"Mængde af Genbrugspladsen & Affaldsterminalen opdateret senest {today}.",
        attachments=[(filename, excel_bytes)],
    )

    logger.info("Affald data processing completed successfully (email sent).")
