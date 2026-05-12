import logging
import pendulum
from rkdigi.database_manager import DatabaseManager
from rkdigi.email_handling import EmailSender
from dag_affald.affald_data import (
    build_affald_excel_bytes,
    fetch_affald_registration_monthly_df,
    sheet_specs_requires_carrier,
    mp_waste_amount_data,
    aggregate_taxes_quantity_by_month,
    build_mp_monthly_excel_bytes
)
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable

logger = logging.getLogger(__name__)


def process_affald() -> None:

    mp_http_hook = HttpHook(http_conn_id="marius_pedersen_api")

    ctx = get_current_context()
    logical_date = ctx["logical_date"]
    dag_tz = ctx["dag"].timezone

    # to_date = date for the run (in DAG timezone)
    to_p = logical_date.in_timezone(dag_tz).date()
    to_p = pendulum.date(to_p.year, to_p.month, to_p.day)

    # from date - (to_date -1 year) and set to 1. january of that year
    from_p = to_p.subtract(years=1).replace(month=1, day=1)

    mp_from_date = from_p.to_date_string()  # "YYYY-MM-DD"
    mp_to_date = to_p.to_date_string()

    logger.info(f"MP data will be fetched for period: {mp_from_date} to {mp_to_date}")

    rows = mp_waste_amount_data(
        http_hook=mp_http_hook,
        customer_numbers=[80067523, 80070490, 80070170],
        from_date=mp_from_date,
        to_date=mp_to_date,
        installation_address_id=[],
    )

    monthly_data = aggregate_taxes_quantity_by_month(rows=rows)
    logger.info(f"Aggregated MP data by month: {monthly_data}")

    mp_excel_bytes = build_mp_monthly_excel_bytes(monthly_data=monthly_data)

    affald_config = Variable.get("affald_config", deserialize_json=True)

    sender = affald_config["sender_email"]
    recipients = affald_config["recipient_emails"]
    smtp_server = affald_config["smtp_server"]

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
    report_date = logical_date.in_timezone(dag_tz).date().isoformat()

    filename_affald = f"Affaldsterminalen_Udregning_{report_date}.xlsx"
    filename_mp = f"Marius_Pedersen_Data_Udregning_{report_date}.xlsx"

    email_sender = EmailSender(smtp_server=smtp_server)
    email_sender.send_email(
        sender=sender,
        recipients=recipients,
        subject=f"Affald mængdeopdatering: {report_date}",
        body=f"Seneste opdatering af mængder for Genbrugspladsen, Affaldsterminalen, Indsamlingsmængder og Marius Pedersen: {report_date}.",
        attachments=[(filename_affald, excel_bytes),(filename_mp, mp_excel_bytes)],
    )

    logger.info("Affald data processing completed successfully (email sent).")
