import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from dag_vognpark.vognpark_data import (
    INSUBIZ_EXCEL_FIELDS,
    compare_motorstyrelsen_vs_insubiz,
    dfs_to_excel_bytes,
    fetch_insubiz_customers,
    fetch_insubiz_vehicles,
    normalize_insubiz_df,
    read_motorstyrelsen_pdf_bytes,
    enrich_vehicles_with_customer_levels,
    find_latest_motorstyrelsen_pdf_attachment,
)
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.hooks.base import BaseHook
from rkdigi.email_handling import EmailSender, EmailReader
from airflow.exceptions import AirflowFailException


logger = logging.getLogger(__name__)


def process_vognpark() -> None:
    """
    Fetch PDF from mailbox + Insubiz API and load into Postgres.
    """
    insubiz_hook = HttpHook(http_conn_id="insubiz_cloud_api")
    vognpark_hook = PostgresHook(postgres_conn_id="vognpark_db")

    vehicles = fetch_insubiz_vehicles(http_hook=insubiz_hook)
    df = pd.json_normalize(vehicles, sep="_")

    customers = fetch_insubiz_customers(http_hook=insubiz_hook)
    df = enrich_vehicles_with_customer_levels(vehicles_df=df, customers=customers)

    df = df.reindex(columns=INSUBIZ_EXCEL_FIELDS)
    df = normalize_insubiz_df(df=df)

    vognpark_imap_conn = BaseHook.get_connection("vognpark_imap")

    email_reader = EmailReader(
        email=vognpark_imap_conn.login,
        password=vognpark_imap_conn.password,
    )

    found = find_latest_motorstyrelsen_pdf_attachment(email_reader=email_reader)

    if not found:
        raise AirflowFailException("No Motorstyrelsen PDF attachment found")

    uid, attachment_name, pdf_bytes = found

    logger.info(f"Found PDF attachment in email UID {uid.decode()}: {attachment_name} ({len(pdf_bytes)} bytes)")

    afg_dato = pd.to_datetime(df["Afg.dato"], errors="coerce")
    inactive_mask = (afg_dato == pd.Timestamp("1900-01-01 00:00:00"))

    insubiz_inactive_df = df.loc[inactive_mask].copy()

    motor_df = read_motorstyrelsen_pdf_bytes(pdf_bytes=pdf_bytes)
    motor_set = set(motor_df["registreringsnummer"].astype(str).str.strip().str.upper())
    insubiz_set = set(
        insubiz_inactive_df["Reg.nr."]
        .astype(str).str.strip().str.upper()
    )

    logger.info(f"Motorstyrelsen PDF Reg NR count= {len(motor_set)} Insubiz Reg NR count= {len(insubiz_set)} overlap={len(motor_set & insubiz_set)}")
    logger.info(f"Motorstyrelsen PDF Reg NR not in Insubiz={len(motor_set - insubiz_set)}")

    # Only compare with udgåede (1900) in Insubiz
    need_to_delete, need_to_add = compare_motorstyrelsen_vs_insubiz(
        motor_df=motor_df,
        insubiz_df=insubiz_inactive_df,
    )

    report_bytes = dfs_to_excel_bytes(
        {
            "Skal slettes": need_to_delete,
            "Skal tilføjes": need_to_add,
        }
    )

    cfg = Variable.get("vognpark_runtime_config", deserialize_json=True)
    sender = cfg["sender_email"]
    recipients = cfg["recipient_emails"]
    smtp_server = cfg["smtp_server"]

    ctx = get_current_context()
    logical_date = ctx["logical_date"]
    dag_tz = ctx["dag"].timezone
    report_date = logical_date.in_timezone(dag_tz).date().isoformat()

    filename = f"uoverensstemmelser_biler_{report_date}.xlsx"

    EmailSender(smtp_server=smtp_server).send_email(
        sender=sender,
        recipients=recipients,
        subject=f"Vognpark data: {report_date}",
        body=f"Vedhæftet er seneste vognpark-udtræk pr. {report_date}.",
        attachments=[(filename, report_bytes)],
    )

    engine = vognpark_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        df.to_sql("vognpark_data", con=conn, if_exists="replace", index=False)

        # latest report date is stored in audit table
        audit_df = pd.DataFrame({"report_date": [report_date]})
        audit_df.to_sql("vognpark_run_audit", con=conn, if_exists="replace", index=False)

    logger.info("Vognpark data processed successfully")
