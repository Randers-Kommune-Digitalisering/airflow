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
    read_motorstyrelsen_excel_bytes,
    enrich_vehicles_with_customer_levels,
    find_latest_motorstyrelsen_excel_attachment
) 
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.hooks.base import BaseHook
from rkdigi.email_handling import EmailSender, EmailReader
from airflow.exceptions import AirflowFailException


logger = logging.getLogger(__name__)


def process_vognpark() -> None:
    """
    Fetch Excel from Mailbox + Insubiz API and load into Postgres.
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

    found = find_latest_motorstyrelsen_excel_attachment(email_reader=email_reader, criteria="UNSEEN")

    if not found:
        raise AirflowFailException("No Motorstyrelsen Excel attachment found")

    uid, attachment_name, excel_bytes = found

    logger.info(f"Found Excel attachment in email UID {uid.decode()}: {attachment_name} ({len(excel_bytes)} bytes)")

    afg_dato = pd.to_datetime(df["Afg.dato"], errors="coerce")
    inactive_mask = (afg_dato == pd.Timestamp("1900-01-01 00:00:00"))
    insubiz_inactive_df = df.loc[inactive_mask].copy()

    motor_df = read_motorstyrelsen_excel_bytes(excel_bytes=excel_bytes)

    # Only compare with udgåede (1900) in Insubiz
    need_to_delete, need_to_add = compare_motorstyrelsen_vs_insubiz(
        motor_df=motor_df,
        insubiz_df=insubiz_inactive_df,
    )

    # Only keep valid Reg.nr that contains at most 7 digits in "Skal slettes"
    reg_pattern = r"^[A-Za-z0-9]{1,7}$"

    need_to_delete = need_to_delete.loc[
        need_to_delete["Reg.nr."]
        .astype(str)
        .str.strip()
        .str.fullmatch(reg_pattern, na=False)
        ].copy()

    report_bytes = dfs_to_excel_bytes({
        "Skal slettes": need_to_delete,
        "Skal tilføjes": need_to_add,
    })

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

        # latest report date is stored in audit_table
        audit_df = pd.DataFrame({
            "report_date": [report_date]
        })

        audit_df.to_sql("vognpark_run_audit", con=conn, if_exists="replace", index=False)
