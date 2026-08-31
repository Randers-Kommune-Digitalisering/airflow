import logging

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dag_bom.bom_data import run_bom_job

logger = logging.getLogger(__name__)


def process_bom(data_interval_end: pendulum.DateTime, **_) -> None:
    """
    Run the BOM browser flow and store the extracted Nøgletal in the database.

    :param data_interval_end: End of the Airflow DAG run interval (UTC), used as run date.
    """
    run_date = data_interval_end.in_timezone("Europe/Copenhagen").date()
    logger.info(f"Starting BOM processing for run date {run_date}")

    bom_conn = BaseHook.get_connection("bom_login")
    bom_url = bom_conn.host
    username = bom_conn.login
    password = bom_conn.password
    if not username or not password or not bom_url:
        raise AirflowFailException("Connection 'bom_login' is missing login and/or password or bom_url")

    df_monthly, df_glidende = run_bom_job(
        username=username,
        password=password,
        bom_url=bom_url,
        run_date=run_date,
    )
    if df_monthly is None or df_glidende is None or df_monthly.empty or df_glidende.empty:
        raise AirflowFailException("BOM job failed to extract data")

    engine = PostgresHook(postgres_conn_id="byggesager").get_sqlalchemy_engine()
    with engine.begin() as connection:
        df_monthly.to_sql("bom_data_monthly", con=connection, if_exists="replace", index=False)
        df_glidende.to_sql("bom_data_glidende", con=connection, if_exists="replace", index=False)

    logger.info(
        f"BOM processing completed; inserted {len(df_monthly)} monthly row(s) and "
        f"{len(df_glidende)} glidende gennemsnit row(s)"
    )
