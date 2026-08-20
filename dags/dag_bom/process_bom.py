import logging

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dag_bom.bom_data import run_bom_job

logger = logging.getLogger(__name__)


def process_bom() -> None:
    """
    Run the BOM browser flow and store the extracted Nøgletal in the database.
    """
    logger.info("Starting BOM processing")

    bom_conn = BaseHook.get_connection("bom_login")
    username = bom_conn.login
    password = bom_conn.password
    if not username or not password:
        raise AirflowFailException(
            "Connection 'bom' is missing host, username or password"
        )

    df_monthly, df_glidende = run_bom_job(
        username=username,
        password=password,
    )

    engine = PostgresHook(postgres_conn_id="byggesager").get_sqlalchemy_engine()
    with engine.begin() as connection:
        df_monthly.to_sql("bom_data_monthly", con=connection, if_exists="append", index=False)
        df_glidende.to_sql("bom_data_glidende", con=connection, if_exists="append", index=False)

    logger.info(
        f"BOM processing completed; inserted {len(df_monthly)} monthly row(s) and "
        f"{len(df_glidende)} glidende gennemsnit row(s)"
    )
