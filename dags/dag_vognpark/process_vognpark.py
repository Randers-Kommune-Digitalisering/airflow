import logging

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dag_vognpark.vognpark_data import (
    read_vognpark_excel_from_sftp,
    get_latest_vognpark_excel_path,
)

logger = logging.getLogger(__name__)


def process_vognpark() -> None:
    """
    Fetch Excel from SFTP and load into Postgres.
    """
    shared_sftp_hook = SFTPHook(ssh_conn_id="shared_sftp")
    vognpark_hook = PostgresHook(postgres_conn_id="vognpark_db")

    excel_path = get_latest_vognpark_excel_path(shared_sftp_hook)
    if not excel_path:
        logger.error("No Excel files found on SFTP")
        return

    df = read_vognpark_excel_from_sftp(shared_sftp_hook, excel_path)

    engine = vognpark_hook.get_sqlalchemy_engine()
    logger.debug("Connected to Postgres")

    table_name = "vognpark_data"
    logger.debug(f"Inserting data into table: {table_name}")

    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists="replace", index=False)

    logger.info("Vognpark Airflow DAG completed successfully!")
