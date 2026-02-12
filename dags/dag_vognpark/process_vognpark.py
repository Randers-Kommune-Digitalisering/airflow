import logging
import pandas as pd

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dag_vognpark.vognpark_data import (
    read_vognpark_excel_from_sftp,
    get_latest_vognpark_excel_info,
)

logger = logging.getLogger(__name__)


def process_vognpark() -> None:
    """
    Fetch Excel from SFTP and load into Postgres.
    """
    shared_sftp_hook = SFTPHook(ssh_conn_id="shared_sftp")
    vognpark_hook = PostgresHook(postgres_conn_id="vognpark_db")

    latest_info = get_latest_vognpark_excel_info(shared_sftp_hook, directory="/Vognpark/")
    if not latest_info:
        logger.error("No Excel files found on SFTP")
        return

    excel_path, modified_at_utc = latest_info
    df = read_vognpark_excel_from_sftp(shared_sftp_hook, excel_path)

    engine = vognpark_hook.get_sqlalchemy_engine()
    logger.debug("Connected to Postgres")

    data_table = "vognpark_data"
    audit_table = "vognpark_file_audit"

    with engine.begin() as conn:
        df.to_sql(data_table, con=conn, if_exists="replace", index=False)

        audit_df = pd.DataFrame([{
            "file_path": excel_path,
            "modified_at_utc": modified_at_utc,
        }])

        audit_df.to_sql(audit_table, con=conn, if_exists="replace", index=False)

    logger.info("Vognpark Airflow DAG completed successfully!")
