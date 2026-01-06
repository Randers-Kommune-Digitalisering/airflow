import logging

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dag_sensum.sensum_data import fetch_and_store_sensum_data, create_merge_lambda
from dag_sensum.sensum_config import SENSUM_CONFIG

logger = logging.getLogger(__name__)


def process_sensum() -> None:
    sensum_db_hook = PostgresHook(postgres_conn_id="sensum_db")
    sensum_db_engine = sensum_db_hook.get_sqlalchemy_engine()

    for config in SENSUM_CONFIG:
        sensum_sftp_hook = SFTPHook(ftp_conn_id="sensum_sftp")
        merge_lambda = create_merge_lambda(config)
        fetch_and_store_sensum_data(
            sftp_hook=sensum_sftp_hook,
            db_engine=sensum_db_engine,
            file_patterns=config["patterns"],
            directories=config["directories"],
            merge_func=merge_lambda,
            output_table=config["name"],
        )
