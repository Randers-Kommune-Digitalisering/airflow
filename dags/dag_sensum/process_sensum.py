import logging

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dag_sensum.sensum_data import get_files, files_to_postgres
from dag_sensum.sensum_config import SENSUM_CONFIG

logger = logging.getLogger(__name__)


def process_sensum() -> None:
    sensum_db_hook = PostgresHook(postgres_conn_id="sensum_db")
    sensum_db_engine = sensum_db_hook.get_sqlalchemy_engine()

    for config in SENSUM_CONFIG:
        with SFTPHook(ssh_conn_id="sensum_sftp").get_conn() as sftp_conn:
            logger.info(f"Processing Sensum data for table: {config['name']}")
            filter = config.get("filter", None)
            file_paths = get_files(
                sftp_conn=sftp_conn,
                dir=config["dir"],
                pattern=config["pattern"]
            )

            if "sec_pattern" in config:
                sec_prefix = config.get("sec_prefix", None)
                sec_file_paths = get_files(
                    sftp_conn=sftp_conn,
                    dir=config['dir'],
                    pattern=config['sec_pattern']
                )

                files_to_postgres(
                    db_engine=sensum_db_engine,
                    table_name=config['name'],
                    key_col=config['key_col'],
                    cols=config["cols"],
                    file_paths=file_paths,
                    sec_cols=config['sec_cols'],
                    sec_file_paths=sec_file_paths,
                    merge_on=config['merge_on'],
                    sec_prefix=sec_prefix,
                    filter=filter
                )
            else:
                files_to_postgres(
                    db_engine=sensum_db_engine,
                    key_col=config['key_col'],
                    table_name=config['name'],
                    cols=config["cols"],
                    file_paths=file_paths,
                    filter=filter
                )
