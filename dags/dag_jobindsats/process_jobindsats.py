
import logging

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dag_jobindsats.jobindsats_data import fetch_and_store_table_updates, get_data
from dag_jobindsats.jobindsats_config import JOBINDSATS_CONFIG

logger = logging.getLogger(__name__)


def process_jobindsats() -> None:
    jobindsats_http_hook = HttpHook(http_conn_id="jobindsats_api")
    jobindsats_db_hook = PostgresHook(postgres_conn_id="jobindsats_db")
    jobindsats_db_engine = jobindsats_db_hook.get_sqlalchemy_engine()

    fetch_and_store_table_updates(http_hook=jobindsats_http_hook, db_engine=jobindsats_db_engine)
    for job in JOBINDSATS_CONFIG:
        get_data(
            http_hook=jobindsats_http_hook,
            db_engine=jobindsats_db_engine,
            name=job['name'],
            years_back=job['years_back'],
            dataset=job['dataset'],
            period_format=job['period_format'],
            data_to_get=job['data_to_get']
        )
