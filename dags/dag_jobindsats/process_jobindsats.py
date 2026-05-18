
import logging

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Any
from dag_jobindsats.jobindsats_data import fetch_and_store_table_updates, get_data
from airflow.models import Variable
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


def _load_jobindsats_config() -> list[dict[str, Any]]:
    variable_value: Any = Variable.get("jobindsats_config", default_var=None, deserialize_json=True,)

    if variable_value is None:
        raise AirflowException("Airflow Variable 'jobindsats_config' is missing or empty")

    config_list: Any
    if isinstance(variable_value, dict):
        config_list = variable_value.get("jobindsats_config")
    else:
        config_list = variable_value

    if not isinstance(config_list, list):
        raise AirflowException("Airflow Variable 'jobindsats_config' must be a JSON list, ")

    return config_list


def process_jobindsats() -> None:
    jobindsats_http_hook = HttpHook(http_conn_id="jobindsats_api")
    jobindsats_db_hook = PostgresHook(postgres_conn_id="jobindsats_db")
    jobindsats_db_engine = jobindsats_db_hook.get_sqlalchemy_engine()

    fetch_and_store_table_updates(http_hook=jobindsats_http_hook, db_engine=jobindsats_db_engine)
    for job in _load_jobindsats_config():
        kwargs = {
            "http_hook": jobindsats_http_hook,
            "db_engine": jobindsats_db_engine,
            "name": job['name'],
            "years_back": job['years_back'],
            "dataset": job['dataset'],
            "period_format": job['period_format'],
            "data_to_get": job['data_to_get'],
        }
        if job.get('id'):
            kwargs["id"] = job['id']
        get_data(**kwargs)
