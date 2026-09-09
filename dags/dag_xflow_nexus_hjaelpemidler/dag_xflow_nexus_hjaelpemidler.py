import logging
import json
from datetime import timedelta

from pendulum import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.config import DEFAULT_DAG_ARGS
from dag_xflow_nexus_hjaelpemidler.main_flow import get_xflow_data_add_to_nexus

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1
dag_args["retry_delay"] = timedelta(minutes=30)

logger = logging.getLogger(__name__)
VAR_NAME = "xflow_nexus_hjaelpemidler_tables"


def get_config_start_main_flow() -> None:
    """ Get configuration from Airflow variables and connections, and start the main flow of processing xFlow data to Nexus. """
    var_str = Variable.get(VAR_NAME, default_var=None)
    if var_str:
        try:
            tables = json.loads(var_str)
        except Exception as e:
            raise ValueError(f"Failed to deserialize '{VAR_NAME}' variable: {e}")

        meta_hook = PostgresHook(postgres_conn_id="meta_db")
        nexus_hook = BaseHook.get_hook("nexus_prod")
        xflow_hook = BaseHook.get_hook("xflow")

        get_xflow_data_add_to_nexus(tables=tables, meta_hook=meta_hook, nexus_hook=nexus_hook, xflow_hook=xflow_hook, var_name=VAR_NAME)

    else:
        raise ValueError(f"Variable '{VAR_NAME}' is not set or is empty")


with DAG(
    dag_id="xflow_nexus_hjaelpemidler",
    start_date=datetime(year=2026, month=9, day=8, tz=timezone("Europe/Copenhagen")),
    schedule="@hourly",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Check meta postgres db for new xFlow form data and send to Nexus",
    tags=["nexus", "xflow", "hjaelpemidler", "db", "database", "postgres"]
) as dag:
    task = PythonOperator(
        task_id="get_config_start_main_flow",
        python_callable=get_config_start_main_flow,
        do_xcom_push=False
    )
