"""Schedule daily replacement of configured X-Flow value lists."""

from datetime import timedelta

from pendulum import datetime, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from dag_xflow_value_lists.sync_value_lists import sync_value_lists
from utils.config import DEFAULT_DAG_ARGS


VAR_NAME = "xflow_value_lists_config"
dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1
dag_args["retry_delay"] = timedelta(minutes=30)


def run_sync() -> None:
    """
    Load the runtime configuration and start the value-list sync.

    :return: None.
    :raises ValueError: If the runtime configuration is missing or empty.
    """
    config = Variable.get(VAR_NAME, default_var=None)
    if not config:
        raise ValueError(f"Variable '{VAR_NAME}' is missing or empty")

    sync_value_lists(config)


with DAG(
    dag_id="xflow_value_lists",
    start_date=datetime(2026, 11, 19, tz=timezone("Europe/Copenhagen")),
    schedule="0 6 * * *",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Replace configured X-Flow value lists from approved sources",
    tags=["xflow", "value-lists"],
) as dag:
    PythonOperator(
        task_id="sync_value_lists",
        python_callable=run_sync,
        do_xcom_push=False,
    )
