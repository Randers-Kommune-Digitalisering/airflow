from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_absence_post_routing_and_journalize.process_absence_post_routing_and_journalize import (
    extract_cpr_from_maindoc_attachments
)

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


with DAG(
    dag_id="absence_post_forward_mails",
    start_date=datetime(year=2026, month=9, day=14, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Forward absence post emails based on department mapping",
    tags=["sd-recipients", "forward mail", "delta"],
) as dag:

    extract_cpr_from_maindoc_attachments_task = PythonOperator(
        task_id="extract_cpr_from_maindoc_attachments_task",
        python_callable=extract_cpr_from_maindoc_attachments,
        do_xcom_push=False,
    )
