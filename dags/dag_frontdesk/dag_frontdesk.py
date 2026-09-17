from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_frontdesk.process_frontdesk import process_frontdesk

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_frontdesk",
    start_date=datetime(year=2026, month=9, day=15, tz=timezone(
        "Europe/Copenhagen")),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for frontdesk",
    tags=["frontdesk", "<tag1>", "<tag2>"],
) as dag:

    run_frontdesk = PythonOperator(
        task_id="process_frontdesk_task",
        python_callable=process_frontdesk,
    )
