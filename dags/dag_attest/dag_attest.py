from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_attest.process_attest import process_attest

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_attest",
    start_date=datetime(year=2026, month=10, day=26, tz=timezone("Europe/Copenhagen")),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for attest",
    tags=["attest", "<tag1>", "<tag2>"],
) as dag:

    run_attest = PythonOperator(
        task_id="process_attest_task",
        python_callable=process_attest,
    )
