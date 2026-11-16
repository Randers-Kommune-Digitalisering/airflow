from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_eindkomst.process_eindkomst import process_eindkomst

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_eindkomst",
    start_date=datetime(year=2026, month=11, day=16, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for eindkomst",
    tags=["eindkomst", "<tag1>", "<tag2>"],
) as dag:

    run_eindkomst = PythonOperator(
        task_id="process_eindkomst_task",
        python_callable=process_eindkomst,
    )
