from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_bom.process_bom import process_bom

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_bom",
    start_date=datetime(year=2026, month=8, day=18, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for bom",
    tags=["bom", "<tag1>", "<tag2>"],
) as dag:

    run_bom = PythonOperator(
        task_id="process_bom_task",
        python_callable=process_bom,
    )
