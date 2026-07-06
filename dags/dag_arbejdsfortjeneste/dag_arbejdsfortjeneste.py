from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_arbejdsfortjeneste.process_arbejdsfortjeneste import process_arbejdsfortjeneste

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_arbejdsfortjeneste",
    start_date=datetime(year=2026, month=7, day=6, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for arbejdsfortjeneste",
    tags=["arbejdsfortjeneste", "<tag1>", "<tag2>"],
) as dag:

    run_arbejdsfortjeneste = PythonOperator(
        task_id="process_arbejdsfortjeneste_task",
        python_callable=process_arbejdsfortjeneste,
    )