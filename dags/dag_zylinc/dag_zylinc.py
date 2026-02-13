from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_zylinc.process_zylinc import process_zylinc

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1

with DAG(
    dag_id="dag_zylinc",
    start_date=datetime(year=2025, month=12, day=8, tz=timezone("Europe/Copenhagen")),
    schedule="0 0 * * *",
    catchup=False,
    default_args=dag_args,
    description="Fetch Zylinc data from Elasticsearch and load it into Postgres",
    tags=["zylinc", "elasticsearch", "postgres"],
) as dag:

    run_zylinc = PythonOperator(
        task_id="process_zylinc_task",
        python_callable=process_zylinc
    )
