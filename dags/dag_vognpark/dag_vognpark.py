from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_vognpark.process_vognpark import process_vognpark

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0

with DAG(
    dag_id="dag_vognpark",
    start_date=datetime(year=2025, month=12, day=8, tz=timezone("Europe/Copenhagen")),
    schedule_interval="0 0 * * 1",
    catchup=False,
    default_args=dag_args,
    description="Fetch latest Vognpark Excel from SFTP and load it into Postgres",
    tags=["vognpark", "sftp", "postgres"],
) as dag:

    run_vognpark = PythonOperator(
        task_id="process_vognpark_task",
        python_callable=process_vognpark
    )
