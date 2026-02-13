from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS
from dag_jobindsats.process_jobindsats import process_jobindsats

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_jobindsats",
    start_date=datetime(year=2025, month=12, day=8, tz=timezone("Europe/Copenhagen")),
    schedule="0 0 * * 1",
    catchup=False,
    default_args=dag_args,
    description="Fetch Jobindsats data from API and load into Postgres",
    tags=["jobindsats", "api", "postgres"],
) as dag:

    run_jobindsats = PythonOperator(
        task_id="process_jobindsats_task",
        python_callable=process_jobindsats
    )
