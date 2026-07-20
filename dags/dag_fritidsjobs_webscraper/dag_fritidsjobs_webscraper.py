from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_fritidsjobs_webscraper.process_fritidsjobs_webscraper import process_fritidsjobs_webscraper

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


with DAG(
    dag_id="dag_fritidsjobs_webscraper",
    start_date=datetime(year=2026, month=7, day=9, tz=timezone("Europe/Copenhagen")),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for fritidsjobs_webscraper",
    tags=["fritidsjobs_webscraper", "rpa", "postgresql", "webscraper"],
) as dag:

    run_fritidsjobs_webscraper = PythonOperator(
        task_id="process_fritidsjobs_webscraper_task",
        python_callable=process_fritidsjobs_webscraper,
    )
