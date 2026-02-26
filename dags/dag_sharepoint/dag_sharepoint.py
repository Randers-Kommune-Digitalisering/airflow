from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


def task_process_sharepoint_list_items():
    from dag_sharepoint.process_sharepoint import process_sharepoint_list_items
    return process_sharepoint_list_items()


with DAG(
    dag_id="dag_sharepoint",
    start_date=datetime(year=2026, month=1, day=8, tz=timezone("Europe/Copenhagen")),
    schedule="0 0 * * *",
    catchup=False,
    default_args=dag_args,
    description="Fetch Sharepoint List data from MS Graph and load into Postgres",
    tags=["sharepoint", "msgraph", "postgres"],
) as dag:

    run_sharepoint = PythonOperator(
        task_id="process_sharepoint_task", python_callable=task_process_sharepoint_list_items
    )
