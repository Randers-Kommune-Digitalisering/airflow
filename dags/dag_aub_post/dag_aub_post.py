from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_aub_post.process_aub_post import process_aub_post

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_aub_post",
    start_date=datetime(year=2026, month=6, day=9, tz=timezone("Europe/Copenhagen")),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Route AUB post emails by education extracted from PDF attachments",
    tags=["aub", "email", "pdf", "routing"],
) as dag:

    run_aub_post = PythonOperator(
        task_id="process_aub_post_task",
        python_callable=process_aub_post,
    )
