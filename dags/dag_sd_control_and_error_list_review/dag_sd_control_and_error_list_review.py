from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_sd_control_and_error_list_review.process_sd_control_and_error_list_review import process_sd_control_and_error_list_review

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="sd_control_and_error_list_review",
    start_date=datetime(year=2026, month=8, day=24, tz=timezone("Europe/Copenhagen")),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for sd_control_and_error_list_review",
    tags=["sd_control_and_error_list_review", "<tag1>", "<tag2>"],
) as dag:

    run_sd_control_and_error_list_review = PythonOperator(
        task_id="process_sd_control_and_error_list_review_task",
        python_callable=process_sd_control_and_error_list_review,
    )
