from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_absence_post_routing_and_journalize.process_absence_post_routing_and_journalize import process_absence_post_routing_and_journalize

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_absence_post_routing_and_journalize",
    start_date=datetime(year=2026, month=9, day=8, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for absence_post_routing_and_journalize",
    tags=["absence_post_routing_and_journalize", "<tag1>", "<tag2>"],
) as dag:

    run_absence_post_routing_and_journalize = PythonOperator(
        task_id="process_absence_post_routing_and_journalize_task",
        python_callable=process_absence_post_routing_and_journalize,
    )
