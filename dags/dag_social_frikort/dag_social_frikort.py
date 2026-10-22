from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_social_frikort.process_social_frikort import process_social_frikort

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_social_frikort",
    start_date=datetime(year=2026, month=10, day=22, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for social_frikort",
    tags=["social_frikort", "<tag1>", "<tag2>"],
) as dag:

    run_social_frikort = PythonOperator(
        task_id="process_social_frikort_task",
        python_callable=process_social_frikort,
    )
