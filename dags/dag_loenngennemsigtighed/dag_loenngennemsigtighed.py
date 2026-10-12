from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_loenngennemsigtighed.process_loenngennemsigtighed import process_loenngennemsigtighed

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_loenngennemsigtighed",
    start_date=datetime(year=2026, month=10, day=12, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for loenngennemsigtighed",
    tags=["loenngennemsigtighed", "<tag1>", "<tag2>"],
) as dag:

    run_loenngennemsigtighed = PythonOperator(
        task_id="process_loenngennemsigtighed_task",
        python_callable=process_loenngennemsigtighed,
    )
