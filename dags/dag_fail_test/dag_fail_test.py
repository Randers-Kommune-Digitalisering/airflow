from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS

dag_args = DEFAULT_DAG_ARGS.copy()

with DAG(
    dag_id="dag_fail_test",
    start_date=datetime(year=2025, month=12, day=16, tz=timezone("Europe/Copenhagen")),
    schedule="@daily",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="DAG to test failure handling",
    tags=['test', 'testing', 'fail', 'failure'],
) as dag:

    task = PythonOperator(
        task_id="fail_task",
        python_callable=lambda: 1 / 0
    )
