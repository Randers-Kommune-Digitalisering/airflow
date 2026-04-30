from airflow import DAG
from airflow.operators.python import PythonOperator

from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0

with DAG(
    dag_id="serviceplatformen_test_python",
    start_date=datetime(year=2026, month=4, day=28, tz=timezone("Europe/Copenhagen")),
    schedule=None,
    catchup=False,
    default_args=dag_args,
    description="Testing serviceplatformen integration",
    tags=["serviceplatformen", "test", "kombit", "python"],
) as dag:
    from dag_serviceplatformen_test.testing import test

    run_python = PythonOperator(
        task_id="run_python",
        python_callable=test,
    )
