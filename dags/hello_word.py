from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import pendulum

local_tz = pendulum.timezone("Europe/Copenhagen")


def say_hello():
    logger = logging.getLogger("airflow.task")
    logger.info("This is an info message: Hello, world!")
    logger.warning("This is a warning message: Something might need attention.")
    logger.error("This is an error message: Something went wrong (simulated).")


with DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"]
) as dag:
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello
    )
