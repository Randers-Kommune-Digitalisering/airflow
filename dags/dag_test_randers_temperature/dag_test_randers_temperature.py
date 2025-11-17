from pendulum import timezone
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_test_randers_temperature.weather_api import get_current_temperature
from dag_test_randers_temperature.log_result import log_temperature
from utils.config import DEFAULT_DAG_ARGS


with DAG(
    dag_id='dag_test_randers_temperature',
    description='Gets the current temperature in Randers from Open-Meteo API',
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval='0 * * * *',
    start_date=datetime(2025, 11, 17, tzinfo=timezone('Europe/Copenhagen')),
    catchup=False,
    max_active_runs=1,
    tags=['test', 'example']
) as dag:
    get_temperature_task = PythonOperator(
        task_id='get_randers_temperature',
        python_callable=get_current_temperature,
        op_kwargs={
            'latitude': 56.4609,
            'longitude': 10.0366
        }
    )

    log_temperature_task = PythonOperator(
        task_id='log_randers_temperature',
        python_callable=log_temperature
    )

    get_temperature_task >> log_temperature_task
