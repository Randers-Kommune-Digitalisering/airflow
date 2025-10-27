from pendulum import timezone
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import airflow_log_cleansup

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 15, tzinfo=timezone('Europe/Copenhagen')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='airflow_cleanup_logs',
    default_args=default_args,
    schedule_interval='0 23 * * *',
    catchup=False,
    description='Deletes oldest Airflow logs until disk usage is below threshold',
    tags=['airflow', 'maintenance']
) as dag:
    cleanup_task = PythonOperator(
        task_id='airflow_cleanup_logs',
        python_callable=airflow_log_cleansup.cleanup_logs_by_disk_usage,
        op_kwargs={
            'directory': '/opt/airflow/logs',
            'threshold_percent': 90
        }
    )
