
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from utils.config import DEFAULT_DAG_ARGS
from dag_meddb_person_check.check_and_update_persons import check_and_update_persons

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1

with DAG(
    dag_id="dag_meddb_person_check",
    start_date=datetime(2025, 12, 3),
    schedule_interval="0 12 * * 0",
    default_args=dag_args,
    catchup=False,
    description="Check and update MedDB person records by querying Delta, MS Graph, and Skole-AD",
    tags=['meddb', 'delta', 'ms_graph', 'meta_db',],
) as dag:

    task = PythonOperator(
        task_id="check_and_update_persons_task",
        python_callable=check_and_update_persons
    )
