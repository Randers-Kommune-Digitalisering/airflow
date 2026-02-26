
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_meddb_person_check.check_and_update_persons import check_and_update_persons

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


with DAG(
    dag_id="dag_meddb_person_check",
    start_date=datetime(year=2025, month=12, day=16, tz=timezone("Europe/Copenhagen")),
    schedule="@weekly",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Check and update MedDB person records by querying Delta, MS Graph, and Skole-AD",
    tags=['meddb', 'delta', 'ms_graph', 'meta_db',],
) as dag:

    task = PythonOperator(
        task_id="check_and_update_persons_task",
        python_callable=check_and_update_persons
    )
