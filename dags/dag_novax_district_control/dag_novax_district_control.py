
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_novax_district_control.check_and_update_district import check_and_update_district

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0

with DAG(
    dag_id="dag_novax_district_control",
    start_date=datetime(year=2025, month=12, day=8, tz=timezone("Europe/Copenhagen")),
    schedule_interval="15 1 * * *",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Check and update Novax district records by querying relevant clients",
    tags=['novax', 'district', 'dataforsyning', 'cpr'],
) as dag:

    task = PythonOperator(
        task_id="check_and_update_district_task",
        python_callable=check_and_update_district,
        op_kwargs={}
    )
