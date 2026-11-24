from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS

from dag_eindkomst.process_eindkomst import process_eindkomst

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


with DAG(
    dag_id="dag_eindkomst",
    start_date=datetime(year=2026, month=11, day=16, tz=timezone("Europe/Copenhagen")),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Generates and emails an eIndkomst income report from a CPR text file",
    tags=["eindkomst", "serviceplatform", "skat", "mailbox", "excel"],
    params={
        "start_month": Param(type="string", minLength=6, maxLength=6, description="Startmåned YYYYMM"),
        "end_month": Param(type="string", minLength=6, maxLength=6, description="Slutmåned YYYYMM"),
    },
) as dag:

    run_eindkomst = PythonOperator(
        task_id="process_eindkomst_task",
        python_callable=process_eindkomst,
    )
