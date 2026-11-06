from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from airflow.models.param import Param

from utils.config import DEFAULT_DAG_ARGS
from dag_social_frikort.process_social_frikort import process_social_frikort

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


with DAG(
    dag_id="social_frikort_income_and_address_report",
    start_date=datetime(year=2026, month=10, day=22, tz=timezone("Europe/Copenhagen")),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Generates and emails an income and address report for Socialt Frikort",
    tags=["social_frikort", "serviceplatform", "skat", "address", "mailbox", "excel"],
    params={
            "start_month": Param(
                type=["string"],
                minLength=6,
                maxLength=6,
                description="Påkrævet startmåned i format YYYYMM, fx 202604",
            ),
            "end_month": Param(
                type=["string"],
                minLength=6,
                maxLength=6,
                description="Påkrævet slutmåned i format YYYYMM, fx 202606",
            ),
        },
) as dag:

    run_social_frikort = PythonOperator(
        task_id="process_social_frikort_task",
        python_callable=process_social_frikort,
    )
