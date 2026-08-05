from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from airflow.models.param import Param

from utils.config import DEFAULT_DAG_ARGS
from dag_arbejdsfortjeneste.process_arbejdsfortjeneste import process_arbejdsfortjeneste

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


with DAG(
    dag_id="dag_arbejdsfortjeneste",
    start_date=datetime(year=2026, month=7, day=6, tz=timezone("Europe/Copenhagen")),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for arbejdsfortjeneste",
    tags=["arbejdsfortjeneste", "serviceplatform", "skat", "mailbox"],
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

    run_arbejdsfortjeneste = PythonOperator(
        task_id="process_arbejdsfortjeneste_task",
        python_callable=process_arbejdsfortjeneste,
        do_xcom_push=False
    )
