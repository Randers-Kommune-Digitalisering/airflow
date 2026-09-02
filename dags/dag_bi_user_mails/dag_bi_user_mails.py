import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_bi_user_mails.process_bi_user_mail import process_bi_user_mail

logger = logging.getLogger(__name__)

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1

with DAG(
    dag_id="dag_bi_user_mail",
    start_date=datetime(year=2026, month=8, day=6, tz=timezone("Europe/Copenhagen")),
    schedule="0 9 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Fetch BI users from sftp and send welcome emails to new users",
    tags=["bi_user_mail", "email", "sftp", "xlsx"],
) as dag:

    run_bi_user_mail = PythonOperator(
        task_id="process_bi_user_mail",
        python_callable=process_bi_user_mail,
        do_xcom_push=False
    )
