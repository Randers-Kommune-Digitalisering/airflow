from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_mailbox_cleaner.process_mailbox_cleaner import process_mailbox_cleaner

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_mailbox_cleaner",
    start_date=datetime(year=2026, month=7, day=21, tz=timezone("Europe/Copenhagen")),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Placeholder description for mailbox_cleaner",
    tags=["mailbox_cleaner", "imap", "mail", "email", "cleaner"],
) as dag:

    run_mailbox_cleaner = PythonOperator(
        task_id="process_mailbox_cleaner_task",
        python_callable=process_mailbox_cleaner,
    )
