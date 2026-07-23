from airflow import DAG
from airflow.models import Variable
from airflow.models.variable import Variable as VariableModel
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session
from pendulum import datetime, timezone
import re

from utils.config import DEFAULT_DAG_ARGS
from dag_mailbox_cleaner.process_mailbox_cleaner import process_mailbox_cleaner

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


def _get_mailbox_cleaner_config_keys(prefix: str = "mailbox_cleaner_conf_"):
    with create_session() as session:
        return [
            row.key
            for row in session.query(VariableModel.key)
            .filter(VariableModel.key.like(f"{prefix}%"))
            .order_by(VariableModel.key)
            .all()
        ]


def _run_mailbox_cleaner_for_config(config_key: str):
    config = Variable.get(config_key, deserialize_json=True)
    return process_mailbox_cleaner(config)


def _to_task_id(config_key: str) -> str:
    suffix = config_key.replace("mailbox_cleaner_conf_", "", 1)
    suffix = re.sub(r"[^a-zA-Z0-9_]+", "_", suffix).strip("_").lower() or "default"
    return f"process_mailbox_cleaner_{suffix}"


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
    config_keys = _get_mailbox_cleaner_config_keys()

    previous_task = None
    for config_key in config_keys:
        current_task = PythonOperator(
            task_id=_to_task_id(config_key),
            python_callable=_run_mailbox_cleaner_for_config,
            op_kwargs={"config_key": config_key},
        )

        if previous_task:
            previous_task >> current_task

        previous_task = current_task

    if not config_keys:
        run_mailbox_cleaner = PythonOperator(
            task_id="process_mailbox_cleaner_task",
            python_callable=process_mailbox_cleaner,
        )
