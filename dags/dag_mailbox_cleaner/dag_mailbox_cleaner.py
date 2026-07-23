from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.variable import Variable as VariableModel
from airflow.utils.session import create_session
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_mailbox_cleaner.process_mailbox_cleaner import process_mailbox_cleaner

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1

CONFIG_PREFIX = "mailbox_cleaner_conf_"


def _get_mailbox_cleaner_config_keys(prefix: str = CONFIG_PREFIX) -> list[str]:
    with create_session() as session:
        return [
            row.key
            for row in session.query(VariableModel.key)
            .filter(VariableModel.key.like(f"{prefix}%"))
            .order_by(VariableModel.key)
            .all()
        ]


@task(task_id="list_mailbox_cleaner_config_keys")
def _list_mailbox_cleaner_config_keys(prefix: str = CONFIG_PREFIX) -> list[str]:
    """Discover mailbox-cleaner variable keys at task runtime."""
    return _get_mailbox_cleaner_config_keys(prefix=prefix)


@task(task_id="process_mailbox_cleaner", max_active_tis_per_dag=1)
def _run_mailbox_cleaner_for_config(config_key: str) -> None:
    config = Variable.get(config_key, deserialize_json=True)
    process_mailbox_cleaner(config)


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
    config_keys = _list_mailbox_cleaner_config_keys()
    _run_mailbox_cleaner_for_config.expand(config_key=config_keys)
