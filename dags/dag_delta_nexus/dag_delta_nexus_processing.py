import logging
from datetime import date, timedelta

import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

from dag_delta_nexus.delta import DeltaClient
from dag_delta_nexus.nexus import NexusClient, DeltaPerson, PreviewCreationSkipped
from utils.config import DEFAULT_DAG_ARGS
from utils.custom_log import get_log_collector, get_styled_log_html


logger = logging.getLogger(__name__)
dag_args = {**DEFAULT_DAG_ARGS, "retries": 2, "retry_delay": timedelta(minutes=2)}


def fetch_user_details(**context) -> None:
    """Process selected Nexus users and email the client log."""
    conf = context["dag_run"].conf or {}
    user_uuids = conf.get("user_uuids")
    if not isinstance(user_uuids, list) or not user_uuids or not all(
        isinstance(uuid, str) for uuid in user_uuids
    ):
        raise ValueError("Expected a non-empty list of user UUIDs")
    valid_date = date.fromisoformat(conf["valid_date"])

    delta_conn = BaseHook.get_connection("delta_prod")
    delta_client = DeltaClient(
        base_url=delta_conn.host,
        token_url=delta_conn.extra_dejson["token_url"],
        client_id=delta_conn.login,
        client_secret=delta_conn.password,
    )

    user_details = delta_client.get_user_details(user_uuids, valid_date)
    nexus_persons = [DeltaPerson.from_dict(user) for user in user_details]
    log_collector = get_log_collector()
    log_collector.setLevel(logging.INFO)
    client_logger = logging.getLogger(NexusClient.__module__)
    client_logger.addHandler(log_collector)
    logger.addHandler(log_collector)
    failed_users = []
    try:
        nexus_conn = BaseHook.get_connection("nexus_prod")
        nexus_client = NexusClient(
            base_url=nexus_conn.host,
            token_url=nexus_conn.extra_dejson["token_url"],
            client_id=nexus_conn.login,
            client_secret=nexus_conn.password,
        )
        for person in nexus_persons:
            logger.info(f"Handling: {person.name}")
            try:
                updated = nexus_client.update_existing_professional(person=person)
            except PreviewCreationSkipped:
                logger.warning("Professional creation intentionally skipped in preview mode")
            except Exception as error:
                failed_users.append(person.user.ad_guid)
                logger.error(f"Professional update failed ({type(error).__name__})")
            else:
                if updated is not True:
                    failed_users.append(person.user.ad_guid)
                    logger.error("Professional update did not succeed")
    finally:
        client_logger.removeHandler(log_collector)
        logger.removeHandler(log_collector)

    if log_collector.entries:
        sent_at = pendulum.now("Europe/Copenhagen").strftime("%H:%M %d-%m-%Y")
        send_email(
            to=["rune.aagaard.keena@randers.dk", "Jane.Scharling.Andersen@randers.dk", "Natashia.Maise.Nygaard@randers.dk"],
            subject=f"NO CHANGES MADE - Delta Nexus processing - {sent_at}",
            html_content=get_styled_log_html(log_collector).replace("\n", "<br>"),
        )

    if failed_users:
        raise RuntimeError(f"Failed to update {len(failed_users)} Nexus professionals")


with DAG(
    dag_id="delta_nexus_processing",
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    schedule=None,
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    description="Process Nexus users affected by Delta changes",
    tags=["delta", "nexus", "processing", "user"],
) as dag:
    PythonOperator(
        task_id="fetch_user_details",
        python_callable=fetch_user_details,
        do_xcom_push=False,
        show_return_value_in_logs=False,
    )
