import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dag_delta_nexus.delta import DeltaClient
from utils.config import DEFAULT_DAG_ARGS


logger = logging.getLogger(__name__)
dag_args = {**DEFAULT_DAG_ARGS, "retries": 2, "retry_delay": timedelta(minutes=2)}


def discover_nexus_users(**context) -> list[str]:
    """Find Nexus-role users linked to engagements changed since the last successful poll."""
    interval_end = context["data_interval_end"]
    last_success = context["prev_data_interval_end_success"]
    interval_start = last_success or context["data_interval_start"]
    valid_date = pendulum.instance(interval_end).in_timezone("UTC").date()

    delta_conn = BaseHook.get_connection("delta_prod")
    delta_client = DeltaClient(
        base_url=delta_conn.host,
        token_url=delta_conn.extra_dejson["token_url"],
        client_id=delta_conn.login,
        client_secret=delta_conn.password,
    )
    engagement_uuids = []
    start_date = pendulum.instance(interval_start).in_timezone("UTC").date()
    if last_success is not None:
        start_date += timedelta(days=1)
    while start_date <= valid_date:
        engagement_uuids.extend(delta_client.get_engagement_uuids_valid_from(start_date))
        start_date += timedelta(days=1)

    engagement_uuids.extend(delta_client.get_engagement_uuids_changed_since(interval_start, valid_date))
    engagement_uuids = list(dict.fromkeys(engagement_uuids))
    user_uuids = delta_client.get_nexus_user_uuids_for_engagements(engagement_uuids, valid_date) if engagement_uuids else []
    logger.info("Delta poll found %d changed engagements and %d Nexus users", len(engagement_uuids), len(user_uuids))
    return user_uuids


def has_nexus_users(**context) -> bool:
    """Skip processing unless an affected engagement has a Nexus-role user."""
    return bool(context["ti"].xcom_pull(task_ids="discover_nexus_users"))


with DAG(
    dag_id="delta_nexus_polling",
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    schedule="*/10 * * * *",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    is_paused_upon_creation=True,
    description="Poll Delta - trigger processing only for changed engagements with Nexus users",
    tags=["delta", "nexus", "polling", "user"],
) as dag:
    discovery = PythonOperator(
        task_id="discover_nexus_users",
        python_callable=discover_nexus_users,
        show_return_value_in_logs=False,
    )
    users_found = ShortCircuitOperator(
        task_id="has_nexus_users",
        python_callable=has_nexus_users,
        do_xcom_push=False,
    )
    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_processing",
        trigger_dag_id="delta_nexus_processing",
        trigger_run_id="polling__{{ dag_run.run_id }}",
        conf={
            "user_uuids": "{{ ti.xcom_pull(task_ids='discover_nexus_users') }}",
            "valid_date": "{{ data_interval_end.in_timezone('UTC').to_date_string() }}",
        },
        wait_for_completion=True,
        reset_dag_run=True,
    )
    discovery >> users_found >> trigger_processing
