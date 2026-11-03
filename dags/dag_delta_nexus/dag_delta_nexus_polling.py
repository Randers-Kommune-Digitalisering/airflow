import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import DagRun
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.session import provide_session

from dag_delta_nexus.delta import DeltaClient
from utils.config import DEFAULT_DAG_ARGS


logger = logging.getLogger(__name__)
dag_args = {**DEFAULT_DAG_ARGS, "retries": 2, "retry_delay": timedelta(minutes=2)}


@provide_session
def processing_cooldown_elapsed(session) -> bool:
    cutoff = pendulum.now("UTC").subtract(minutes=10)
    return session.query(DagRun.id).filter(
        DagRun.dag_id == "delta_nexus_processing",
        DagRun.start_date >= cutoff,
    ).first() is None


def discover_nexus_users(**context) -> dict:
    """Batch unprocessed engagement registrations with Nexus users."""
    interval_end = pendulum.instance(context["data_interval_end"]).in_timezone("UTC")
    local_interval_end = interval_end.in_timezone("Europe/Copenhagen")
    valid_date = local_interval_end.date()
    last_success = context.get("prev_data_interval_end_success")
    lookback_start = interval_end.subtract(hours=1)
    if last_success is not None:
        lookback_start = min(lookback_start, pendulum.instance(last_success).in_timezone("UTC"))

    delta_conn = BaseHook.get_connection("delta_prod")
    delta_client = DeltaClient(
        base_url=delta_conn.host,
        token_url=delta_conn.extra_dejson["token_url"],
        client_id=delta_conn.login,
        client_secret=delta_conn.password,
    )
    registrations = list(delta_client.get_engagement_uuids_changed_since(lookback_start, valid_date))
    if last_success is not None:
        start_date = pendulum.instance(last_success).in_timezone("Europe/Copenhagen").date() + timedelta(days=1)
    else:
        start_date = valid_date if local_interval_end.hour == 0 else valid_date + timedelta(days=1)
    if local_interval_end.hour == 0:
        start_date = min(start_date, valid_date)
    while start_date <= valid_date:
        registrations.extend(delta_client.get_engagement_uuids_valid_from(start_date))
        start_date += timedelta(days=1)

    existing_runs = DagRun.find(
        dag_id="delta_nexus_processing",
        execution_start_date=lookback_start,
    )
    triggered_ids = {
        str(registration_id)
        for run in existing_runs
        for registration_id in (run.conf or {}).get("registration_ids", [])
    }
    pending = {
        str(registration["id"]): registration["objectUuid"]
        for registration in registrations
        if str(registration["id"]) not in triggered_ids
    }
    users_by_engagement = {
        engagement_uuid: delta_client.get_nexus_user_uuids_for_engagements([engagement_uuid], valid_date)
        for engagement_uuid in dict.fromkeys(pending.values())
    }
    ready_ids = [registration_id for registration_id, engagement_uuid in pending.items() if users_by_engagement[engagement_uuid]]
    user_uuids = list(dict.fromkeys(
        user
        for registration_id in ready_ids
        for user in users_by_engagement[pending[registration_id]]
    ))
    logger.info("Delta poll found %d pending registrations and %d Nexus users", len(pending), len(user_uuids))
    return {"registration_ids": ready_ids, "user_uuids": user_uuids}


def has_nexus_users(**context) -> bool:
    """Skip processing unless the current batch contains Nexus users."""
    return bool(context["ti"].xcom_pull(task_ids="discover_nexus_users")["user_uuids"])


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
    processing_cooldown = PythonSensor(
        task_id="processing_cooldown",
        python_callable=processing_cooldown_elapsed,
        mode="reschedule",
        poke_interval=30,
    )
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
            "registration_ids": "{{ ti.xcom_pull(task_ids='discover_nexus_users')['registration_ids'] }}",
            "user_uuids": "{{ ti.xcom_pull(task_ids='discover_nexus_users')['user_uuids'] }}",
            "valid_date": "{{ data_interval_end.in_timezone('Europe/Copenhagen').to_date_string() }}",
        },
        wait_for_completion=True,
        reset_dag_run=False,
        skip_when_already_exists=True,
        retries=0,
    )
    processing_cooldown >> discovery >> users_found >> trigger_processing
