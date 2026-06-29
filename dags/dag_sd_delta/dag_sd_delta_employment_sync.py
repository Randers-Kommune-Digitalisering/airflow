import json
import logging
import pendulum
from datetime import timedelta
from pendulum import datetime, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook

from utils.config import DEFAULT_DAG_ARGS
from utils.custom_log import get_log_collector, get_styled_log_html
from dag_sd_delta.extract_transform import get_and_transform_changes
from dag_sd_delta.load import upload_excel_file_to_delta, handle_deleted_employments
from dag_sd_delta.delta_client import DeltaClient
from dag_sd_delta.utils import validate_insts_to_import


dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["email_on_failure"] = True
dag_args["email"] = ["delta@randers.dk", "digitalisering@randers.dk"]
dag_args["retries"] = 2
dag_args["retry_delay"] = timedelta(minutes=5)

logger = logging.getLogger(__name__)


def extract_transform(**context: dict) -> dict[str, str | bool]:
    """Fetches and validates configuration from Airflow Variables and DAG params, then starts the main flow."""

    # set up internal log collector to capture logs for email report
    log_collector = get_log_collector()
    root_logger = logging.getLogger()
    root_logger.addHandler(log_collector)

    # Keep hook logger noise low in task logs (removes lines for each time a hook is used)
    logging.getLogger("airflow.hooks.base").setLevel(logging.WARNING)

    # Parameters - use data interval as default but allow override with DAG params
    start_time_string = context['params'].get('start_time')
    end_time_string = context['params'].get('end_time')

    if end_time_string and start_time_string:
        end_time = pendulum.parse(end_time_string, exact=True).replace(tzinfo=timezone("Europe/Copenhagen"))
        start_time = pendulum.parse(start_time_string, exact=True).replace(tzinfo=timezone("Europe/Copenhagen"))

        if start_time >= end_time:
            raise ValueError(
                "Invalid time window: start_time must be earlier than end_time. "
                f"Received start_time={start_time}, end_time={end_time}"
            )
    else:
        start_time = context.get('data_interval_start')
        end_time = context.get('data_interval_end')

    if not start_time or not end_time:
        raise ValueError(f"Start time and end time must be provided either through DAG params or data interval.Received start_time={start_time}, end_time={end_time}")

    end_time = pendulum.instance(end_time).in_timezone(timezone("Europe/Copenhagen"))
    start_time = pendulum.instance(start_time).in_timezone(timezone("Europe/Copenhagen"))
    logger.info(f"\nStarting '{context['dag'].dag_id}' with start_time: {start_time.strftime('%Y-%m-%dT%H:%M:%S')} and end_time: {end_time.strftime('%Y-%m-%dT%H:%M:%S')}")

    # Variable fetching and validation
    insts_to_import_raw = Variable.get("delta_sd_insts_to_import", default_var=None)
    insts_to_import = json.loads(insts_to_import_raw)
    validate_insts_to_import(insts_to_import)

    # Get and transform changes from SD
    try:
        result = get_and_transform_changes(
            insts_to_import=insts_to_import,
            start_time=start_time,
            end_time=end_time
        )
    finally:
        root_logger.removeHandler(log_collector)

    # Build log html for email report.
    html_prefix = "".join([
        "<h3>Task log summary</h3>",
        "<pre style='white-space: pre-wrap; font-family: monospace;'>",
    ])
    styled_log_lines = get_styled_log_html(log_collector)
    result["log_html"] = html_prefix + styled_log_lines + "</pre>"
    return result


with DAG(
    dag_id="sd_delta_employment_sync",
    start_date=datetime(year=2026, month=6, day=23, tz=timezone("Europe/Copenhagen")),
    schedule="0 7,12,15 * * *",
    render_template_as_native_obj=True,
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    params={
        "start_time": Param(
            default=None,
            type=["null", "string"],
            description=(
                "Start datetime to check for changes in employments from SD. "
                "Format is ISO datetime string (YYYY-MM-DDTHH:mm:ss). "
                "If not provided, defaults to data interval start."
            )
        ),
        "end_time": Param(
            default=None,
            type=["null", "string"],
            description=(
                "End datetime to check for changes in employments from SD. "
                "Format is ISO datetime string (YYYY-MM-DDTHH:mm:ss). "
                "If not provided, defaults to data interval end."
            )
        ),
    },
    description="Check SD for employment changes and sync those to Delta.",
    tags=['sd', 'silkeborgdata', 'delta', 'sync', 'employment']
) as dag:
    get_changes = PythonOperator(
        task_id="get_and_transform_changes",
        python_callable=extract_transform
    )

    handle_deleted = PythonOperator(
        task_id="handle_deleted_employment",
        python_callable=handle_deleted_employments,
        op_kwargs={
            "sd_http_hook": HttpHook(method="POST", http_conn_id="sd_silkeborgdata"),
            "delta_client": DeltaClient(BaseHook.get_connection("delta_prod")),
            "deleted_employments": "{{ ti.xcom_pull(task_ids='get_and_transform_changes')['deleted_employments'] }}"
        }
    )

    upload_file = PythonOperator(
        task_id="upload_excel_to_delta",
        python_callable=upload_excel_file_to_delta,
        op_kwargs={
            "delta_client": DeltaClient(BaseHook.get_connection("delta_prod")),
            "file_path": "{{ ti.xcom_pull(task_ids='get_and_transform_changes')['report_path'] }}"
        }
    )

    send_email = EmailOperator(
        task_id="send_email",
        to=["delta@randers.dk"],
        subject=(
            "SD Delta sync report: "
            "{{ ti.xcom_pull(task_ids='get_and_transform_changes')['start_time'] }}"
            " - "
            "{{ ti.xcom_pull(task_ids='get_and_transform_changes')['end_time'] }}"
        ),
        html_content=(
            "{{ ti.xcom_pull(task_ids='upload_excel_to_delta')['upload_html'] }}"
            "{{ ti.xcom_pull(task_ids='handle_deleted_employment')['log_html'] }}"
            "{{ ti.xcom_pull(task_ids='get_and_transform_changes')['log_html'] }}"
        ),
        files="{{ [ti.xcom_pull(task_ids='get_and_transform_changes')['report_path']] if ti.xcom_pull(task_ids='get_and_transform_changes')['report_path'] is not none else [] }}",
    )

    get_changes >> [upload_file, handle_deleted] >> send_email
