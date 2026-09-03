import json
import logging
import pendulum
from datetime import timedelta
from pendulum import datetime, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.models import Variable, DagRun
from airflow.hooks.base import BaseHook
from airflow.utils.email import send_email
from airflow.utils.state import DagRunState

from utils.config import DEFAULT_DAG_ARGS
from utils.custom_log import get_log_collector, get_styled_log_html
from dag_sd_delta.extract_transform import get_and_transform_changes
from dag_sd_delta.load import upload_excel_file_to_delta, handle_deleted_employments
from dag_sd_delta.delta_client import DeltaClient
from dag_sd_delta.utils import validate_insts_to_import


dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["email_on_failure"] = True
dag_args["retries"] = 2
dag_args["retry_delay"] = timedelta(minutes=5)
dag_args["email"].append("delta@randers.dk")

logger = logging.getLogger(__name__)


def get_start_time_since_last_success(context: dict) -> pendulum.DateTime | None:
    """Returns the data_interval_end of the last successful run for this DAG, so failed/skipped runs don't create gaps."""
    dag_id = context["dag"].dag_id
    current_run_id = context["dag_run"].run_id

    past_runs = DagRun.find(dag_id=dag_id, state=DagRunState.SUCCESS)
    past_runs = [run for run in past_runs if run.run_id != current_run_id and run.data_interval_end]
    if not past_runs:
        return None

    last_success = max(past_runs, key=lambda run: run.data_interval_end)
    return last_success.data_interval_end


def extract_transform(**context: dict) -> None:
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
        start_time = get_start_time_since_last_success(context) or context.get('data_interval_start')
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

        # Upload in the same task that creates the file to avoid cross-pod file path issues on Kubernetes.
        upload_result = upload_excel_file_to_delta(
            delta_client=DeltaClient(BaseHook.get_connection("delta_prod")),
            file_path=result.get("report_path")
        )
        result.update(upload_result)

        deleted_result = handle_deleted_employments(
            delta_client=DeltaClient(BaseHook.get_connection("delta_prod")),
            deleted_employments=result.get("deleted_employments")
        )
        result["deleted_log_html"] = deleted_result.get("log_html")
    finally:
        root_logger.removeHandler(log_collector)

    # Build log html for email report.
    html_prefix = "".join([
        "<h3>Task log summary</h3>",
        "<pre style='white-space: pre-wrap; font-family: monospace;'>",
    ])
    styled_log_lines = get_styled_log_html(log_collector)
    result["log_html"] = html_prefix + styled_log_lines + "</pre>"

    email_subject = (
        "SD Delta sync report: "
        f"{result['start_time']} - {result['end_time']}"
    )
    email_html = "".join([
        result.get("upload_html") or "",
        result.get("deleted_log_html") or "",
        result.get("log_html") or "",
    ])
    email_kwargs = {
        "to": ["delta@randers.dk"],
        "subject": email_subject,
        "html_content": email_html,
    }
    report_path = result.get("report_path")
    if report_path:
        email_kwargs["files"] = [report_path]
    send_email(**email_kwargs)

    return None


with DAG(
    dag_id="sd_delta_employment_sync",
    start_date=datetime(year=2026, month=8, day=3, tz=timezone("Europe/Copenhagen")),
    schedule="0 6,12,16 * * *",
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
        python_callable=extract_transform,
        do_xcom_push=False
    )
