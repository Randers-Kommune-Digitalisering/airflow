import json
import logging
import pendulum
from datetime import timedelta
from pendulum import datetime, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from utils.config import DEFAULT_DAG_ARGS
from dag_nexus_user_sync.delta import DeltaClient
from dag_nexus_user_sync.nexus import NexusClient

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1
dag_args["retry_delay"] = timedelta(minutes=30)

logger = logging.getLogger(__name__)


def get_config_start_main_flow(**context):
    changes_date_string = context['params'].get('changes_date')
    if changes_date_string:
        changes_date = pendulum.parse(changes_date_string, exact=True)
    else:
        changes_date = pendulum.DateTime.now(timezone("Europe/Copenhagen")).subtract(days=1).date()

    logger.info(f"Starting '{context['dag'].dag_id}' with the changes_date set to: {changes_date}")

    report_list = []

    # Airflow Variables to import and pass to DeltaClient
    delta_var_ids_to_import = ["nexus_adm_org_dict", "nexus_job_functions_to_import", "nexus_position_types_to_import"]
    delta_client_params = {}
    for var_id in delta_var_ids_to_import:
        var_str = Variable.get(var_id, default_var=None)
        if var_str:
            try:
                var = json.loads(var_str)
            except Exception as e:
                raise ValueError(f"Failed to deserialize '{var_id}' variable: {e}")
        else:
            raise ValueError(f"Variable '{var_id}' is not set or is empty")
        # Remove 'nexus_' prefix to get the parameter name for DeltaClient
        param_name = var_id.replace("nexus_", "")
        delta_client_params[param_name] = var
    delta_client_params["changes_date"] = changes_date

    delta_hook = BaseHook.get_connection('delta_prod')
    delta_client = DeltaClient(hook=delta_hook, **delta_client_params)
    employment_changes = delta_client.get_employment_changes(report_list=report_list)

    nexus_var_ids_to_import = ["nexus_adm_org_dict", "nexus_supplier_list"]
    nexus_client_params = {}
    for var_id in nexus_var_ids_to_import:
        var_str = Variable.get(var_id, default_var=None)
        if var_str:
            try:
                var = json.loads(var_str)
            except Exception as e:
                raise ValueError(f"Failed to deserialize '{var_id}' variable: {e}")
        else:
            raise ValueError(f"Variable '{var_id}' is not set or is empty")
        # Remove 'nexus_' prefix to get the parameter name for NexusClient
        param_name = var_id.replace("nexus_", "")
        nexus_client_params[param_name] = var

    nexus_hook = BaseHook.get_hook("nexus_review")
    nexus_client = NexusClient(hook=nexus_hook, **nexus_client_params)
    try:
        nexus_client.import_to_nexus_and_set_permissions(employees_changed_list=employment_changes, report_list=report_list)
    finally:
        nexus_client.logout()

    return report_list, changes_date.format("YYYY-MM-DD")


def report_list_to_html(**context):
    ti = context['ti']
    report_list, changes_date = ti.xcom_pull(task_ids='check_and_update_users_task')
    if not report_list:
        html_content = '<p>Ingen problemer blev fundet under synkroniseringen.</p>'
    else:
        html_content = '<ul>' + ''.join(f'<li>{item}</li>' for item in report_list) + '</ul>'
    logger.info(f"### The report for {changes_date} ###")
    logger.info(html_content)
    return html_content, changes_date


with DAG(
    dag_id="nexus_report_user_permission_sync",
    start_date=datetime(year=2026, month=4, day=22, tz=timezone("Europe/Copenhagen")),
    schedule="0 6 * * *",
    default_args=dag_args,
    catchup=True,
    max_active_runs=1,
    params={
        "changes_date": Param(
            default=None,
            type=["null", "string"],
            description=(
                "Date to check for changes in employments from Delta. "
                "If not provided, defaults to the current date. "
                "Format is ISO date string (YYYY-MM-DD)."
            )
        ),
    },
    description="Check Delta for employment changes and update users in Nexus accordingly - send report email with issues found during the sync",
    tags=['nexus', 'delta', 'report', 'permission', 'sync', 'user']
) as dag:
    task = PythonOperator(
        task_id="check_and_update_users_task",
        python_callable=get_config_start_main_flow
    )

    html_task = PythonOperator(
        task_id="make_report_html",
        python_callable=report_list_to_html,
        provide_context=True
    )

    send_email = EmailOperator(
        task_id="send_email",
        to=["digitalisering@randers.dk", "Jane.Scharling.Andersen@randers.dk"],
        subject="Nexus Delta Synkronisering Report - {{ ti.xcom_pull(task_ids='make_report_html')[1] }}",
        html_content="{{ ti.xcom_pull(task_ids='make_report_html')[0] }}",
    )

    task >> html_task >> send_email
