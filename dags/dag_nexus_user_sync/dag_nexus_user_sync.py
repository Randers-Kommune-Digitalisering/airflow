import json
import logging
import pendulum
from datetime import timedelta
from pendulum import datetime, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from utils.config import DEFAULT_DAG_ARGS
from dag_nexus_user_sync.delta import DeltaClient
from dag_nexus_user_sync.nexus import NexusClient

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 2
dag_args["retry_delay"] = timedelta(minutes=2)

logger = logging.getLogger(__name__)


def get_config_start_main_flow(**context):
    changes_date_string = context['params'].get('changes_date')
    if changes_date_string:
        changes_date = pendulum.parse(changes_date_string, exact=True)
    else:
        changes_date = pendulum.DateTime.now(timezone("Europe/Copenhagen")).date()

    logger.info(f"Starting '{context['dag'].dag_id}' with the changes_date set to: {changes_date}")

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
    employment_changes = delta_client.get_employment_changes()

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
    
    nexus_client.import_to_nexus_and_set_permissions(employees_changed_list=employment_changes)


with DAG(
    dag_id="nexus_user_permission_sync",
    start_date=datetime(year=2026, month=3, day=20, tz=timezone("Europe/Copenhagen")),
    schedule="*/10 * * * *",
    default_args=dag_args,
    catchup=False,
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
    description="Check Delta for employment changes and update users in Nexus accordingly",
    tags=['nexus', 'delta', 'permission', 'sync', 'user']
) as dag:
    task = PythonOperator(
        task_id="check_and_update_users_task",
        python_callable=get_config_start_main_flow
    )
