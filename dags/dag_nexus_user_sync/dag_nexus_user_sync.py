from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.http.hooks.http import BaseHook
# import pendulum
from pendulum import datetime, timezone
# import json
# from airflow.models import Variable
from utils.config import DEFAULT_DAG_ARGS
# from dag_nexus_user_sync.delta import DeltaClient

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


def print_nexus_adm_org_list():
    # TODO: Implement - just a placeholder for now
    pass

    # var_ids_to_import = ["nexus_adm_org_dict", "nexus_job_functions_to_import", "nexus_position_types_to_import"]
    # delta_client_params = {}
    # for var_id in var_ids_to_import:
    #     var_str = Variable.get(var_id, default_var=None)
    #     if var_str:
    #         try:
    #             var = json.loads(var_str)
    #         except Exception as e:
    #             raise ValueError(f"Failed to deserialize '{var_id}' variable: {e}")
    #     else:
    #         raise ValueError(f"Variable '{var_id}' is not set or is empty")
    #     param_name = var_id.replace("nexus_", "")
    #     delta_client_params[param_name] = var
    # delta_client_params["changes_date"] = pendulum.DateTime.now(timezone("Europe/Copenhagen")).date()
    # delta_hook = BaseHook.get_connection('delta_prod')
    # delta_client = DeltaClient(delta_hook=delta_hook, **delta_client_params)


with DAG(
    dag_id="dag_nexus_user_sync",
    start_date=datetime(year=2026, month=3, day=20, tz=timezone("Europe/Copenhagen")),
    schedule="*/10 * * * *",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Check Delta for employment changes and update users in Nexus accordingly",
    tags=['nexus', 'delta', 'permission', 'sync',]
) as dag:

    task = PythonOperator(
        task_id="check_and_update_users_task",
        python_callable=print_nexus_adm_org_list
    )
