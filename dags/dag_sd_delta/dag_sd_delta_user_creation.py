import json
import logging
import requests
import pandas as pd
import pendulum
import tempfile

from pathlib import Path
from datetime import timedelta
from pendulum import datetime, timezone
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from utils.config import DEFAULT_DAG_ARGS
from utils.custom_log import get_log_collector, get_styled_log_html
from dag_sd_delta.sd import get_employment_on_date_df, get_person_on_date_df, get_professions_xml, get_institutions_df
from dag_sd_delta.extract_transform import build_output_df
from dag_sd_delta.load import upload_excel_file_to_delta
from dag_sd_delta.delta_client import DeltaClient
from dag_sd_delta.signflow import SignflowClient
from dag_sd_delta.utils import validate_insts_to_import

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["email_on_failure"] = True
dag_args["email"] = ["delta@randers.dk", "D-It-Supporten@randers.dk", "digitalisering@randers.dk"]
dag_args["retries"] = 2
dag_args["retry_delay"] = timedelta(minutes=5)

logger = logging.getLogger(__name__)


def extract_transform(**context: dict) -> dict[str, str | bool]:
    insts_to_import_raw = Variable.get("delta_sd_insts_to_import", default_var="{}")
    insts_to_import = json.loads(insts_to_import_raw)
    validate_insts_to_import(insts_to_import)
    inst_id_list = [inst["inst_id"] for inst in insts_to_import]
    unique_inst_ids = list(dict.fromkeys(inst_id_list))

    signflow_client = SignflowClient(BaseHook.get_connection("logiva_signflow"))
    delta_client = DeltaClient(BaseHook.get_connection("delta_prod"))

    # set up internal log collector to capture logs for email report
    log_collector = get_log_collector()
    root_logger = logging.getLogger()
    root_logger.addHandler(log_collector)

    logging.getLogger("airflow.hooks.base").setLevel(logging.WARNING)

    df = signflow_client.get_authorizations()

    emp_dfs: list[pd.DataFrame] = []
    per_dfs: list[pd.DataFrame] = []

    for _, row in df.iterrows():
        cpr = row['CPR']
        los = row['LOS']
        date = row['Fra dato']

        navn_text = (lambda value: f"{value[:40]}..." if len(value) > 40 else f"{value:<40}")(str(row.get('Navn') or '').strip())
        los_text = (lambda value: f"{value[:12]}..." if len(value) > 12 else f"{value:<12}")(str(row.get('LOS') or '').strip())
        log_template = f"Sagsnummer: {row['Sagsnummer']} - Fra dato: {date} LOS: {los_text}- Navn: {navn_text} - Kommentar: "
        res = delta_client.get_engagement_by_los_and_cpr(los=los, cpr=cpr, valid_date=date)
        if not res:
            logger.warning(f"{log_template}Ikke fundet i Delta")
        elif len(res) > 1:
            logger.warning(f"{log_template}Flere ansættelser fundet i Delta")
        else:
            if res[0]['user']:
                logger.info(f"{log_template}Har allerede en bruger {res[0]['user']}")
            else:
                if res[0]['institution_id'] in inst_id_list:
                    try:
                        emp = get_employment_on_date_df(
                            inst_id=res[0]['institution_id'],
                            cpr=cpr,
                            employment_id=res[0]['employment_id'],
                            effective_date=date
                        )
                        emp["InstitutionIdentifier"] = res[0]['institution_id']
                        per = get_person_on_date_df(
                            inst_id=res[0]['institution_id'],
                            cpr=cpr,
                            employment_id=res[0]['employment_id'],
                            effective_date=date
                        )
                        per["InstitutionIdentifier"] = res[0]['institution_id']
                        emp_dfs.append(emp)
                        per_dfs.append(per)
                        logger.info(f"{log_template}BRUGER OPRETTES")
                    except requests.exceptions.HTTPError as e:
                        if "EmploymentIdentifier does not exist" in str(e):
                            logger.warning(f"{log_template}Ikke fundet i SD")
                else:
                    logger.warning(f"{log_template}Ugyldig institution {res[0]['institution_id']}")

    emp_df = pd.concat(emp_dfs, ignore_index=True) if emp_dfs else pd.DataFrame()
    per_df = pd.concat(per_dfs, ignore_index=True) if per_dfs else pd.DataFrame()

    inst_name_mapping_df = get_institutions_df()
    prof_name_mapping_xml = get_professions_xml(inst_id="RG")

    out_df = pd.DataFrame()
    if not emp_df.empty:
        emp_column_map = {
            "EmploymentStatus_EmploymentStatusCode": "EmploymentStatusCode",
            "Department_DepartmentIdentifier": "DepartmentIdentifier",
            "Profession_JobPositionIdentifier": "JobPositionIdentifier",
            "Profession_EmploymentName": "EmploymentName",
            "EmploymentStatus_ActivationDate": "ActivationDate",
            "EmploymentStatus_DeactivationDate": "DeactivationDate",
        }
        normalized_emp_df = emp_df.rename(columns=emp_column_map)

        required_emp_columns = [
            "InstitutionIdentifier",
            "PersonCivilRegistrationIdentifier",
            "EmploymentIdentifier",
            "EmploymentStatusCode",
            "DepartmentIdentifier",
            "JobPositionIdentifier",
            "EmploymentName",
            "ActivationDate",
            "DeactivationDate",
        ]
        for column_name in required_emp_columns:
            if column_name not in normalized_emp_df.columns:
                normalized_emp_df[column_name] = None
        normalized_emp_df = normalized_emp_df[required_emp_columns]

        name_columns = [
            "InstitutionIdentifier",
            "PersonCivilRegistrationIdentifier",
            "PersonGivenName",
            "PersonSurnameName",
        ]
        per_name_df = per_df[
            [column_name for column_name in name_columns if column_name in per_df.columns]
        ].drop_duplicates(
            subset=[
                column_name
                for column_name in ["InstitutionIdentifier", "PersonCivilRegistrationIdentifier"]
                if column_name in per_df.columns
            ],
            keep="first",
        )

        normalized_emp_df = normalized_emp_df.merge(
            per_name_df,
            on=[
                column_name
                for column_name in ["InstitutionIdentifier", "PersonCivilRegistrationIdentifier"]
                if column_name in normalized_emp_df.columns and column_name in per_name_df.columns
            ],
            how="left",
        )

        out_dfs: list[pd.DataFrame] = []
        for inst_id in unique_inst_ids:
            inst_emp_df = normalized_emp_df[
                normalized_emp_df["InstitutionIdentifier"] == inst_id
            ].copy()
            if inst_emp_df.empty:
                continue

            out_dfs.append(
                build_output_df(
                    employment_changes_df=inst_emp_df,
                    inst_id=inst_id,
                    inst_name_mapping_df=inst_name_mapping_df,
                    start_time=pendulum.now("Europe/Copenhagen"),
                    prof_name_mapping_xml=prof_name_mapping_xml
                )
            )

        out_file = None
        if out_dfs:
            out_df = pd.concat(out_dfs, ignore_index=True)
            out_df['Handling'] = 'x'

            # Write result as excel file
            output_dir = Path(tempfile.gettempdir()) / "sd_delta_sync"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "sd-delta-sync.xlsx"
            with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                out_df.to_excel(
                    writer,
                    sheet_name="Ark1",
                    index=False,
                    header=True,
                )
            # out_df.to_excel(output_file, index=False)
            out_file = str(output_file)
            logger.info(f"Saved user creation data with {len(out_df)} rows to file {output_file}")

    result = {"file_path": out_file}

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
    dag_id="sd_delta_user_creation",
    start_date=datetime(year=2026, month=6, day=23, tz=timezone("Europe/Copenhagen")),
    schedule="30 7 * * *",
    render_template_as_native_obj=True,
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Check Signflow authorizations and import new users to Delta.",
    tags=['sd', 'silkeborgdata', 'delta', 'creation', 'user', 'sync']
) as dag:
    get_and_handle_authorizations = PythonOperator(
        task_id="get_and_handle_authorizations",
        python_callable=extract_transform
    )

    upload_file = PythonOperator(
        task_id="upload_excel_to_delta",
        python_callable=upload_excel_file_to_delta,
        op_kwargs={
            "delta_client": DeltaClient(BaseHook.get_connection("delta_prod")),
            "file_path": "{{ ti.xcom_pull(task_ids='get_and_handle_authorizations')['file_path'] }}"
        }
    )

    send_email = EmailOperator(
        task_id="send_email",
        to=["delta@randers.dk", "D-It-Supporten@randers.dk"],
        subject=("SD Delta user creation - {{ macros.datetime.utcnow().replace(tzinfo=macros.dateutil.tz.tzutc()).astimezone(macros.dateutil.tz.gettz('Europe/Copenhagen')).strftime('%Y-%m-%d %H:%M:%S') }}"),
        html_content=(
            "{{ ti.xcom_pull(task_ids='upload_excel_to_delta')['upload_html'] }}"
            "{{ ti.xcom_pull(task_ids='get_and_handle_authorizations')['log_html'] }}"
        ),
        files="{{ [ti.xcom_pull(task_ids='get_and_handle_authorizations')['file_path']] if ti.xcom_pull(task_ids='get_and_handle_authorizations')['file_path'] is not none else [] }}",
    )

    get_and_handle_authorizations >> upload_file >> send_email
