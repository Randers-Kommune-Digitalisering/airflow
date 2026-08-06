import logging
import json
from datetime import timedelta
from datetime import datetime as dt

import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS

logger = logging.getLogger(__name__)


def process_udddannels_stattistik_kmd_insight() -> None:
    """Fetch data from KMD Insight API and upload the file to SFTP."""
    context = get_current_context()
    start_year_raw = context["params"].get("start_year")

    try:
        start_year = int(start_year_raw)
    except (TypeError, ValueError) as exc:
        raise AirflowFailException("Invalid param 'start_year'. Expected an integer year.") from exc

    file_path = str(Variable.get("udddannels_stattistik_kmd_insight_file_path", default_var="")).strip()
    http_hook = HttpHook(method="POST", http_conn_id="uddannelsesstatistik_api")
    sftp_hook = SFTPHook(ssh_conn_id="kmd_insight")
    http_conn = http_hook.get_connection(http_hook.http_conn_id)
    api_key = str(http_conn.password or "").strip()

    if not file_path:
        raise AirflowFailException("Missing Airflow variable 'udddannels_stattistik_kmd_insight_file_path'")

    if not api_key:
        raise AirflowFailException(
            "Missing API key in password for Airflow connection 'uddannelsesstatistik_api'"
        )

    current_year = dt.now().year

    years: list[str] = []
    while current_year > start_year:
        years.append(f"{current_year - 1}/{current_year}")
        current_year -= 1

    if not years:
        raise AirflowFailException("No school years generated. Ensure 'start_year' is less than the current year.")

    request_payload = {
        "område": "GS",
        "emne": "TRIV",
        "underemne": "TRIVIND",
        "nøgletal": ["Indikatorsvar"],
        "detaljering": [
            "[Institution].[Institution]",
            "[Klassetrin].[Klassetringruppe]",
            "[Skoleår].[Skoleår]",
            "[Trivselsindikator].[Trivselsindikator]",
        ],
        "filtre": {
            "[Institution].[Institution Beliggenhedskommune]": ["Randers"],
            "[Institution].[Institutionstype]": ["Folkeskoler"],
            "[Klassetrin].[Klassetringruppe]": ["Udskoling", "Mellemtrin"],
            "[Skoleår].[Skoleår]": years,
        },
        "indlejret": True,
        "tomme_rækker": False,
        "formattering": "json",
    }

    response = http_hook.run(
        endpoint="Api/v1/statistik",
        data=json.dumps(request_payload),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        extra_options={"check_response": False},
    )

    if not 200 <= response.status_code < 300:
        raise AirflowFailException(
            f"HTTP request failed for connection 'uddannelsesstatistik_api' with status code {response.status_code}"
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise AirflowFailException("API response was not valid JSON") from exc

    if not data:
        raise AirflowFailException("API response was empty; nothing to transform/upload")

    expanded_data = []
    for school_name, school_rows in data.items():
        for grade_group, grade_rows in school_rows.items():
            for school_year, year_rows in grade_rows.items():
                row = {"År": school_year, "Skolenavn": school_name, "Trin": grade_group}
                for indicator_name, indicator_rows in year_rows.items():
                    for _, indicator_value in indicator_rows.items():
                        row[indicator_name] = indicator_value
                expanded_data.append(row)

    if not expanded_data:
        raise AirflowFailException("No rows were produced from API response")

    csv_payload = pd.DataFrame(expanded_data).to_csv(index=False, sep=";").encode("utf-8")

    with sftp_hook.get_conn() as sftp_client:
        with sftp_client.open(file_path, "wb") as target:
            target.write(csv_payload)

    logger.info("Uploaded KMD Insight uddannelsesstatistik file to SFTP")


dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 2
dag_args["retry_delay"] = timedelta(minutes=30)


with DAG(
    dag_id="udddannels_stattistik_kmd_insight",
    start_date=datetime(2026, 8, 12, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    params={
        "start_year": Param(
            2020,
            type="integer",
            description="Start year for the uddannelsesstatistik data.",
        )
    },
    default_args=dag_args,
    description="Fetch data from uddannelsesstatistik API and upload file to KMD Insight SFTP",
    tags=["kmd", "insight", "uddannelsesstatistik", "http", "sftp", "data"],
) as dag:
    run_udddannels_stattistik_kmd_insight = PythonOperator(
        task_id="run_udddannels_stattistik_kmd_insight",
        python_callable=process_udddannels_stattistik_kmd_insight,
        do_xcom_push=False,
    )

    run_udddannels_stattistik_kmd_insight
