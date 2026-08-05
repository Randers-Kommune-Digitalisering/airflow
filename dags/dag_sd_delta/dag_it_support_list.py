import logging
import tempfile

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pendulum

from pendulum import timezone
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

from dag_sd_delta.delta_client import DeltaClient
from dag_sd_delta.signflow import LogivaSignflowClient
from utils.config import DEFAULT_DAG_ARGS

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["email_on_failure"] = True
dag_args["retries"] = 2
dag_args["retry_delay"] = timedelta(minutes=5)
dag_args["email"].append("D-It-Supporten@randers.dk")

logger = logging.getLogger(__name__)


# Helpers for date parsing and formatting
def _parse_dk_date(value: object) -> date | None:
    """Parse a date from Danish formats (dd.mm.yyyy, dd.mm.yy, yyyy-mm-dd) or return None if parsing fails or the value is NaN."""
    if isinstance(value, date):
        return value
    if pd.isna(value):
        return None

    text = str(value).strip()
    for date_format in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, date_format).date()
        except ValueError:
            continue

    return None


def _to_dk_date_string(value: date | None) -> str | None:
    """Convert a date object to a Danish date string format (dd.mm.yyyy) or return None if the value is None."""
    return value.strftime("%d.%m.%Y") if value else None


def _autosize_excel_columns(worksheet, dataframe: pd.DataFrame, padding: int = 2) -> None:
    """Autosize Excel columns based on the maximum length of the column names and values, with optional padding."""
    for column_index, column_name in enumerate(dataframe.columns):
        column_values = dataframe[column_name].fillna("").astype(str)
        max_value_length = column_values.map(len).max() if not column_values.empty else 0
        column_width = max(len(str(column_name)), int(max_value_length)) + padding
        worksheet.set_column(column_index, column_index, column_width)


def extract_transform() -> dict[str, str]:
    """Build IT support authorization list from Signflow enriched with Delta values and save it as an Excel file. Returns a dictionary with the file path of the saved Excel file."""
    signflow_client = LogivaSignflowClient(BaseHook.get_connection("logiva_signflow"))
    delta_client = DeltaClient(BaseHook.get_connection("delta_prod"))

    logging.getLogger("airflow.hooks.base").setLevel(logging.WARNING)

    signflow_df = signflow_client.get_authorizations().copy()
    today = pendulum.now("Europe/Copenhagen").date()

    def include_row(row: pd.Series) -> bool:
        from_date = row["Fra dato"]
        if pd.isna(from_date):
            return False
        if row["Handling"] == "Nyansat":
            return from_date <= today + timedelta(days=14)
        return from_date <= today

    filtered_df = signflow_df[signflow_df.apply(include_row, axis=1)].copy()

    out_df = filtered_df.rename(
        columns={
            "Sagsnummer": "Sagsnummer",
            "Navn": "Navn",
            "CPR": "CPR",
            "LOS": "LOS",
            "Fra dato": "Fra dato",
            "Handling": "Handling",
            "lederemail": "Lederemail",
        }
    )

    out_df["Fra dato"] = out_df["Fra dato"].apply(_to_dk_date_string)
    out_df["Loginnavn(e)"] = None
    out_df["Findes ikke i Delta"] = None

    for idx, row in out_df.iterrows():
        from_date = _parse_dk_date(row["Fra dato"])
        if from_date is None:
            out_df.at[idx, "Findes ikke i Delta"] = "x"
            continue

        engagements = delta_client.get_engagement_by_los_and_cpr(
            los=str(row["LOS"]),
            cpr=str(row["CPR"]),
            valid_date=from_date,
        )

        if not engagements:
            out_df.at[idx, "Findes ikke i Delta"] = "x"
            continue

        usernames = sorted(
            {
                str(engagement["user"]).strip()
                for engagement in engagements
                if engagement.get("user")
            }
        )
        if usernames:
            out_df.at[idx, "Loginnavn(e)"] = ", ".join(usernames)

    out_df = out_df.assign(
        _sort_date=out_df["Fra dato"].apply(
            lambda value: datetime.strptime(str(value), "%d.%m.%Y")
        )
    ).sort_values(by="_sort_date", ascending=True).drop(columns=["_sort_date"])

    column_order = [
        "Sagsnummer",
        "Navn",
        "CPR",
        "LOS",
        "Loginnavn(e)",
        "Fra dato",
        "Handling",
        "Lederemail",
        "Findes ikke i Delta",
    ]
    out_df = out_df[[column_name for column_name in column_order if column_name in out_df.columns]]

    output_dir = Path(tempfile.gettempdir()) / "sd_delta_sync"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"it-support-autorisationer-{today.strftime('%Y-%m-%d')}.xlsx"
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        out_df.to_excel(
            writer,
            sheet_name="Ark1",
            index=False,
            header=True,
        )
        worksheet = writer.sheets["Ark1"]
        _autosize_excel_columns(worksheet=worksheet, dataframe=out_df)

    output_file_path = str(output_file)
    logger.info("Saved IT support list with %s rows to %s", len(out_df), output_file_path)

    timestamp = pendulum.now("Europe/Copenhagen").strftime("%Y-%m-%d %H:%M:%S")
    send_email(
        to=["D-It-Supporten@randers.dk"],
        subject=f"Signflow Autorisationer - {timestamp}",
        html_content="Liste af autorisationer er vedh\u00e6ftet.",
        files=[output_file_path],
    )

    return {"file_path": output_file_path}


with DAG(
    dag_id="it_support_list",
    start_date=pendulum.datetime(year=2026, month=8, day=3, tz=timezone("Europe/Copenhagen")),
    schedule="0 8 * * 1-5",
    render_template_as_native_obj=True,
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Check Signflow authorizations and send IT support list with Delta details.",
    tags=["it-support", "delta", "user", "signflow", "authorization"],
) as dag:
    get_and_handle_authorizations = PythonOperator(
        task_id="get_and_handle_authorizations",
        python_callable=extract_transform,
        do_xcom_push=False
    )
