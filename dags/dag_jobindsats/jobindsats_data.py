import logging
import json
import pandas as pd

from datetime import datetime
from typing import List, Optional
from airflow.providers.http.hooks.http import HttpHook
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_data(http_hook: HttpHook, db_engine: Engine, name: str, years_back: int, dataset: str, period_format: str, data_to_get: dict[str, list[str]]) -> bool:
    """
    Fetch data from the Jobindsats API, transform it into a DataFrame, and store it in the database.

    :param http_hook: HttpHook for the Jobindsats API.
    :param db_engine: SQLAlchemy Engine for the database.
    :param name: Name of the jobindsats
    :param years_back: Number of years back to fetch data for.
    :param dataset: Dataset name in the Jobindsats API.
    :param period_format: Format of the periods ('QMAT', 'Q', 'M').
    :param data_to_get: Dictionary with additional data fields for the API call.
    :return: True if data was fetched and stored successfully, otherwise False.
    """
    try:
        logger.info(f"Starting jobindsats: {name}")

        latest_period = _period_request(http_hook=http_hook, dataset=dataset, period_format=period_format)
        if not latest_period:
            logger.error("Failed to get the latest period")
            return False

        period = _dynamic_period(latest_period=latest_period, years_back=years_back, period_format=period_format)
        if not period:
            logger.error("Failed to generate periods")
            return False

        payload = {"area": "*", "period": period} | data_to_get
        headers = _get_jobindsats_api_headers(http_hook=http_hook)

        res = http_hook.run(
            endpoint=f'v2/data/{dataset}/json',
            headers=headers,
            data=json.dumps(payload)
        )

        data = res.json()
        variables = data[0]['Variables']
        data = data[0]['Data']

        column_names = [var['Label'] for var in variables]
        df = pd.DataFrame(data, columns=column_names)
        df[f'Periode {name}'] = df['Periode'].apply(_convert_to_datetime)

        rename_map = {
            "Area": "Område",
            "Forventet og faktisk antal fuldtidspersoner på offentlig forsørgelse: Forventet antal": "Forventet antal",
            "Forventet og faktisk antal fuldtidspersoner på offentlig forsørgelse: Faktisk antal": "Faktisk antal",
            "Forventet og faktisk antal fuldtidspersoner på offentlig forsørgelse: Forskel mellem forventet og faktisk antal": "Forskel mellem forventet og faktisk antal",
            "Forventet og faktisk andel fuldtidspersoner på offentlig forsørgelse: Forventet andel (pct.)": "Forventet andel (pct.)",
            "Forventet og faktisk andel fuldtidspersoner på offentlig forsørgelse: Faktisk andel (pct.)": "Faktisk andel (pct.)",
            "Forventet og faktisk andel fuldtidspersoner på offentlig forsørgelse: Forskel mellem forventet og faktisk andel (pct. point)": "Forskel mellem forventet og faktisk andel (pct. point)"
        }
        df.rename(columns=rename_map, inplace=True)

        output_table = f"jobindsats_{dataset.replace('_', '').lower()}"

        df.to_sql(name=output_table, con=db_engine, if_exists='replace', index=False)
        logger.info(f"Successfully saved {output_table} to the database")
        return True

    except Exception as e:
        logger.error(f'Error {e}')
        return False


def _dynamic_period(latest_period: str, years_back: int, period_format: str) -> List[str]:
    """
    Generate a list of periods based on the latest period, years back, and period format.

    :param latest_period: Latest period string from the API.
    :param years_back: Number of years back.
    :param period_format: Format of the periods ('QMAT', 'Q', 'M').
    :return: List of period strings.
    """
    try:
        period: List[str] = []
        if period_format == 'QMAT' and 'QMAT' in latest_period:
            current_year = int(latest_period[:4])
            current_qmat = int(latest_period[8:])
            for qmat in range(1, current_qmat + 1):
                period.append(f"{current_year}QMAT{qmat:02d}")
            if years_back:
                for year in range(current_year - years_back, current_year):
                    for qmat in range(1, 13):
                        period.append(f"{year}QMAT{qmat:02d}")
        elif period_format == 'Q' and 'Q' in latest_period:
            current_year = int(latest_period[:4])
            current_quarter = int(latest_period[5:])
            for quarter in range(1, current_quarter + 1):
                period.append(f"{current_year}Q{quarter}")
            if years_back:
                for year in range(current_year - years_back, current_year):
                    for quarter in range(1, 5):
                        period.append(f"{year}Q{quarter}")
        elif period_format == 'M' and 'M' in latest_period:
            current_year = int(latest_period[:4])
            current_month = int(latest_period[5:])
            for month in range(1, current_month + 1):
                period.append(f"{current_year}M{month:02d}")
            if years_back:
                for year in range(current_year - years_back, current_year):
                    for month in range(1, 13):
                        period.append(f"{year}M{month:02d}")
        return period
    except Exception as e:
        logger.error(f'Error in dynamic_period: {e}')
        return []


def _convert_to_datetime(period_str: str) -> datetime:
    """
    Convert a period string to a datetime object.

    :param period_str: Period as a string (e.g. '2023QMAT01', '2023Q1', '2023M01').
    :return: datetime object representing the start of the period.
    """
    year = int(period_str[:4])
    if 'QMAT' in period_str:
        qmat = int(period_str[8:])
        month = qmat
    elif 'Q' in period_str:
        quarter = int(period_str[5:])
        month = (quarter - 1) * 3 + 1
    else:
        month = int(period_str[5:])
    return datetime(year, month, 1)


def _period_request(http_hook: HttpHook, dataset: str, period_format: str) -> Optional[str]:
    """
    Fetch the latest period for a dataset from the Jobindsats API.

    :param http_hook: HttpHook for the Jobindsats API.
    :param dataset: Name of the dataset.
    :param period_format: Format of the periods ('QMAT', 'Q', 'M').
    :return: Latest period as a string, or None if not found.
    """
    try:
        headers = _get_jobindsats_api_headers(http_hook=http_hook)

        res = http_hook.run(
            endpoint=f'v2/tables/{dataset}/json/',
            headers=headers,
            data="{}"
        )
        data = res.json()
        periods = data[0]['Period']

        if period_format == 'QMAT':
            valid_periods = [p for p in periods if len(p) == 10 and p[4:8] == 'QMAT' and p[8:].isdigit()]
        elif period_format == 'Q':
            valid_periods = [p for p in periods if len(p) == 6 and p[4] == 'Q' and p[5:].isdigit()]
        elif period_format == 'M':
            valid_periods = [p for p in periods if len(p) == 7 and p[4] == 'M' and p[5:].isdigit()]
        else:
            valid_periods = []

        if not valid_periods:
            logger.error("No valid periods found")
            return None

        latest_period = max(valid_periods)
        return latest_period
    except Exception as e:
        logger.error(f'Error fetching period for dataset {dataset}: {e}')
        return None


def fetch_and_store_table_updates(http_hook: HttpHook, db_engine: Engine) -> bool:
    """
    Fetch table metadata from the Jobindsats API and store it in the database.

    :param http_hook: HttpHook for the Jobindsats API.
    :param db_engine: SQLAlchemy Engine for the database.
    :return: True if metadata was fetched and stored successfully, otherwise False.
    """
    try:
        logger.info("Fetching tables metadata from jobindsats API")
        headers = _get_jobindsats_api_headers(http_hook=http_hook)

        response = http_hook.run(
            endpoint='v2/tables/json',
            headers=headers,
            data="{}"
        )
        tables_data = response.json()

        if not tables_data:
            logger.error("No tables data received")
            return False

        updates = []
        for table in tables_data:
            updates.append({
                "TableID": table.get("TableID"),
                "TableName": table.get("TableName"),
                "SubjectName": table.get("SubjectName"),
                "LatestUpdate": table.get("LatestUpdate"),
                "NextUpdate": table.get("NextUpdate"),
                "UpdateFrequency": table.get("UpdateFrequency"),
            })

        df_updates = pd.DataFrame(updates)
        output_table = "jobindsats_table_updates"

        df_updates.to_sql(name=output_table, con=db_engine, if_exists='replace', index=False)
        logger.info(f"Successfully saved {output_table} to the database")
        return True

    except Exception as e:
        logger.error(f"Error fetching and storing table updates: {e}")
        return False


def _get_jobindsats_api_headers(http_hook: HttpHook) -> dict:
    """
    Build HTTP headers for the Jobindsats API using the API key from the Airflow connection.

    :param http_hook: Airflow HttpHook for the Jobindsats API.
    :return: Dictionary with HTTP headers.
    """
    conn = http_hook.get_connection(http_hook.http_conn_id)
    api_key = conn.extra_dejson.get("api_key")
    return {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }
