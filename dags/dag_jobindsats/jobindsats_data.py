import logging
import pandas as pd

from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_data(
    http_hook: HttpHook,
    db_engine: Engine,
    name: str,
    years_back: int,
    dataset: str,
    period_format: str,
    params: dict[str, list[str] | str],
    id: str = None,
) -> bool:
    """
    Fetch data from Jobindsats API v3,
    transform to DataFrame,
    and store in database.
    """

    try:
        logger.info(f"Starting Jobindsats job: {name}")

        valid_periods = _period_request(
            http_hook=http_hook,
            dataset=dataset,
            period_format=period_format,
        )

        if not valid_periods:
            logger.error("Failed to get latest period")
            return False

        periods = _dynamic_period(
            valid_periods=valid_periods,
            years_back=years_back
        )

        if not periods:
            logger.error("Failed generating periods")
            return False

        headers = _get_jobindsats_api_headers(http_hook=http_hook)

        payload = {
            f"period.{period_format}": ",".join(periods),
            "format": "json",
        }

        payload.update(params)

        query_string = "&".join(
            f"{key}={value if not isinstance(value, list) else ','.join(value)}"
            for key, value in payload.items()
        )

        endpoint = f"v3/data/{dataset}?{query_string}"

        logger.info(f"Jobindsats request: dataset={dataset} endpoint={endpoint}")

        res = http_hook.run(
            endpoint=endpoint,
            headers=headers,
        )

        response_data = res.json()

        columns = response_data.get("columns")
        rows = response_data.get("rows")

        if not columns or not rows:
            logger.error("No columns or rows in response")
            return False

        df = pd.DataFrame(rows, columns=columns)

        if "Periode" in df.columns:
            df[f"Periode {name}"] = df["Periode"].apply(
                _convert_to_datetime
            )

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

        output_table = f"jobindsats_{dataset.replace('', '').lower()}{f'{id.lower()}' if id else ''}"

        df.to_sql(name=output_table, con=db_engine, if_exists='replace', index=False)
        logger.info(f"Successfully saved {output_table} to the database")
        return True

    except Exception as e:
        logger.exception(f"Error in get_data: {e}")
        return False


def _dynamic_period(
    valid_periods: list[str],
    years_back: int,
) -> list[str]:
    """
    Filter valid periods based on years_back.

    :param valid_periods: List of valid API periods
    :param years_back: Number of years back
    :return: Filtered periods
    """
    try:

        if not valid_periods:
            return []

        current_year = datetime.now().year
        min_year = current_year - years_back

        filtered_periods = []

        for period in valid_periods:

            year = int(period[:4])

            if year >= min_year:
                filtered_periods.append(period)

        return sorted(filtered_periods)

    except Exception as e:
        logger.error(f"Error in dynamic_period: {e}")
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


def _period_request(
    http_hook: HttpHook,
    dataset: str,
    period_format: str,
) -> list[str] | None:
    """
    Fetch valid periods from Jobindsats API v3.

    :param http_hook: HttpHook for Jobindsats API
    :param dataset: Dataset/table id
    :param period_format: M, Q, QMAT, Y, MYTD
    :return: List of valid period ids
    """

    try:
        headers = _get_jobindsats_api_headers(http_hook=http_hook)

        res = http_hook.run(
            endpoint=f"v3/table/{dataset}?format=json",
            headers=headers,
        )

        data = res.json()

        periods = data.get("periods", [])

        for periodtype in periods:

            if periodtype.get("periodtype_id") != period_format:
                continue

            values = periodtype.get("values", [])

            valid_periods = [
                p.get("period_id")
                for p in values
                if p.get("period_id")
            ]

            logger.info(f"Found {len(valid_periods)} valid periods for dataset={dataset} type={period_format}")

            return valid_periods

        logger.error(f"No matching period type found for dataset={dataset} with period_format={period_format}")

        return None

    except Exception as e:
        logger.exception(f"Error fetching periods: {e}")
        return None


def fetch_and_store_table_updates(http_hook: HttpHook, db_engine: Engine) -> bool:
    """
    Fetch table metadata from the Jobindsats API v3 and store it in the database.

    :param http_hook: HttpHook for the Jobindsats API.
    :param db_engine: SQLAlchemy Engine for the database.
    :return: True if metadata was fetched and stored successfully, otherwise False.
    """

    try:
        logger.info("Fetching tables metadata from Jobindsats API v3")

        headers = _get_jobindsats_api_headers(http_hook=http_hook)

        response = http_hook.run(
            endpoint="v3/tables?format=json",
            headers=headers,
        )

        data = response.json()

        if not data:
            logger.error("No tables data received")
            return False

        rows = []

        for subject in data:
            subject_name = subject.get("subject_name")

            for grp in subject.get("table_groups", []):

                for t in grp.get("tables", []):

                    rows.append({
                        "TableID": t.get("table_id"),
                        "TableName": t.get("table_name"),
                        "SubjectName": subject_name,
                        "LatestUpdate": t.get("update_latest"),
                        "NextUpdate": t.get("update_next"),
                        "UpdateFrequency": t.get("update_freq"),
                    })

        df_updates = pd.DataFrame(rows)
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
    if not conn.password:
        raise ValueError("Missing API Key (connection password) for Jobindsats API connection.")

    return {
        "Authorization": f"Bearer {conn.password}",
        "Content-Type": "application/json"
    }
