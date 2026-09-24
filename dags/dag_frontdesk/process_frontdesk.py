import logging
import pandas as pd
from datetime import datetime
import holidays
from prophet import Prophet

from airflow.exceptions import AirflowFailException

from dag_frontdesk.frontdesk_data import (
    fetch_operations,
    upload_operations,
    upload_forecasts,
)
from rkdigi.database_manager import DatabaseManager

"""
Frontdesk processing module.

This module contains the business logic for the Frontdesk pipeline:
1. Validate and transform raw operation data.
2. Aggregate daily visitor counts.
3. Build Prophet forecasts for total volume and queue groups.
4. Upload processed operations and forecast output.

Use process_frontdesk() as the orchestration entry point from the DAG.
"""

logger = logging.getLogger(__name__)

QUEUES = [
     'Pas', 'MitID', 'Afhent pas/kørekort/sundhedskort', 'Kørekort', 'Pension',
     'Informationen', 'Buskort til pensionister', 'Andet',
     'Beboerindskud og boligstøtte', 'Skat', 'Flytning og Folkeregister',
     'Sundhedskort og lægevalg', 'Legitimationskort',
     'Tilflytning fra udlandet', 'Fritagelse for digitalpost',
]

QUEUE_GROUPS = {
    "Afhent pas/kørekort/sundhedskort ": "Afhent pas/kørekort/sundhedskort",
    "Beboerindskud ": "Beboerindskud og boligstøtte",
    "Beboerindskud og boligstøtte ": "Beboerindskud og boligstøtte",
    "Boligstøtte": "Beboerindskud og boligstøtte",
    "Den Boligsociale Enhed - Anders": "Den Boligsociale Enhed",
    "Den Boligsociale Enhed  - Janni": "Den Boligsociale Enhed",
    "Flytning og folkeregister ": "Flytning og Folkeregister",
    "Flytning og Folkeregister": "Flytning og Folkeregister",
    "Kontrolenheden - Bjørn": "Kontrolenheden",
    "Kontrolenheden - Erik": "Kontrolenheden",
    "Kontrolenheden - Marianne": "Kontrolenheden",
    "Kørekort ": "Kørekort",
    "Kørekort Selfie": "Kørekort",
    "Legitimationskort/ID-kort": "Legitimationskort",
    "Legitimationskort ": "Legitimationskort",
    "MitID ": "MitID",
    "MitID": "MitID",
    "Mit-ID Aktiveringskode": "MitID",
    "Pas ": "Pas",
    "Pas Selfie": "Pas",
    "Pension ": "Pension",
    "Pension": "Pension",
    "Pension akut tid": "Pension",
    "Resultat af årsopgørelse": "Skat",
    "SKAT": "Skat",
    "Skat": "Skat",
}

DROPPED_COLUMNS = [
        "MunicipalityID", "QueueId", "QueueCategoryId", "State", "StateId",
        "CounterId", "EmployeeId", "DelayedUntil", "DelayedFrom",
        "IsEmployeeAnonymized", "EmployeeInitials"
    ]

EXCLUDED_COUNTERS = [
    'Jobcenter', 'Ydelseskontoret', 'Integration'
]

DATETIME_COLUMNS = [
    'CreatedAt',
    'CalledAt',
    'EndedAt',
    'LastAggregatedDataUpdateTime',
]


def _validate_required_columns(data: pd.DataFrame, required: set[str]) -> None:
    """
    Ensure required columns exist before transforming data.

    :param data (pd.DataFrame): Input dataframe to validate.
    :param required (set[str]): Set of required column names.
    :return: None.
    """
    missing_columns = sorted(required - set(data.columns))
    if missing_columns:
        raise ValueError(
            "Frontdesk data is missing required columns: "
            f"{missing_columns}"
        )


def _drop_columns_if_present(
    data: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """
    Drop columns when they exist in the input dataframe.

    :param data (pd.DataFrame): Input dataframe.
    :param columns (list[str]): Columns that should be dropped when present.
    :return pd.DataFrame: Dataframe without the selected columns.
    """
    existing = [column for column in columns if column in data.columns]
    if not existing:
        return data
    return data.drop(columns=existing)


def _normalize_datetime_columns(
    data: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """
    Parse datetime columns and strip timezone information if present.

    :param data (pd.DataFrame): Input dataframe.
    :param columns (list[str]): Datetime-like columns to parse.
    :return pd.DataFrame: Dataframe with normalized datetime columns.
    """
    for column in columns:
        parsed = pd.to_datetime(data[column], errors="coerce")
        if getattr(parsed.dt, "tz", None) is not None:
            parsed = parsed.dt.tz_localize(None)
        data[column] = parsed
    return data


def _cutoff_date() -> datetime:
    """
    Return the lower bound date for kept operation rows.

    :return pd.datetime : Cutoff datetime used for record filtering.
    """
    today = datetime.now()
    try:
        two_years_ago = today.replace(year=today.year - 2)
    except ValueError:
        # Handles leap day by falling back to the 28th.
        two_years_ago = today.replace(year=today.year - 2, day=28)
    return max(two_years_ago, datetime(2023, 1, 1))


def _holiday_dates() -> pd.DatetimeIndex:
    """
    Return Danish public holidays for the historical and forecast periods.

    :return pd.DatetimeIndex: Datetime index of holiday/closure dates.
    """
    today = datetime.now()
    years = range(today.year - 2, today.year + 2)
    danish_holidays = holidays.DK(years=years)

    dates = set(danish_holidays.keys())
    # Dec 31 is treated as a closure day for forecasting purposes.
    dates.update(datetime(year, 12, 31).date() for year in years)
    return pd.to_datetime(sorted(dates))


def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Filter and transform raw operation data from the Frontdesk database.

    :param data (pd.DataFrame): Raw operations dataframe.
    :return pd.DataFrame: Cleaned dataframe with date/time fields.
    """
    required_columns = {
        "CreatedAt", "CalledAt", "EndedAt", "LastAggregatedDataUpdateTime",
        "CounterName", "QueueName", "AggregatedProcessingTime",
    }
    _validate_required_columns(data, required_columns)

    data = data.copy()

    # Keep only fields needed for downstream transformations and publishing.
    data = _drop_columns_if_present(data, DROPPED_COLUMNS)
    data = _normalize_datetime_columns(data, DATETIME_COLUMNS)

    # Remove counters that are outside Borgerservice scope.
    data = data[~data["CounterName"].isin(EXCLUDED_COUNTERS)]

    # Keep only recent data while enforcing an absolute lower bound.
    cutoff = _cutoff_date()
    data = data[data['CreatedAt'] >= cutoff]

    data["dato"] = data["CreatedAt"].dt.normalize()
    data["ugenr"] = data["CreatedAt"].dt.isocalendar().week
    data["år"] = data["CreatedAt"].dt.year
    data["QueuesGrouped"] = (
        data["QueueName"]
        .map(QUEUE_GROUPS)
        # Preserve original queue names when no grouping rule exists.
        .fillna(data['QueueName'])
    )

    data["BehandlingstidMinutter"] = data["EndedAt"] - data["CalledAt"]
    data["BehandlingstidMinutterDecimal"] = (
        # Frontdesk processing time is stored in 100ns ticks.
        data["AggregatedProcessingTime"] / (10**7 * 60)
        ).round(2)
    data["VentetidMinutter"] = data["CalledAt"] - data["CreatedAt"]
    data["VentetidMinutterDecimal"] = (
        data["VentetidMinutter"].dt.total_seconds() / 60
        ).round(2)

    return data.reset_index(drop=True)


def daily_visitors(data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate operation records by date.

    :param data (pd.DataFrame): Transformed operations dataframe.
    :return pd.DataFrame: Dataframe with columns dato and antal.
    """
    if data.empty:
        return pd.DataFrame(columns=["dato", "antal"])
    daily = data.groupby("dato").size()
    full_dates = pd.date_range(data["dato"].min(), data["dato"].max(), freq="D")
    return daily.reindex(full_dates, fill_value=0).rename_axis("dato").reset_index(name="antal")


def forecast(data: pd.DataFrame, model_name: str) -> pd.DataFrame:
    """
    Train a Prophet model and return daily history plus forecast.

    :param data (pd.DataFrame): Daily visitor counts, dato, antal.
    :param model_name (str): Label for the forecast model output.
    :return pd.DataFrame: Dataframe with dato, model, antal, and yhat.
    """
    if data.empty:
        logger.warning(
            "No data available for model '%s'; "
            "skipping forecast", model_name
            )
        return pd.DataFrame(columns=["dato", "model", "antal", "yhat"])

    holiday_frame = pd.DataFrame({
        'holiday': 'lukkedage',
        'ds': _holiday_dates(),
        'lower_window': 0,
        'upper_window': 1
    })

    historical = data.rename(columns={'dato': 'ds', 'antal': 'y'}).copy()
    historical["ds"] = pd.to_datetime(historical["ds"])

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        seasonality_mode='multiplicative',
        holidays=holiday_frame,
        growth='flat'
    )
    model.fit(historical)

    future = model.make_future_dataframe(periods=365)
    future['cap'] = 500
    future['floor'] = 1
    # Service is closed during weekends, so forecast only weekdays.
    future = future[future['ds'].dt.weekday < 5]

    prediction = model.predict(future)

    prediction["dato"] = prediction["ds"].dt.normalize()

    historical_by_date = historical[["ds", "y"]].rename(columns={"y": "antal"})
    historical_by_date["dato"] = historical_by_date["ds"].dt.normalize()

    # Merge actual historical counts into the predicted timeline.
    result = prediction[["dato", "yhat"]].merge(
        historical_by_date[["dato", "antal"]],
        on="dato",
        how="left"
    )

    result['model'] = model_name
    # Future rows have no historical count; they are set to 0 for consistency
    result['antal'] = result['antal'].fillna(0).round(2)
    result['yhat'] = result['yhat'].round(2)

    return result[['dato', 'model', 'antal', 'yhat']]


def build_forecast(workdata: pd.DataFrame) -> pd.DataFrame:
    """
    Build forecasts for all visitors and configured queue groups.

    :param workdata (pd.DataFrame): Transformed operations dataframe.
    :return pd.DataFrame: Forecast dataframe for total and queue groups.
    """
    predictions = [forecast(daily_visitors(workdata), 'samlet')]

    for queue in QUEUES:
        subset = workdata[workdata['QueuesGrouped'] == queue]
        if subset.empty:
            logger.warning("No rows for queue '%s'; skipping forecast", queue)
            continue
        if subset["dato"].nunique() < 2:
            logger.warning("Not enough dates for queue '%s'; skipping forecast", queue)
            continue
        try:
            predictions.append(forecast(daily_visitors(subset), queue))
        except Exception:
            logger.exception("Failed to forecast queue '%s'", queue)
            raise

    return pd.concat(predictions, axis=0).reset_index(drop=True)


def process_frontdesk() -> None:
    """
    Fetch, transform, forecast, and publish Frontdesk operations data

    :return: None.
    """
    logger.info("Starting to process frontdesk data...")

    source_db = DatabaseManager(
        profile_name="azure_frontdesk_db",
        db_type="mssql",
        airflow_connection_id="azure_frontdesk_db"
    )
    source_engine = source_db._engine

    target_db = DatabaseManager(
        profile_name="frontdesk_db",
        db_type="postgres",
        airflow_connection_id="frontdesk_db"
    )
    target_engine = target_db._engine

    # raw operations from the frontdesk database
    raw_data = fetch_operations(source_engine)

    # clean and transform the source data for forecasting
    workdata = transform_data(raw_data)

    if workdata.empty:
        raise AirflowFailException(
            "No Frontdesk operations remain after filtering"
        )

    # Store processed operations in the postgres database
    upload_operations(workdata, target_engine)

    # Generate forecasts and upload them to the postgres database
    predictions = build_forecast(workdata)
    if predictions.empty:
        raise AirflowFailException("No forecast rows generated")

    required_columns = ["dato", "model", "antal", "yhat"]
    missing = [
        col for col in required_columns if col not in predictions.columns]

    if missing:
        raise AirflowFailException(
            f"Forecast is missing required columns: {missing}"
        )

    upload_forecasts(predictions, target_engine)

    logger.info("Finished processing frontdesk data.")
