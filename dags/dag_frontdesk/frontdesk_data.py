import logging
from datetime import datetime

import holidays
import pandas as pd
from prophet import Prophet

from airflow.exceptions import AirflowFailException
from airflow.models import Variable

"""
Frontdesk data access module.

This module contains data access,
transformation, and forecasting helpers for the Frontdesk pipeline.

All writes replace target tables to keep reporting data aligned per DAG run.
"""

logger = logging.getLogger(__name__)


def validate_runtime_config(frontdesk_runtime_config: object) -> None:
    """
    Validate the Airflow Variable structure before processing data.

    :param frontdesk_runtime_config: Variable loaded from Airflow.
    :return: None.
    """
    if not isinstance(frontdesk_runtime_config, dict):
        raise AirflowFailException(
            "frontdesk_runtime_config must contain a JSON object"
        )

    required_keys = {
        "queues",
        "queue_groups",
        "dropped_columns",
        "excluded_counters",
        "datetime_columns",
        "operation_column_renames",
    }
    missing_keys = sorted(required_keys - frontdesk_runtime_config.keys())
    if missing_keys:
        raise AirflowFailException(
            f"frontdesk_runtime_config is missing keys: {missing_keys}"
        )

    list_keys = (
        "queues", "dropped_columns",
        "excluded_counters", "datetime_columns")
    invalid_lists = [
        key for key in list_keys
        if not isinstance(frontdesk_runtime_config[key], list)
        or not all(
            isinstance(value, str)
            for value in frontdesk_runtime_config[key]
        )
    ]
    if invalid_lists:
        raise AirflowFailException(
            f"frontdesk_runtime_config values must be lists of strings: {
                invalid_lists}"
        )

    queue_groups = frontdesk_runtime_config["queue_groups"]
    if not isinstance(queue_groups, dict) or not all(
        isinstance(source, str) and isinstance(group, str)
        for source, group in queue_groups.items()
    ):
        raise AirflowFailException(
            "frontdesk_runtime_config['queue_groups']"
            " must be a string-to-string object"
        )

    operation_column_renames = frontdesk_runtime_config[
        "operation_column_renames"]
    if not isinstance(operation_column_renames, dict) or not all(
        isinstance(source, str) and isinstance(target, str)
        for source, target in operation_column_renames.items()
    ):
        raise AirflowFailException(
            "frontdesk_runtime_config['operation_column_renames']"
            " must be a string-to-string object"
        )


def load_frontdesk_runtime_config() -> dict:
    """
    Load and validate Frontdesk settings from the Airflow Variable.

    :return dict: Validated Frontdesk runtime configuration.
    """
    frontdesk_runtime_config = Variable.get(
        "frontdesk_runtime_config",
        deserialize_json=True,
    )
    validate_runtime_config(frontdesk_runtime_config=frontdesk_runtime_config)
    return frontdesk_runtime_config


def _validate_required_columns(data: pd.DataFrame, required: set[str]) -> None:
    """
    Ensure required columns exist before transforming data.

    :param data: Source dataframe to validate.
    :param required: Column names that must be present in the dataframe.
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
    Drop configured columns that exist in the input dataframe.

    :param data: Dataframe from which columns are dropped.
    :param columns: Column names to remove when present.
    :return pd.DataFrame: Dataframe without the specified existing columns.
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

    :param data: Dataframe containing the datetime columns.
    :param columns: Names of columns to parse as datetimes.
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
    Calculate the lower bound date for operation rows to retain.

    :return datetime: Cutoff date
    """
    today = datetime.now()
    try:
        two_years_ago = today.replace(year=today.year - 2)
    except ValueError:
        # Handles leap day by falling back to the 28th.
        two_years_ago = today.replace(year=today.year - 2, day=28)
    return two_years_ago


def _holiday_dates() -> pd.DatetimeIndex:
    """
    Get closing days

    :return pd.DatetimeIndex: Dates excluded or adjusted by the forecast model.
    """
    today = datetime.now()
    years = range(today.year - 2, today.year + 2)
    danish_holidays = holidays.DK(years=years)

    dates = set(danish_holidays.keys())
    # Frontdesk treats New Year's Eve as a closing day as well.
    dates.update(datetime(year, 12, 31).date() for year in years)
    return pd.to_datetime(sorted(dates))


def transform_data(
    data: pd.DataFrame,
    frontdesk_runtime_config: dict,
) -> pd.DataFrame:
    """
    Filter and transform raw operation data from the Frontdesk database.

    :param data: Raw operation records fetched from Frontdesk.
    :param frontdesk_runtime_config: Validated configuration for filtering.
    :return pd.DataFrame: Filtered operations with derived and renamed columns.
    """
    required_columns = {
        "CreatedAt", "CalledAt", "EndedAt", "LastAggregatedDataUpdateTime",
        "CounterName", "QueueName", "AggregatedProcessingTime",
    }
    _validate_required_columns(data=data, required=required_columns)

    data = data.copy()
    dropped_columns = frontdesk_runtime_config["dropped_columns"]
    datetime_columns = frontdesk_runtime_config["datetime_columns"]
    excluded_counters = frontdesk_runtime_config["excluded_counters"]
    queue_groups = frontdesk_runtime_config["queue_groups"]
    data = _drop_columns_if_present(
        data=data,
        columns=dropped_columns,
    )
    data = _normalize_datetime_columns(
        data=data,
        columns=datetime_columns,
    )
    data = data[~data["CounterName"].isin(excluded_counters)]
    data = data[data['CreatedAt'] >= _cutoff_date()]

    data["dato"] = data["CreatedAt"].dt.normalize()
    data["ugenr"] = data["CreatedAt"].dt.isocalendar().week
    data["år"] = data["CreatedAt"].dt.year
    data["QueuesGrouped"] = (
        data["QueueName"]
        .map(queue_groups)
        .fillna(data['QueueName'])
    )

    data["BehandlingstidMinutter"] = data["EndedAt"] - data["CalledAt"]
    # AggregatedProcessingTime is stored in 100-nanosecond ticks.
    data["BehandlingstidMinutterDecimal"] = (
        data["AggregatedProcessingTime"] / (10**7 * 60)
    ).round(2)
    data["VentetidMinutter"] = data["CalledAt"] - data["CreatedAt"]
    data["VentetidMinutterDecimal"] = (
        data["VentetidMinutter"].dt.total_seconds() / 60
    ).round(2)

    data = data.rename(
        columns=frontdesk_runtime_config["operation_column_renames"]
    )

    return data.reset_index(drop=True)


def daily_visitors(data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate operation records into daily visitor counts.

    :param data: Transformed operation records containing the dato column.
    :return pd.DataFrame: Daily counts with dato and antal columns.
    """
    if data.empty:
        return pd.DataFrame(columns=["dato", "antal"])
    daily = data.groupby("dato").size()
    full_dates = pd.date_range(
        data["dato"].min(), data["dato"].max(), freq="D")
    return (
        daily.reindex(full_dates, fill_value=0)
        .rename_axis("dato")
        .reset_index(name="antal")
    )


def fetch_operations(source_engine) -> pd.DataFrame:
    """
    Reads all operation records from the frontdesk database.

    :param source_engine: Engine for the MSSQL database.
    :return pd.DataFrame: Containing all rows from the Operation table.
    """
    return pd.read_sql_query(
        "SELECT * FROM Operation "
        "WHERE CreatedAt >= DATEADD(year, -2, GETDATE())",
        con=source_engine,
    )


def forecast(data: pd.DataFrame, model_name: str) -> pd.DataFrame:
    """
    Train a Prophet model and return daily history plus forecast values.

    :param data: Daily visitor counts containing dato and antal columns.
    :param model_name: Name identifying the forecast model or queue group.
    :return pd.DataFrame: Historical counts and predictions for the model.
    """
    if data.empty:
        logger.warning(
            "No data available for model '%s'; skipping forecast", model_name
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
    future = future[future['ds'].dt.weekday < 5]
    prediction = model.predict(future)
    prediction["dato"] = prediction["ds"].dt.normalize()

    historical_by_date = historical[["ds", "y"]].rename(columns={"y": "antal"})
    historical_by_date["dato"] = historical_by_date["ds"].dt.normalize()
    result = prediction[["dato", "yhat"]].merge(
        historical_by_date[["dato", "antal"]],
        on="dato",
        how="left"
    )

    result['model'] = model_name
    result['antal'] = result['antal'].fillna(0).round(2)
    result['yhat'] = result['yhat'].round(2)
    return result[['dato', 'model', 'antal', 'yhat']]


def build_forecast(
    workdata: pd.DataFrame,
    frontdesk_runtime_config: dict,
) -> pd.DataFrame:
    """
    Build forecasts for all visitors and configured queue groups.

    :param workdata: Transformed operation records.
    :param frontdesk_runtime_config: Configuration containing forecast queues.
    :return pd.DataFrame: Combined forecasts for the overall and queue models.
    """
    if workdata["dato"].nunique() < 2:
        raise AirflowFailException("Not enough dates to build forecasts")
    daily_data = daily_visitors(data=workdata)
    predictions = [forecast(data=daily_data, model_name="samlet")]

    queues = frontdesk_runtime_config["queues"]
    for queue in queues:
        subset = workdata[workdata['queues_grouped'] == queue]
        if subset.empty:
            logger.warning(
                "No rows for queue '%s'; skipping forecast", queue)
            continue
        if subset["dato"].nunique() < 2:
            logger.warning(
                "Not enough dates for queue '%s'; skipping forecast", queue)
            continue
        try:
            queue_daily_data = daily_visitors(data=subset)
            predictions.append(
                forecast(data=queue_daily_data, model_name=queue)
            )
        except Exception:
            logger.exception("Failed to forecast queue '%s'", queue)
            raise

    return pd.concat(predictions, axis=0).reset_index(drop=True)


def validate_forecast_output(predictions: pd.DataFrame) -> None:
    """
    Ensure forecast output includes all columns required by its destination.

    :param predictions: Forecast dataframe to validate.
    :return: None.
    """
    required_columns = ["dato", "model", "antal", "yhat"]
    missing = [column for column
               in required_columns if column not in predictions.columns]
    if missing:
        raise AirflowFailException(
            f"Forecast is missing required columns: {missing}"
        )


def upload_operations(workdata: pd.DataFrame, target_engine) -> None:
    """
    Replace the operations table in the reporting database.

    :param workdata: Processed operations dataframe to upload.
    :param target_engine: Engine for the target Postgres database.
    :return: None.
    """
    _upload_dataset(
        data=workdata,
        table_name="operations",
        target_engine=target_engine)


def upload_forecasts(predictions: pd.DataFrame, target_engine) -> None:
    """
    Replace the forecasts table in the reporting database.

    :param predictions: Forecast dataframe to upload.
    :param target_engine: Engine for the target Postgres database.
    :return: None.
    """
    _upload_dataset(
        data=predictions,
        table_name="forecasts",
        target_engine=target_engine)


def _upload_dataset(
    data: pd.DataFrame,
    table_name: str,
    target_engine,
) -> None:
    """
    Upload a dataframe to PostgreSQL by replacing the destination table.

    :param data: Dataframe to upload.
    :param table_name: Destination table name.
    :param target_engine: Engine for the target Postgres database.
    :return: None.
    """
    if data.empty:
        logger.warning("No rows to upload for %s", table_name)
        return

    # Full-table replace keeps reporting tables aligned with each DAG run.
    data.to_sql(
        table_name,
        con=target_engine,
        if_exists="replace",
        index=False,
    )

    logger.info("Uploaded %s rows to %s", len(data), table_name)
