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
    "Flytning og folkeregister ": "Flytning og folkeregister",
    "Flytning og Folkeregister": "Flytning og folkeregister",
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


def _holiday_dates() -> pd.DatetimeIndex:
    """
    Return Danish public holidays for the historical and forecast periods.
    """
    today = datetime.now()
    years = range(today.year - 2, today.year + 2)
    danish_holidays = holidays.DK(years=years)

    dates = set(danish_holidays.keys())
    dates.update(datetime(year, 12, 31).date() for year in years)
    return pd.to_datetime(sorted(dates))


def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Filter and transform raw operation data from the Frontdesk database.
    """
    required_columns = {
        "CreatedAt", "CalledAt", "EndedAt", "LastAggregatedDataUpdateTime",
        "CounterName", "QueueName", "AggregatedProcessingTime",
    }
    missing_columns = sorted(required_columns - set(data.columns))
    if missing_columns:
        raise ValueError(
            "Frontdesk data is missing required columns: "
            f"{missing_columns}"
        )

    data = data.copy()

    # Dropping unnecessary columns
    for col in DROPPED_COLUMNS:
        if col in data.columns:
            data = data.drop(columns=col)

    for column in ['CreatedAt', 'CalledAt', 'EndedAt',
                   'LastAggregatedDataUpdateTime']:
        series = pd.to_datetime(data[column], errors="coerce")
        if getattr(series.dt, "tz", None) is not None:
            series = series.dt.tz_localize(None)
        data[column] = series

    # Removes data that isn't borgerservice
    data = data[~data["CounterName"].isin(EXCLUDED_COUNTERS)]

    # data older than two years or before 1/1/2023 will be removed
    today = datetime.now()
    try:
        two_years_ago = today.replace(year=today.year - 2)
    except ValueError:
        two_years_ago = today.replace(year=today.year - 2, day=28)
    cutoff = max(two_years_ago, datetime(2023, 1, 1))
    data = data[data['CreatedAt'] >= cutoff]

    data["dato"] = data["CreatedAt"].dt.normalize()
    data["ugenr"] = data["CreatedAt"].dt.isocalendar().week
    data["år"] = data["CreatedAt"].dt.year
    data["QueuesGrouped"] = (
        data["QueueName"]
        .map(QUEUE_GROUPS)
        .fillna(data['QueueName'])
    )

    data["BehandlingstidMinutter"] = data["EndedAt"] - data["CalledAt"]
    data["BehandlingstidMinutterDecimal"] = (
        data["AggregatedProcessingTime"] / (10**7 * 60)
        ).round(2)
    data["VentetidMinutter"] = data["CalledAt"] - data["CreatedAt"]
    data["VentetidMinutterDecimal"] = (
        data["VentetidMinutter"].dt.total_seconds() / 60
        ).round(2)

    return data.reset_index(drop=True)


def daily_visitors(data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate operation records by date
    """
    return data.groupby('dato').size().reset_index(name='antal')


def forecast(data: pd.DataFrame, model_name: str) -> pd.DataFrame:
    if data.empty:
        logger.warning(
            "No data available for model '%s'; "
            "skipping forecast", model_name
            )
        return pd.DataFrame(columns=["dato", "model", "antal", "yhat"])

    holidays = pd.DataFrame({
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
        holidays=holidays,
        growth='flat'
    )
    model.fit(historical)

    future = model.make_future_dataframe(periods=365)
    future['cap'] = 500
    future['floor'] = 1
    future = future[future['ds'].dt.weekday < 5]  # only weekdays

    prediction = model.predict(future)

    prediction["dato"] = prediction["ds"].dt.normalize()
    result = prediction[['dato', 'yhat']].copy()

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


def build_forecast(workdata: pd.DataFrame) -> pd.DataFrame:
    """
    Build forecasts for all visitors and configured queue groups.
    """
    predictions = [forecast(daily_visitors(workdata), 'samlet')]

    for queue in QUEUES:
        subset = workdata[workdata['QueuesGrouped'] == queue]
        if subset.empty:
            logger.warning("No rows for queue '%s'; skipping forecast", queue)
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
