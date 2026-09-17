import logging
import pandas as pd
from datetime import datetime

from prophet import Prophet

# from airflow.exceptions import AirflowFailException

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

HOLIDAY_DATES = pd.to_datetime([
    # Nytårsdag
    '2023-01-01', '2024-01-01', '2025-01-01', '2026-01-01',
    # Skærtorsdag
    '2023-04-13', '2024-03-29', '2025-04-18', '2026-04-03',
    # Langfredag
    '2023-04-14', '2024-03-30', '2025-04-19', '2026-04-04',
    # 2. Påskedag
    '2023-04-17', '2024-04-01', '2025-04-21', '2026-04-06',
    # Store Bededag
    '2023-05-05',
    # Kr. Himmelfartsdag
    '2023-05-25', '2024-05-16', '2025-06-05', '2026-05-21',
    # 2. Pinsedag
    '2023-06-04', '2024-05-26', '2025-06-15', '2026-05-31',
    # 1. Juledag
    '2023-12-25', '2024-12-25', '2025-12-25', '2026-12-25',
    # 2. Juledag
    '2023-12-26', '2024-12-26', '2025-12-26', '2026-12-26',
    # Nytårsaften
    '2023-12-31', '2024-12-31', '2025-12-31', '2026-12-31',
])


def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Filter and transforming raw operations data from the Frontdesk database.
    """
    # Dropping unnecessary columns
    data = data.drop(columns=DROPPED_COLUMNS)
    
    for column in ['CreatedAt', 'CalledAt', 'EndedAt',
                   'LastAggregatedDataUpdateTime']:
        data[column] = pd.to_datetime(data[column]).dt.tz_localize(None)

    # Removes data that isn't borgerservice
    data = data[~data["CounterName"].isin(EXCLUDED_COUNTERS)]

    # data older than two years or before 1/1/2023 will be removed
    two_years_ago = datetime.now().replace(year=datetime.now().year - 2)
    cutoff = max(two_years_ago, datetime(2023, 1, 1))
    data = data[data['CreatedAt'] >= cutoff]

    data["dato"] = data["CreatedAt"].dt.date
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
    holidays = pd.DataFrame(
        ({'holiday': 'lukkedage', 'ds': HOLIDAY_DATES, 'lower_window': 0,
          'upper_window': 1})
    )
    """
    Create a one-year weekday visitor forecast for one model
    """

    data_model = data.rename(columns={'dato': 'ds', 'antal': 'y'})
    data_model['cap'] = 500
    data_model['floor'] = 1

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        seasonality_mode='multiplicative',
        holidays=holidays,
        growth='flat'
    )
    model.fit(data_model)

    future = model.make_future_dataframe(periods=365)
    future['cap'] = 500
    future['floor'] = 1
    future = future[future['ds'].dt.weekday < 5]  # only weekdays

    prediction = model.predict(future)

    result = pd.concat([prediction[['ds', 'yhat']], data['antal']], axis=1)
    result = result.rename(columns={'ds': 'dato'})
    result['model'] = model_name
    result['antal'] = result['antal'].round(2)
    result['yhat'] = result['yhat'].round(2)

    return result[['dato', 'model', 'antal', 'yhat']]


def build_forecast(workdata: pd.DataFrame) -> pd.DataFrame:
    predictions = [forecast(daily_visitors(workdata), 'samlet')]
    """
    Build forecasts for all visitors and configured queue groups
    """

    for queue in QUEUES:
        subset = workdata[workdata['QueuesGrouped'] == queue]
        try:
            predictions.append(forecast(daily_visitors(subset), queue))
        except Exception as e:
            logger.error(f"Failed to forecast queue '{queue}': {e}")

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

    # Store processed operations in the postgres database
    upload_operations(workdata, target_engine)

    # Generate forecasts and upload them to the postgres database
    predictions = build_forecast(workdata)
    upload_forecasts(predictions, target_engine)

    logger.info("Finished processing frontdesk data.")
