import logging
from airflow.exceptions import AirflowFailException

from dag_frontdesk.frontdesk_data import (
    fetch_operations,
    load_frontdesk_runtime_config,
    transform_data,
    build_forecast,
    validate_forecast_output,
    upload_operations,
    upload_forecasts,
)
from rkdigi.database_manager import DatabaseManager

"""
Frontdesk orchestration module.

This module coordinates database access and data processing
for the Frontdesk pipeline.

Use process_frontdesk() as the orchestration entry point from the DAG.
"""

logger = logging.getLogger(__name__)


def process_frontdesk() -> None:
    """
    Fetch, transform, forecast, and publish Frontdesk operations data

    :return: None.
    """
    logger.info("Starting to process frontdesk data...")

    frontdesk_runtime_config = load_frontdesk_runtime_config()

    frontdesk_mssql_db_manager = DatabaseManager(
        profile_name="azure_frontdesk_db",
        db_type="mssql",
        airflow_connection_id="azure_frontdesk_db"
    )
    source_engine = frontdesk_mssql_db_manager._engine

    reporting_postgres_db_manager = DatabaseManager(
        profile_name="frontdesk_db",
        db_type="postgres",
        airflow_connection_id="frontdesk_db"
    )
    target_engine = reporting_postgres_db_manager._engine

    # raw operations from the frontdesk database
    raw_data = fetch_operations(source_engine=source_engine)

    # clean and transform the source data for forecasting
    workdata = transform_data(
        data=raw_data,
        frontdesk_runtime_config=frontdesk_runtime_config,
    )

    if workdata.empty:
        raise AirflowFailException(
            "No Frontdesk operations remain after filtering"
        )

    # Store processed operations in the postgres database
    upload_operations(workdata=workdata, target_engine=target_engine)

    # Generate forecasts and upload them to the postgres database
    predictions = build_forecast(
        workdata=workdata,
        frontdesk_runtime_config=frontdesk_runtime_config,
    )
    if predictions.empty:
        raise AirflowFailException("No forecast rows generated")

    validate_forecast_output(predictions=predictions)

    upload_forecasts(predictions=predictions, target_engine=target_engine)

    logger.info("Finished processing frontdesk data.")
