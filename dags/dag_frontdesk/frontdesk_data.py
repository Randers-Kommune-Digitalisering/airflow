import logging
import pandas as pd

"""
Frontdesk data access module.

This module contains database I/O helpers for the Frontdesk pipeline:
1. Fetch raw operation rows from the source system.
2. Upload transformed operations to the reporting database.
3. Upload forecast results to the reporting database.

All writes replace target tables to keep reporting data aligned per DAG run.
"""

logger = logging.getLogger(__name__)

# Kept as a constant so query intent is explicit and reusable.
OPERATION_QUERY = "SELECT * FROM Operation WHERE CreatedAt >= DATEADD(year, -2, GETDATE())"


def fetch_operations(source_engine) -> pd.DataFrame:
    """
    Reads all operation records from the frontdesk database.

    :param source_engine: Engine for the MSSQL database.
    :return pd.DataFrame: Containing all rows from the Operation table.
    """
    return pd.read_sql_query(OPERATION_QUERY, con=source_engine)


def upload_operations(workdata: pd.DataFrame, target_engine) -> None:
    """
    Replace the operations table in the reporting database.

    :param workdata (pd.DataFrame): Processed operations dataframe to upload.
    :param target_engine: Engine for the target Postgres database.
    :return: None.
    """
    _upload_dataset(workdata, "operations", target_engine)


def upload_forecasts(predictions: pd.DataFrame, target_engine) -> None:
    """
    Replace the forecasts table in the reporting database.

    :param predictions (pd.DataFrame): Forecast dataframe to upload.
    :param target_engine: Engine for the target Postgres database.
    :return: None.
    """
    _upload_dataset(predictions, "forecasts", target_engine)


def _upload_dataset(
    data: pd.DataFrame,
    table_name: str,
    target_engine,
) -> None:
    """
    Upload a dataframe to PostgreSQL by replacing the destination table.

    :param data (pd.DataFrame): Dataframe to upload.
    :param table_name (str): Destination table name.
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
        index=False
    )

    logger.info("Uploaded %s rows to %s", len(data), table_name)
