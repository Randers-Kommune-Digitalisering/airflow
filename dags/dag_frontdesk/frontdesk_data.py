import logging
import pandas as pd

logger = logging.getLogger(__name__)


def fetch_operations(source_engine) -> pd.DataFrame:
    """
    Reads all operation records from the frontdesk database.
    """
    return pd.read_sql_query(
        "SELECT * FROM Operation",
        con=source_engine
    )


def upload_operations(workdata: pd.DataFrame, target_engine) -> None:
    """
    Replace the operations table in the reporting database
    """
    _upload_to_postgres(workdata, "operations", target_engine)


def upload_forecasts(predictions: pd.DataFrame, target_engine) -> None:
    """
    Replace the forecasts table in the reporting database"""
    _upload_to_postgres(predictions, "forecasts", target_engine)


def _upload_to_postgres(data: pd.DataFrame, table_name: str,
                        target_engine) -> None:
    data.to_sql(
        table_name,
        con=target_engine,
        if_exists="replace",
        index=False
    )
    logger.info("Uploaded %s rows to %s", len(data), table_name)
