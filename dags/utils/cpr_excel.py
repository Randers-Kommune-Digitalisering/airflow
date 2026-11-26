import logging
import pandas as pd

logger = logging.getLogger(__name__)


def _normalize_cpr(value: object) -> str | None:
    """
    Normalize CPR value by removing - separator
    """
    if value is None or pd.isna(value):
        return None

    normalized = str(value).strip().replace("-", "")

    return normalized


def extract_cprs(
    df: pd.DataFrame,
    column: str | None = None,
) -> list[str]:
    """
    Extract CPR numbers from a DataFrame column without deduplicating.

    :param df: DataFrame from Excel.
    :param column: Column name containing CPR numbers.
    :return: CPR list
    """
    if column is None:
        raise ValueError("Column name must be provided for CPR extraction")

    cprs = [
        cpr
        for cpr in df[column].map(_normalize_cpr).tolist()
        if cpr is not None
    ]
    logger.info(f"Extracted {len(cprs)} CPR values from Excel column '{column}'")
    return cprs
