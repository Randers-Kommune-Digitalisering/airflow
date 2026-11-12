from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
import re


def get_missing_or_empty_keys(config: dict, required_keys: tuple[str, ...]) -> list[str]:
    """
    Return required keys that are missing or have an empty value.

    :param config: Configuration dictionary to validate.
    :param required_keys: Keys that must be present and non-empty.
    :return: Missing/empty key names.
    """
    return [key for key in required_keys if not config.get(key)]


def resolve_month_interval() -> tuple[str, str]:
    """
    Resolve and validate month interval from DAG runtime params.

    :return: Tuple containing (start_month, end_month).
    :raises AirflowFailException: If required params are missing or the interval is invalid.
    """
    ctx = get_current_context()
    start_month = (ctx.get("params") or {}).get("start_month")
    end_month = (ctx.get("params") or {}).get("end_month")

    if not start_month or not end_month:
        raise AirflowFailException("Need to specify both start_month and end_month (YYYYMM).")

    start_month = str(start_month).strip()
    end_month = str(end_month).strip()

    regex_pattern = r"^\d{4}(?:0[1-9]|1[0-2])$"
    if not (re.fullmatch(regex_pattern, start_month) and re.fullmatch(regex_pattern, end_month)):
        raise AirflowFailException("start_month and end_month must be in YYYYMM format.")

    if start_month > end_month:
        raise AirflowFailException("start_month must not be after end_month.")

    return start_month, end_month
