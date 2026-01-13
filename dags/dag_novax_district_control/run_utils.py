import datetime
from airflow.operators.python import get_current_context
from airflow.models import DagRun
from airflow.utils import timezone as airflow_tz
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from sqlalchemy import desc
import logging

logger = logging.getLogger(__name__)
FINISHED_STATES = {DagRunState.SUCCESS, DagRunState.FAILED}


def _as_local_date(value, tz) -> datetime.date | None:
    """
    Convert a datetime-like value to a timezone-aware date in the given timezone.
    """
    if value is None:
        return None
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    coerced = airflow_tz.coerce_datetime(value)
    return coerced.in_timezone(tz).date()


def _as_local_dt(value, tz):
    """
    Convert a datetime-like value to a timezone-aware datetime in the given timezone.
    """
    if value is None:
        return None
    coerced = airflow_tz.coerce_datetime(value)
    return coerced.in_timezone(tz)


def _infer_daily_interval_end(logical_date, tz):
    """
    Infer the end of a daily data interval based on the logical date and timezone.
    """
    if logical_date is None:
        return None
    local = _as_local_dt(logical_date, tz)
    if local is None:
        return None
    return local + datetime.timedelta(days=1)


def determine_date_range() -> tuple[datetime.date, datetime.date] | None:
    """
    Determine the date range for processing based on provided dates or DAG context.
    :return: A tuple of (start_date, end_date).
    """
    # Determine the date range from the current Airflow run.
    # The processing window is based on full calendar days in the DAG's timezone
    start_date: datetime.date | None = None
    end_date: datetime.date | None = None
    ctx = get_current_context()
    dag = ctx["dag"]
    dag_id = dag.dag_id
    dag_tz = getattr(dag, "timezone", None) or airflow_tz.UTC

    dag_run = ctx.get("dag_run")
    current_run_id = dag_run.run_id if dag_run else None
    current_logical_date = ctx.get("logical_date") or getattr(dag_run, "logical_date", None) or ctx.get("data_interval_start")
    current_data_interval_end = ctx.get("data_interval_end") or getattr(dag_run, "data_interval_end", None)

    if current_data_interval_end is None:
        # Infer for scheduled daily DAGs, otherwise fall back to "now".
        current_data_interval_end = _infer_daily_interval_end(current_logical_date, dag_tz)
    if current_data_interval_end is None:
        current_data_interval_end = airflow_tz.now().in_timezone(dag_tz)

    end_date = _as_local_date(current_data_interval_end, dag_tz)

    # Use the last successful run as the base for start_date.
    # Note: in some Airflow versions, DagRun.logical_date is a Python-level property,
    # while execution_date is the actual SQLAlchemy column used for querying.
    with create_session() as session:
        query = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.state == DagRunState.SUCCESS,
            DagRun.execution_date.isnot(None),
            DagRun.run_type == DagRunType.SCHEDULED,
        )
        if current_run_id is not None:
            query = query.filter(DagRun.run_id != current_run_id)
        if current_logical_date is not None:
            query = query.filter(DagRun.execution_date < airflow_tz.coerce_datetime(current_logical_date))
        prev_success = query.order_by(desc(DagRun.execution_date)).first()

    if prev_success is None:
        # No previous successful runs; start from DAG start_date
        dag_start = getattr(dag, "start_date", None)
        start_date = _as_local_date(dag_start, dag_tz)
        logger.info(
            "No previous successful runs found for this DAG; starting from DAG start_date %s (DAG tz %s).",
            start_date,
            getattr(dag_tz, "name", str(dag_tz)),
        )
    else:
        # Use the end of the last successful run as the start date
        prev_end_dt = getattr(prev_success, "data_interval_end", None) or _infer_daily_interval_end(
            getattr(prev_success, "execution_date", None),
            dag_tz,
        )
        prev_success_end = _as_local_date(prev_end_dt, dag_tz)
        logger.info(
            "Previous successful run %s ended at %s (DAG tz %s).",
            prev_success.run_id,
            prev_success_end,
            getattr(dag_tz, "name", str(dag_tz)),
        )
        start_date = prev_success_end

    # Validate date range
    if not start_date or not end_date:
        raise ValueError("Error inferring start_date and end_date from previous runs.")
    if start_date >= end_date:
        logger.info(f"No new data to process: start_date {start_date} is not before end_date {end_date}.")
        return None, None
    return start_date, end_date
