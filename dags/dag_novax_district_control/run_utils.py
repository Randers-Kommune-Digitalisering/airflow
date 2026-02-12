import datetime
import logging

from airflow.models import DagRun
from airflow.operators.python import get_current_context
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

logger = logging.getLogger(__name__)


def determine_date_range() -> tuple[datetime.date, datetime.date] | None:
    """
    Determine the date range for processing based on provided dates or DAG context.
    :return: A tuple of (start_date, end_date) where start is inclusive and end is exclusive.
             Returns None when there is no new interval to process (e.g. start_date >= end_date).
    """
    ctx = get_current_context()
    dag = ctx["dag"]
    dag_id = dag.dag_id
    dag_tz = getattr(dag, "timezone", None) or timezone.UTC

    dag_run = ctx.get("dag_run")
    data_interval_end = ctx.get("data_interval_end") or getattr(dag_run, "data_interval_end", None)
    if data_interval_end is None:
        data_interval_end = timezone.now().in_timezone(dag_tz)
    end_date = timezone.coerce_datetime(data_interval_end).in_timezone(dag_tz).date()

    with create_session() as session:
        prev_success = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.state == DagRunState.SUCCESS,
                DagRun.run_type == DagRunType.SCHEDULED,
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )

    if prev_success is None:
        dag_start = getattr(dag, "start_date", None)
        if dag_start is None:
            raise ValueError("DAG has no start_date and no successful run history.")
        start_date = timezone.coerce_datetime(dag_start).in_timezone(dag_tz).date()
    else:
        prev_end = prev_success.data_interval_end
        if prev_end is None:
            prev_end = timezone.coerce_datetime(prev_success.execution_date) + datetime.timedelta(days=1)
        start_date = timezone.coerce_datetime(prev_end).in_timezone(dag_tz).date()

    logger.info("Determined date range for processing: start_date=%s, end_date=%s", start_date, end_date)
    if start_date >= end_date:
        logger.info("No new data to process: start_date %s is not before end_date %s.", start_date, end_date)
        return None
    return start_date, end_date
