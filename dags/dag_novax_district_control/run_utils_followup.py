from __future__ import annotations

import calendar
import datetime
from dataclasses import dataclass


def determine_run_date() -> datetime.date:
    """
    :return: The run's logical date as a local date in the DAG timezone.
             Falls back to "today" when executed outside Airflow.
    """
    try:
        from airflow.operators.python import get_current_context
        from airflow.utils import timezone

        ctx = get_current_context()
        dag = ctx.get("dag")
        dag_tz = getattr(dag, "timezone", None) or timezone.UTC

        dag_run = ctx.get("dag_run")
        data_interval_end = ctx.get("data_interval_end") or getattr(dag_run, "data_interval_end", None)
        if data_interval_end is None:
            data_interval_end = timezone.utcnow().astimezone(dag_tz)

        return timezone.coerce_datetime(data_interval_end).in_timezone(dag_tz).date()
    except Exception:
        return datetime.datetime.now().date()


@dataclass(frozen=True)
class DateWindow:
    start: datetime.date
    end: datetime.date

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError(f"Invalid DateWindow: end {self.end} must be after start {self.start}")


def _add_months(year: int, month: int, delta_months: int) -> tuple[int, int]:
    total = (year * 12 + (month - 1)) + int(delta_months)
    new_year = total // 12
    new_month = (total % 12) + 1
    return new_year, new_month


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _week_index_in_month(anchor: datetime.date) -> int:
    """
    Return 1..5 where 'week 5' means the last 7 days of the month.

    :param anchor: Date to determine week index for.
    :return: Week index in month (1-5).
    """
    last_day = _last_day_of_month(anchor.year, anchor.month)
    if anchor.day >= (last_day - 6):
        return 5
    return ((anchor.day - 1) // 7) + 1


def _window_for_month(year: int, month: int, week_index: int) -> DateWindow:
    last_day = _last_day_of_month(year, month)

    if week_index >= 5:
        start_day = max(1, last_day - 6)
        end_day = last_day
    else:
        start_day = 1 + (week_index - 1) * 7
        end_day = min(start_day + 6, last_day)

    start = datetime.date(year, month, start_day)
    end = datetime.date(year, month, end_day) + datetime.timedelta(days=1)
    return DateWindow(start=start, end=end)


def coalesce_windows(windows: list[DateWindow]) -> list[DateWindow]:
    """
    Merge overlapping/adjacent windows (i.e., where end1 >= start2).

    :param windows: List of DateWindow to coalesce.
    :return: Coalesced list of DateWindow sorted by start date.
    """
    if not windows:
        return []

    ordered = sorted(windows, key=lambda w: (w.start, w.end))
    merged: list[DateWindow] = [ordered[0]]
    for w in ordered[1:]:
        cur = merged[-1]
        if w.start <= cur.end:
            merged[-1] = DateWindow(start=cur.start, end=max(cur.end, w.end))
        else:
            merged.append(w)
    return merged


def followup_due_date_windows(run_date: datetime.date, months_ahead: int = 9) -> list[DateWindow]:
    """
    Determine due-date date range windows to query for the followup job.

    Rules:
    - DAG runs weekly.
    - Each run targets the 'next-week block' for the anchor month and for the next
      (months_ahead-1) months.
    - Additionally include a window for due dates ~2-3 weeks ahead:
      [run_date+14, run_date+21).

    The 'next-week block' is computed from anchor = run_date + 7 days by taking
    the corresponding week index within that month, where 'week 5' means the last
    7 days of the month.

    :param run_date: The logical date of the DAG run.
    :param months_ahead: Number of months ahead to include (default: 9).
    :return: List of coalesced date windows.
    """
    if months_ahead <= 0:
        raise ValueError("months_ahead must be > 0")

    anchor = run_date + datetime.timedelta(days=7)
    week_index = _week_index_in_month(anchor)

    monthly: list[DateWindow] = []
    for i in range(months_ahead):
        y, m = _add_months(anchor.year, anchor.month, i)
        monthly.append(_window_for_month(y, m, week_index))

    two_weeks = DateWindow(
        start=run_date + datetime.timedelta(days=14),
        end=run_date + datetime.timedelta(days=21),
    )

    return coalesce_windows(monthly + [two_weeks])


def determine_followup_due_date_windows(months_ahead: int = 9) -> list[DateWindow]:
    """
    Convenience wrapper to determine followup due date windows for the current run date.

    :param months_ahead: Number of months ahead to include (default: 9).
    :return: List of coalesced date windows for the current run date.
    """
    return followup_due_date_windows(run_date=determine_run_date(), months_ahead=months_ahead)
