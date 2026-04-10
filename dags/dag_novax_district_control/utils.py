from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional


def _s(value: Any) -> str:
    """Safe normalize to stripped string; returns empty string for None."""
    return str(value).strip() if value is not None else ""


def _i(value: Any) -> Optional[int]:
    """Safe parse int from nullable/blank values; returns None if not parseable."""
    text = _s(value)
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _to_date(value: Any) -> Optional[date]:
    """Convert a datetime/date to date, otherwise return None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None
