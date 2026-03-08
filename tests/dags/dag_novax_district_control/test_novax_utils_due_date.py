from datetime import date, timedelta

from dags.dag_novax_district_control.novax_utils import _calculate_due


def test_calculate_due_returns_date_from_string():
    due = _calculate_due(gestations_weeks=17, gestations_days=1, dato_str="01.02.2026")
    assert isinstance(due, date)
    assert due == date(2026, 2, 1) + timedelta(days=(40 * 7 - (17 * 7 + 1) - 1))
