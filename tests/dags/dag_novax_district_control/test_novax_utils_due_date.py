from datetime import date, timedelta

from dags.dag_novax_district_control.novax_utils import calculate_due, parse_journal_data


def test_calculate_due_returns_date_from_string():
    due = calculate_due(gestations_weeks=17, gestations_days=1, dato_str="01.02.2026")
    assert isinstance(due, date)
    assert due == date(2026, 2, 1) + timedelta(days=(40 * 7 - (17 * 7 + 1) - 1))


def test_parse_journal_data_due_dates_are_dates():
    journal = """ADRESSE: Testvej 1, 8000 Aarhus C
Tlf.nr.: 12345678
Gestationsalder
Uge: 17, Dag: 1 
Termin: 26.11.2025
Afsendt: 01-02-2026
"""

    parsed = parse_journal_data(journal)

    assert isinstance(parsed["due_date"], date)
    assert parsed["due_date"] == date(2025, 11, 26)

    assert isinstance(parsed["calculated_due_date"], date)
    assert parsed["calculated_due_date"] == date(2026, 2, 1) + timedelta(days=(40 * 7 - (17 * 7 + 1) - 1))
