from __future__ import annotations
from datetime import date, datetime, timedelta
import logging
import re


logger = logging.getLogger(__name__)


def _calculate_due(gestations_weeks: int, gestations_days: int, date_obj: date) -> date:
    """
    Calculate due date based on gestational age and given date.

    :param gestations_weeks: Number of full weeks in gestational age, e.g., 17
    :param gestations_days: Number of days in gestational age, e.g., 1
    :param date_obj: Date as a datetime date object
    """
    # Total gestational days
    gestations_total_days = gestations_weeks * 7 + gestations_days
    # Normal pregnancy length in days
    pregnancy_days = 40 * 7  # 280 days
    # Days remaining until due date
    days_until_due = pregnancy_days - gestations_total_days
    # Calculate due date (subtract 1 day to match clinical convention)
    return date_obj + timedelta(days=days_until_due - 1)


def parse_journal_data(journal_string: str) -> dict:
    """
    Parse journal data from Novax to dict.

    :param journal_string: Journal data as string.
    :return: A dictionary containing:
        phone number,
        due date (if explicitly stated in journal),
        calculated due date (based on gestational age and journal date)
    """

    date_match = re.search(r"Afsendt:\s*(\d{2}-\d{2}-\d{4})\s*kl\.\s*(\d{2}:\d{2})", journal_string)
    journal_date = datetime.strptime(date_match.group(1) + " " + date_match.group(2), "%d-%m-%Y %H:%M").date() if date_match else None

    phone_match = re.search(r'(?:(?:Tlf\.*)(?:\s*nr\.*)?|Mobil):*[\s ](?:(?:\+|00)45\s?)?(\d{8}|(?:\d{2}\s){3}\d{2})', journal_string, re.IGNORECASE)
    phone = phone_match.group(1).strip() if phone_match else None
    normalized_phone = normalize_phone_number(phone)

    gest_match = re.search(r'Gestationsalder\r?\nUge:\s*(\d{1,2})(?:,\s*Dag:\s*(\d)\s?)?', journal_string)
    gest_week = int(gest_match.group(1)) if gest_match else None
    gest_day = int(gest_match.group(2)) if gest_match and gest_match.group(2) else 0  # Default to 0 if not found, as gestational days may not always be provided.

    termin_match = re.search(
        r'(?:T(?:ermin)?\s*:?)\s*(?:d\.*|den)?\s*(?P<date>\d{1,2}[./-]{1}\d{1,2}(?:[\s./-](?:\d{4}|\d{2}))?)',
        journal_string
    )
    termin_str = termin_match.group('date') if termin_match else None
    due_date: date | None = None

    if termin_str:
        normalized = re.sub(r'[\s/-]+', '.', termin_str)  # Normalize separators to dots
        parts = [part for part in normalized.split('.') if part]  # Handle missing or short year
        if len(parts) in (2, 3):
            day, month = parts[0], parts[1]
            if len(parts) == 3:
                year = parts[2]
                if len(year) == 2:
                    year = '20' + year
            else:
                today = date.today()
                try:
                    current_year_candidate = date(today.year, int(month), int(day))
                except ValueError:
                    logger.warning(f"Invalid termin date format in journal: {termin_str}")
                    current_year_candidate = None

                if current_year_candidate is not None:
                    year = str(today.year if current_year_candidate > today else today.year + 1)
                else:
                    year = ""
            if year:
                try:
                    due_date = datetime.strptime(f"{day}.{month}.{year}", '%d.%m.%Y').date()
                except ValueError:
                    logger.warning(f"Invalid termin date format in journal: {termin_str}")

    if journal_date and gest_week is not None and gest_day is not None:
        calculated_due_date = _calculate_due(gest_week, gest_day, date_obj=journal_date)
    else:
        calculated_due_date = None

    return {
        'phone': normalized_phone,
        'due_date': due_date,
        'calculated_due_date': calculated_due_date
    }


def get_allowed_journal_times(journal_time: str) -> set[str]:
    """
    Get allowed journal times based on the given journal time.

    :param journal_time: Journal time as string in format "HH:MM"
    :return: A set of allowed journal times (base time and one minute later)
    """
    base_dt = datetime.strptime(journal_time.strip(), "%H:%M")
    next_dt = base_dt + timedelta(minutes=1)
    return {base_dt.strftime("%H:%M"), next_dt.strftime("%H:%M")}


def normalize_phone_number(phone_number: str | None) -> str:
    if not phone_number:
        return ""

    # Keep only digits to avoid false mismatches from formatting/trailing spaces.
    normalized = "".join(ch for ch in str(phone_number).strip() if ch.isdigit())
    return normalized
