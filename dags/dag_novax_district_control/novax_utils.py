from __future__ import annotations
from datetime import date, datetime, timedelta
import logging
import re


logger = logging.getLogger(__name__)


def to_int_or_none(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


class Address:
    def __init__(self, street: str | None = None, number: str | None = None, door_extension: str | None = None, postal_code: str | None = None, city: str | None = None, full_address: str | None = None):
        """
        Initialize Address either from components or from full address string.
        Either provide all components (street, number, postal_code, city (optional)) or full_address.
        """
        # Initialize from address components
        if street and number and postal_code:
            self.street: str = street
            self.number: str = number
            self.door_extension: str | None = door_extension  # optional
            self.postal_code: str = postal_code
            self.city: str | None = city  # optional
            self.street_code: str | None = None  # populated by Dataforsyning lookup
            self.is_protected: bool = False  # populated by Novax / CPR lookup

            # Construct full address from components
            self.full_address: str = f"{street} {number}"
            if door_extension:  # Append door extension if provided
                self.full_address += f", {door_extension}"
            self.full_address += f", {postal_code}"
            if city:  # Append city if provided
                self.full_address += f" {city}"

            # Also construct a version of the full address without the door extension and city name for Dataforsyning lookup
            self.address_dataforsyningen_lookup: str = f"{street} {number}, {postal_code}"

        # Initialize from full_address, parsing it into components
        elif full_address:
            parsed = parse_address(full_address)  # Parse address into components
            if parsed:
                # Copy attributes from the parsed object onto this instance
                for attr in ("street", "number", "door_extension", "postal_code", "city", "full_address", "address_dataforsyningen_lookup", "x", "y", "street_code", "municipality_code", "is_protected"):
                    if hasattr(parsed, attr):
                        setattr(self, attr, getattr(parsed, attr))
            else:
                self.full_address = full_address
                raise ValueError("Invalid full_address format.")

        else:
            raise ValueError("Either full_address or all address components must be provided (street, number, postal_code).")


class UserData:
    def __init__(self, cpr: str, navnid: str, address: Address | None, district: str, municipality_code: int | None, tlf_nr: str | None, due_date: date | None, timestamp: datetime, journal: str | None = None):
        self.cpr: str = cpr
        self.navnid: str = navnid
        self.current_address: Address | None = address
        self.current_district: str = district
        self.current_municipality_code: int | None = municipality_code
        self.current_tlf_nr: str | None = tlf_nr
        self.current_due_date: date | None = due_date
        self.timestamp: datetime = timestamp
        self.journal: str | None = journal

        self.new_address: Address | None = None
        self.new_district: str | None = None
        self.new_municipality_code: int | None = None  # populated by Dataforsyning lookup
        self.new_tlf_nr: str | None = None
        self.new_due_date: date | None = None
        self.parsed_journal: dict | None = None


def parse_address(address: str) -> Address | None:
    """
    Parse address from a string into an Address object.

    :param address: Full address as a string, e.g., "Regimentvej 16F, 3. tv, 8920 Randers NV"
    """
    if not address:
        logger.warning("Empty address string provided for parsing.")
        return None

    match = re.match(
        r'^(?P<street_name>[^\d,]+?)\s+'  # street name
        r'(?P<house_number>\d+\s?[A-Za-z]?)'  # house number with optional letter
        r'(?:\s*,?\s*\d+\s*[a-zA-ZæøåÆØÅ]{1,3})?'  # optional door info, "A" in "3A", etc.
        r'(?:\s*,?\s*(?P<door_extension>(?:st\.?\s*(?:tv|th|mf)?|[0-9]+\.\s*(?:tv|th|mf)?)))?'  # optional floor and door info, "st. tv", "1. th", etc.
        r'\s*,?\s*(?P<city_part>[^\d,]+)?\s*'  # optional city part
        r'(?P<postal_code>\d{4})\s*'  # postal code (whitespace optional)
        r'(?:\s*(?P<city_name>.+))?$',  # optional city name
        address.strip()
    )
    if not match:
        logger.warning(f"Address string did not match expected format: {address}")
        return None

    street_name = match.group('street_name').strip()
    house_number = match.group('house_number').replace(' ', '')
    door_extension = match.group('door_extension').strip() if match.group('door_extension') else None
    postal_code = match.group('postal_code')
    city_name = match.group('city_name').strip() if match.group('city_name') else ''
    city_part = match.group('city_part')
    if city_part:
        city_part = city_part.strip()
        city_name = f"{city_part} {city_name}".strip()

    if not street_name or not house_number or not postal_code:
        logger.warning(f"Incomplete address components: street_name={street_name}, house_number={house_number}, postal_code={postal_code}")
        return None

    return Address(
        street=street_name,
        number=house_number,
        door_extension=door_extension,
        postal_code=postal_code,
        city=city_name if city_name else None
    )


def calculate_due(gestations_weeks: int, gestations_days: int, dato_str: str | None = None, dato_obj: date | datetime | None = None) -> date:
    """
    Calculate due date based on gestational age and given date.

    :param gestations_weeks: Number of full weeks in gestational age, e.g., 17
    :param gestations_days: Number of days in gestational age, e.g., 1
    :param dato_str: Date as a string in the format 'dd.mm.yyyy', e.g., '26.11.2025'
    :param dato_obj: Date as a datetime/date object
    """
    # Convert inputs to a date object
    if dato_obj is not None:
        if isinstance(dato_obj, datetime):
            dato = dato_obj.date()
        elif isinstance(dato_obj, date):
            dato = dato_obj
        else:
            raise TypeError("dato_obj must be a datetime or date object")
    elif dato_str is not None:
        dato = datetime.strptime(dato_str, '%d.%m.%Y').date()
    else:
        raise ValueError("Either dato_str or dato_obj must be provided")

    # Total gestational days
    gestations_total_days = gestations_weeks * 7 + gestations_days
    # Normal pregnancy length in days
    pregnancy_days = 40 * 7  # 280 days
    # Days remaining until due date
    days_until_due = pregnancy_days - gestations_total_days
    # Calculate due date (subtract 1 day to match clinical convention)
    return dato + timedelta(days=days_until_due - 1)


def parse_journal_data(journal_string: str, journal_date: datetime | None = None) -> dict:
    """
    Parse journal data from Novax to dict.

    :param journal_string: Journal data as string.
    """
    if not journal_string:
        journal_string = ""

    address_match = re.search(r'ADRESSE:\s*(.+?)\r?\n', journal_string)
    address = address_match.group(1).strip() if address_match else None

    phone_match = re.search(r'(?:Tlf\.?nr?\.?|Mobil):\s*(\d+)\r?\n', journal_string, re.IGNORECASE)
    phone = phone_match.group(1).strip() if phone_match else None

    gest_match = re.search(r'Gestationsalder\r?\nUge:\s*(\d+),\s*Dag:\s*(\d+)\s', journal_string)
    gest_week = int(gest_match.group(1)) if gest_match else None
    gest_day = int(gest_match.group(2)) if gest_match else None

    termin_match = re.search(
        r'(?:T(?:ermin)?\s*:?\s*)(?P<date>(\d{1,2}[./-]\d{1,2}[./-]\d{2,4}))',
        journal_string
    )
    termin_str = termin_match.group('date') if termin_match else None
    termin_date: date | None = None
    if termin_str:
        normalized = re.sub(r'[/-]', '.', termin_str)  # Normalize separators to dots
        parts = normalized.split('.')  # Handle year: 2 or 4 digits
        if len(parts) == 3:
            day, month, year = parts
            if len(year) == 2:
                year = '20' + year
            try:
                termin_date = datetime.strptime(f"{day}.{month}.{year}", '%d.%m.%Y').date()
            except ValueError:
                logger.warning(f"Invalid termin date format in journal: {termin_str}")

    # Calculate due date using gestational age and journal date
    dato_str = None
    dato_obj: date | datetime | None = None
    if journal_date is not None:
        dato_obj = journal_date
    else:
        afsendt_match = re.search(r'Afsendt:\s*(\d{2})-(\d{2})-(\d{4})', journal_string)
        if afsendt_match:
            dato_str = f"{afsendt_match.group(1)}.{afsendt_match.group(2)}.{afsendt_match.group(3)}"
    if (dato_obj or dato_str) and gest_week is not None and gest_day is not None:
        calculated_termin_date = calculate_due(gest_week, gest_day, dato_str=dato_str, dato_obj=dato_obj)
    else:
        calculated_termin_date = None

    return {
        'address': address,
        'phone': phone,
        'gestational_week': gest_week,
        'gestational_day': gest_day,
        'due_date': termin_date,
        'calculated_due_date': calculated_termin_date
    }
