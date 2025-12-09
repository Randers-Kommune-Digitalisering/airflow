from datetime import datetime, timedelta
import re


def parse_address(address):
    """
    Parse adresse fra en streng til en Address objekt.
    
    :param address: Fuld adresse som en streng, fx "Regimentvej 16F, 3. tv, 8920 Randers NV"
    """
    if not address:
        return None

    match = re.match(
        r'^(?P<street_name>[^\d,]+?)\s+'  # street name
        r'(?P<house_number>\d+\s?[A-Za-z]?)'  # house number with optional letter
        r'(?:\s*,?\s*\d+\s*[a-zA-ZæøåÆØÅ]{1,3})?'  # optional door info, "A" in "3A", etc.
        r'(?:\s*,?\s*(?:st\.?\s*(?:tv|th|mf)?|[0-9]+\.\s*(?:tv|th|mf)?))?'  # optional floor and door info, "st. tv", "1. th", etc.
        r'\s*,?\s*(?P<city_part>[^\d,]+)?\s*'  # optional city part
        r'(?P<postal_code>\d{4})\s*'  # postal code (whitespace optional)
        r'(?:\s*(?P<city_name>.+))?$',  # optional city name
        address.strip()
    )
    if not match:
        return None

    street_name = match.group('street_name').strip()
    house_number = match.group('house_number').replace(' ', '')
    postal_code = match.group('postal_code')
    city_name = match.group('city_name').strip() if match.group('city_name') else ''
    city_part = match.group('city_part')
    if city_part:
        city_part = city_part.strip()
        city_name = f"{city_part} {city_name}".strip()

    from dag_novax_district_control.novax_data import Address
    return Address(
        street=street_name,
        number=house_number,
        postal_code=postal_code,
        city=city_name if city_name else None
    )


def beregn_termin(dato_str, gestations_uger, gestations_dage):
    """
    Beregn terminsdato ud fra en given dato og gestationsalder.

    :param dato_str: Dato som string i formatet 'dd.mm.yyyy', fx '26.11.2025'
    :param gestations_uger: Antal fulde uger i gestationsalderen, fx 17
    :param gestations_dage: Antal dage i gestationsalderen, fx 1
    :return: Terminsdato som datetime objekt
    """
    # Convert dato_str to datetime object
    dato = datetime.strptime(dato_str, '%d.%m.%Y')
    # Total gestational days
    gestations_total_dage = gestations_uger * 7 + gestations_dage
    # Normal pregnancy length in days
    pregnancy_days = 40 * 7  # 280 days
    # Days remaining until due date
    days_until_due = pregnancy_days - gestations_total_dage
    # Calculate due date
    due_date = dato + timedelta(days=days_until_due)

    return due_date

# Example:
# due_date = beregn_termin('26.11.2025', 17, 1)
# print("Due date:", due_date.strftime('%d.%m.%Y'))
