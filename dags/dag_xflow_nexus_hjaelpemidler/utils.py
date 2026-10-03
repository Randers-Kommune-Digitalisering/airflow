from datetime import date


def calculate_age(cpr: str) -> int:
    """Calculate a person's age in years from a Danish CPR number, formatted as 'xxxxxx-xxxx' or 'xxxxxxxxxx'."""
    digits = cpr.replace("-", "")
    if len(digits) != 10 or not digits.isdigit():
        raise ValueError("Invalid CPR number")

    day, month, year, century_digit = int(digits[0:2]), int(digits[2:4]), int(digits[4:6]), int(digits[6])

    # Century is derived from the 7th digit combined with the 2-digit year, per the official CPR rules.
    if century_digit in (0, 1, 2, 3):
        century = 1900
    elif century_digit in (4, 9):
        century = 2000 if year <= 36 else 1900
    else:
        century = 2000 if year <= 57 else 1800

    try:
        birth_date = date(century + year, month, day)
    except ValueError as error:
        raise ValueError("Invalid CPR number") from error

    today = date.today()

    age = today.year - birth_date.year
    if (today.month, today.day) < (birth_date.month, birth_date.day):
        age -= 1
    return age
