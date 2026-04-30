from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Mapping

from dag_novax_district_control.model import Address, Name, PersonDistrict

logger = logging.getLogger(__name__)

SENTINEL_OPEN_END_DT = datetime(1753, 1, 1)
SENTINEL_OPEN_END_DATE = SENTINEL_OPEN_END_DT.date()


def is_valid_cpr(cpr: str) -> bool:
    """
    Validate a CPR string.

    :param cpr: CPR value as a string.
    :return: True if `cpr` is exactly 10 digits, else False.
    """
    return bool(cpr) and cpr.isdigit() and len(cpr) == 10


def _coerce_to_datetime(value: date | datetime) -> datetime:
    """
    Coerce a `date` or `datetime` into a `datetime`.

    :param value: A `date` or `datetime`.
    :return: A `datetime` value; `date` inputs are converted to midnight.
    """
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, datetime.min.time())


def _is_open_end(value: Any) -> bool:
    """
    Check whether a DB end-date represents an open-ended interval.

    :param value: A date-like value (often `datetime` from SQL Server) or None.

    :return: True when `value` is None or equals the sentinel open-end date.
    """
    if value is None:
        return True
    if isinstance(value, datetime):
        return value.date() == SENTINEL_OPEN_END_DATE
    if isinstance(value, date):
        return value == SENTINEL_OPEN_END_DATE
    return False


def update_protected_address_status(
    *,
    entry: Name,
    is_protected_address: bool,
) -> bool:
    """

    :param entry: Novax `Name` ORM object with joined `details`.
    :param is_protected_address: True if CPR lookup indicates protected address.

    :return: True if a change was applied, else False.
    """
    prev_protected = bool(entry.details.BESKYTTETADRESSE)
    if is_protected_address == prev_protected:
        return False

    entry.details.BESKYTTETADRESSE = int(is_protected_address)
    logger.info("Updated protected address status for Name ID %s", entry.ID)
    return True


def clear_district_due_to_missing_address(
    *,
    entry: Name,
    now_dt: datetime,
    now_time: str,
) -> bool:
    """
    Clear district when address lookup fails.

    Closes any open-ended `PersonDistrict` rows and clears `entry.DISTRIKT`.

    :param entry: Novax `Name` ORM object.
    :param now_dt: Current timestamp.
    :param now_time: Current time formatted as "HH:MM".
    :return: True if `entry.DISTRIKT` was cleared, else False.
    """
    for d in entry.person_districts:
        if _is_open_end(d.DATETO):
            d.DATETO = now_dt
            d.TS_UPDD = now_dt
            d.TS_UPDT = now_time

    if str(entry.DISTRIKT).strip() == "":
        return False

    entry.DISTRIKT = ""
    logger.info("Cleared district for Name ID %s due to missing address", entry.ID)
    return True


def update_address_from_dataforsyning(
    *,
    entry: Name,
    address_info: Mapping[str, Any],
    reference_date: date | datetime,
    close_to_dt: date | datetime,
    new_from_dt: date | datetime,
    now_dt: datetime,
    now_time: str,
) -> bool:
    """
    Update `entry.ADRESSE` and maintain address history rows.

    If the address differs from the current `entry.ADRESSE`, this updates the
    field and ensures there is an `Address` row covering `reference_date`. If
    no covering row exists, open-ended rows are closed at `close_to_dt` and a
    new row is inserted starting at `new_from_dt`.

    :param entry: Novax `Name` ORM object with joined `addresses`.
    :param address_info: Dataforsyning mapping (expects keys like `full_address`,
        `street_code`, `municipality_code`, `postal_code`, `town_name`, `number_floor`).
    :param reference_date: Date to validate coverage against.
    :param close_to_dt: Timestamp/date to use when closing open-ended rows.
    :param new_from_dt: Timestamp/date to use as the new row's start.
    :param now_dt: Current timestamp.
    :param now_time: Current time formatted as "HH:MM".
    :return: True if `entry.ADRESSE` was updated (and possibly history updated), else False.
    """
    new_full_address = address_info.get("full_address")
    if new_full_address == str(entry.ADRESSE).strip():
        return False

    entry.ADRESSE = new_full_address
    logger.info("Updated address for Name ID %s", entry.ID)

    ref_dt = _coerce_to_datetime(reference_date)

    has_valid_address = any(
        a.NR_LT_ETAGE == address_info.get("number_floor")
        and a.VEJKODE == address_info.get("street_code")
        and a.STEDNAVN == address_info.get("town_name")
        and a.POSTNR == address_info.get("postal_code")
        and a.KOMMUNEKODE == address_info.get("municipality_code")
        and (a.DATO_FRA is not None and _coerce_to_datetime(a.DATO_FRA) <= ref_dt)
        and (
            _is_open_end(a.DATO_TIL)
            or (a.DATO_TIL is not None and _coerce_to_datetime(a.DATO_TIL) > ref_dt)
        )
        for a in entry.addresses
    )

    if has_valid_address:
        return True

    close_dt = _coerce_to_datetime(close_to_dt)
    for a in entry.addresses:
        if _is_open_end(a.DATO_TIL):
            a.DATO_TIL = close_dt
            a.TS_UPDD = now_dt
            a.TS_UPDT = now_time
            logger.info(
                "Closed existing address code %s for Name ID %s with end date %s",
                str(a.VEJKODE).strip(),
                entry.ID,
                close_dt,
            )

    from_dt = _coerce_to_datetime(new_from_dt)
    new_address_entry = Address(
        NAVNID=entry.ID,
        VEJKODE=str(address_info["street_code"]),
        KOMMUNEKODE=str(address_info["municipality_code"]),
        POSTNR=str(address_info["postal_code"]),
        STEDNAVN=address_info["town_name"],
        NR_LT_ETAGE=address_info["number_floor"],
        DATO_FRA=from_dt,
        DATO_TIL=SENTINEL_OPEN_END_DT,
        TS_DATE=now_dt,
        TS_TIME=now_time,
        TS_UPDD=now_dt,
        TS_UPDT=now_time,
    )
    entry.addresses.append(new_address_entry)
    logger.info("Added new address for Name ID %s", entry.ID)

    return True


def update_district_from_coordinates(
    *,
    entry: Name,
    new_district: str | None,
    reference_date: date | datetime,
    close_to_dt: date | datetime,
    new_from_dt: date | datetime,
    now_dt: datetime,
    now_time: str,
) -> bool:
    """
    Update `entry.DISTRIKT` and maintain person-district history rows.

    If `new_district` differs from the current district, this updates
    `entry.DISTRIKT` and ensures there is a `PersonDistrict` row covering
    `reference_date`. If no covering row exists, open-ended rows are closed at
    `close_to_dt` and a new row is inserted starting at `new_from_dt`.

    :param entry: Novax `Name` ORM object with joined `person_districts`.
    :param new_district: District code computed from coordinates.
    :param reference_date: Date to validate coverage against.
    :param close_to_dt: Timestamp/date to use when closing open-ended rows.
    :param new_from_dt: Timestamp/date to use as the new row's start.
    :param now_dt: Current timestamp.
    :param now_time: Current time formatted as "HH:MM".
    :return: True if `entry.DISTRIKT` was updated (and possibly history updated), else False.
    """

    if not new_district or new_district == str(entry.DISTRIKT).strip():
        return False

    entry.DISTRIKT = new_district
    logger.info("Updated district for Name ID %s", entry.ID)

    ref_dt = _coerce_to_datetime(reference_date)

    has_valid_person_district = any(
        str(d.DISTRICT).strip() == str(new_district).strip()
        and (d.DATEFROM is not None and _coerce_to_datetime(d.DATEFROM) <= ref_dt)
        and (
            _is_open_end(d.DATETO)
            or (d.DATETO is not None and _coerce_to_datetime(d.DATETO) > ref_dt)
        )
        for d in entry.person_districts
    )

    if has_valid_person_district:
        return True

    close_dt = _coerce_to_datetime(close_to_dt)
    for d in entry.person_districts:
        if _is_open_end(d.DATETO):
            d.DATETO = close_dt
            d.TS_UPDD = now_dt
            d.TS_UPDT = now_time
            logger.info(
                "Closed existing person district %s for Name ID %s with end date %s",
                str(d.DISTRICT).strip(),
                entry.ID,
                close_dt,
            )

    from_dt = _coerce_to_datetime(new_from_dt)
    new_person_district = PersonDistrict(
        NAVNID=entry.ID,
        DISTRICT=new_district,
        DATEFROM=from_dt,
        DATETO=SENTINEL_OPEN_END_DT,
        TS_DATE=now_dt,
        TS_TIME=now_time,
        TS_UPDD=now_dt,
        TS_UPDT=now_time,
    )
    entry.person_districts.append(new_person_district)
    logger.info("Added new person district for Name ID %s: %s", entry.ID, new_district)

    return True


def update_kommunekode(
    *,
    entry: Name,
    kommune_code: str = "730",
) -> tuple[bool, bool]:
    """
    Ensure kommune code is set on `Name` and `NameDetails`.

    :param entry: Novax `Name` ORM object with joined `details`.
    :param kommune_code: Kommune code to enforce.
    :return: Tuple `(changed_name, changed_details)` indicating what was updated.
    """
    is_new_kommunekode = False
    is_new_kommunekode_details = False

    if str(entry.TS_KOMID).strip() != kommune_code:
        entry.TS_KOMID = kommune_code
        is_new_kommunekode = True

    if str(entry.details.TS_KOMID).strip() != kommune_code or str(entry.details.KOMMUNE_OPR).strip() != kommune_code:
        entry.details.TS_KOMID = kommune_code
        entry.details.KOMMUNE_OPR = kommune_code
        is_new_kommunekode_details = True

    return is_new_kommunekode, is_new_kommunekode_details


def ensure_active(*, entry: Name) -> bool:
    """
    Ensure `entry.AKTIV` is set to "1".

    :param entry: Novax `Name` ORM object.
    :return: True if `AKTIV` was changed from ""/"0" to "1", else False.
    """
    if entry.AKTIV not in ("", "0"):
        return False

    entry.AKTIV = "1"
    logger.info("Updated active status for Name ID %s", entry.ID)
    return True
