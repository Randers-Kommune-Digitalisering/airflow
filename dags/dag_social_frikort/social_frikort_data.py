import io
import pandas as pd
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from openpyxl.utils import get_column_letter


def get_social_frikort_report_fields() -> dict[str, str]:
    """
    Get the configured report column names and their blanket field identifiers.

    :return: Mapping of report column names to blanket field identifiers.
    :raises AirflowFailException: If the configuration or field mapping is invalid.
    """
    config = Variable.get("social_frikort_report_config", deserialize_json=True)
    if not isinstance(config, dict):
        raise AirflowFailException("Socialt Frikort report configuration must be a JSON object.")
    fields = config.get("report_field_to_blanket_field_id")
    if (
        not isinstance(fields, dict)
        or len(fields) != 2
        or any(
            not isinstance(name, str) or not name.strip()
            or not isinstance(field_id, str) or not field_id.strip()
            for name, field_id in fields.items()
        )
        or len(set(fields.values())) != 2
    ):
        raise AirflowFailException(
            "Socialt Frikort report configuration requires exactly two distinct "
            "field IDs in report_field_to_blanket_field_id."
        )
    return fields


def social_frikort_income_columns() -> list[str]:
    """
    Get income report columns in worksheet order.

    :return: CPR, search period and total income columns.
    """
    return ["CPR", "Søgeperiode", "Samlet Indtægt"]


def social_frikort_address_columns() -> list[str]:
    """
    Get address report columns in worksheet order.

    :return: List of CPR, address, and moving date columns.
    """
    return [
        "CPR", "AdresseType", "StandardAdresse", "Postnummer",
        "Vejnavn", "Husnummer", "Tilflytningsdato", "Fraflytningsdato",
        "Etage", "Sidedoer", "Postdistrikt"
    ]


def extract_social_frikort_address_rows(
    payload: dict[str, Any], cpr: str
) -> list[dict[str, Any]]:
    """
    Extract the current address followed by historical addresses for one CPR.

    :param payload: Address response containing current and historical addresses.
    :param cpr: CPR to include in each address row.
    :return: Address rows, or one CPR-only row if no addresses are available.
    """
    address = payload.get("adresse") or {}
    rows: list[dict[str, Any]] = []
    current = address.get("aktuelAdresse")
    if isinstance(current, dict):
        rows.append({
            "CPR": cpr,
            "AdresseType": "Aktuel",
            "StandardAdresse": current.get("standardadresse"),
            "Postnummer": current.get("postnummer"),
            "Tilflytningsdato": _address_date(value=current.get("tilflytningsdato")),
            "Postdistrikt": current.get("postdistrikt")
        })

    for historical in address.get("adresseHistorik") or []:
        if not isinstance(historical, dict):
            continue
        rows.append({
            "CPR": cpr,
            "AdresseType": "Historisk",
            "Vejnavn": historical.get("vejnavn"),
            "Husnummer": historical.get("husnummer"),
            "Tilflytningsdato": _address_date(value=historical.get("tilflytningsdato")),
            "Fraflytningsdato": _address_date(value=historical.get("fraflytningsdato")),
            "Postnummer": historical.get("postnummer"),
            "Etage": historical.get("etage"),
            "Sidedoer": historical.get("sidedoer"),
            "Postdistrikt": historical.get("postdistrikt"),
        })

    return rows or [{"CPR": cpr}]


def _address_date(value: Any) -> str | None:
    """
    Extract the date part of an address date value.

    :param value: Address date mapping containing a dato value, or another value.
    :return: Date string without the time component, or None if unavailable.
    """
    date = value.get("dato") if isinstance(value, dict) else None
    return str(date).split("T")[0] if date else None


def _coerce_to_list(value: Any) -> list[Any]:
    """
    Coerce an optional scalar or list value into a list.

    :param value: A single value, list of values, or None.
    :return: Empty list for None, the original list, or a single-item list.
    """
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _iter_blanket_tree(blanket: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """
    Traverse a blanket and its UnderAngivelseSamling children depth-first.

    :param blanket: Root blanket node.
    :return: Iterator of blanket nodes, starting with the root.
    """
    yield blanket
    for child in _coerce_to_list(value=blanket.get("UnderAngivelseSamling")):
        if isinstance(child, dict):
            yield from _iter_blanket_tree(child)


def _extract_fields(blanket: dict[str, Any], field_ids: set[str]) -> dict[str, Any]:
    """
    Extract configured field values from a blanket tree.

    :param blanket: Root blanket node containing fields and child blankets.
    :param field_ids: Blanket field identifiers to include.
    :return: Mapping of matching field identifiers to their values.
    """
    fields: dict[str, Any] = {}
    for node in _iter_blanket_tree(blanket=blanket):
        for felt in _coerce_to_list(value=node.get("AngivelseFeltSamling")):
            if not isinstance(felt, dict):
                continue
            field_id = (
                (felt.get("BlanketFeltEnhedStruktur") or {})
                .get("BlanketFeltNummerIdentifikator")
            )
            if str(field_id) in field_ids:
                # Later matches in nested blankets replace earlier values for the same field ID.
                fields[str(field_id)] = felt.get("AngivelseFeltIndholdTekst")
    return fields


def _income_amount(value: object) -> Decimal:
    """
    Convert a SKAT income value to a finite decimal amount.

    :param value: Unvalidated income value from the SKAT response.
    :return: Decimal amount, or zero for an empty value.
    """
    if value is None or value == "":
        return Decimal(0)
    if not isinstance(value, (str, int, float, Decimal)) or isinstance(value, bool):
        raise AirflowFailException("Invalid income amount in SKAT response.")
    text = str(value).strip()
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        amount = Decimal(text)
    except InvalidOperation as error:
        raise AirflowFailException("Invalid income amount in SKAT response.") from error
    if not amount.is_finite():
        raise AirflowFailException("Invalid income amount in SKAT response.")
    return amount


def extract_social_frikort_income_total(
    payload: dict[str, Any], report_fields: dict[str, str]
) -> Decimal:
    """
    Sum the configured income fields across the entire SKAT response.

    Traverse people, income entries, periods, and blanket trees.

    :param payload: Raw response from IndkomstOplysningPersonHent_O.
    :param report_fields: Mapping of income names to blanket field identifiers.
    :return: Total income, including zero when no matching fields are found.
    """
    if not isinstance(payload, dict):
        raise AirflowFailException("Invalid SKAT income response.")

    root = payload.get("IndkomstOplysningPersonHent_O") or {}
    person_data = root.get("IndkomstPersonUddata") or {}
    item = person_data.get("Item") or {}
    people = _coerce_to_list(value=item.get("IndkomstOplysningPersonStruktur"))
    total = Decimal(0)
    field_ids = set(report_fields.values())

    for person in people:
        if not isinstance(person, dict):
            continue
        for income_info in _coerce_to_list(value=person.get("IndkomstOplysningSamling")):
            if not isinstance(income_info, dict):
                continue
            for period in _coerce_to_list(value=income_info.get("IndkomstLoenPeriodeSamling")):
                if not isinstance(period, dict):
                    continue
                blanket_content = period.get("AngivelseBlanketIndholdStruktur") or {}
                for declaration in _coerce_to_list(value=blanket_content.get("AngivelseOplysningSamling")):
                    if not isinstance(declaration, dict):
                        continue
                    values = _extract_fields(blanket=declaration, field_ids=field_ids)
                    total += sum((_income_amount(value) for value in values.values()), Decimal(0))

    return total


def write_social_frikort_report_excel_bytes(
    rows: list[dict[str, Any]], address_rows: list[dict[str, Any]],
) -> bytes:
    """
    Write the Socialt Frikort report to Excel bytes with two sheets.

    Sheet 1: Indkomstoplysninger.
    Sheet 2: Adresse Historik (with blank rows separating CPR groups).

    :param rows: Extracted income report rows.
    :param address_rows: Current and historical address rows grouped by CPR.
    :return: Excel file as bytes.
    """
    income_df = pd.DataFrame(rows, columns=social_frikort_income_columns())
    separated_address_rows: list[dict[str, Any]] = []
    for address_row in address_rows:
        if separated_address_rows and address_row["CPR"] != separated_address_rows[-1].get("CPR"):
            separated_address_rows.append({})
        separated_address_rows.append(address_row)
    address_df = pd.DataFrame(separated_address_rows, columns=social_frikort_address_columns())
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, frame in (
            ("Indkomstoplysninger", income_df),
            ("Adresse Historik", address_df),
        ):
            frame.to_excel(writer, index=False, sheet_name=sheet_name)
            worksheet = writer.sheets[sheet_name]
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions

            for column_index, column_name in enumerate(frame.columns, start=1):
                values = frame[column_name].fillna("").astype(str)
                content_width = values.map(len).max() if not values.empty else 0
                width = max(int(content_width), len(column_name)) + 2
                worksheet.column_dimensions[get_column_letter(column_index)].width = min(width, 60)

    return output.getvalue()
