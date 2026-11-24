import csv
import io
import logging
from typing import Any, Iterator

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter


logger = logging.getLogger(__name__)


def get_eindkomst_report_config() -> tuple[list[str], dict[str, str], dict[str, str]]:
    """
    Load eIndkomst report columns and field mappings from Airflow Variables.

    :return: Ordered columns, column-to-SKAT-field identifiers, and column-to-field names.
    """
    config = Variable.get("eindkomst_report_config", deserialize_json=True)
    if not isinstance(config, dict):
        raise AirflowFailException("eIndkomst report configuration must be a JSON object.")
    fields = config.get("report_field_to_blanket_field_id")
    columns = config.get("report_columns")
    column_fields = config.get("report_column_to_field_name")

    return columns, {column: fields[name] for column, name in column_fields.items()}, column_fields


def extract_cpr_se_pairs_from_txt(content: bytes) -> list[tuple[str, str]]:
    """
    Read distinct CPR-NUMMER and SE-NR pairs from a semicolon-separated file.

    The input must be UTF-8 encoded and contain both named columns. Duplicate
    pairs are removed while preserving their first occurrence.

    :param content: Raw bytes of the eIndkomst text file.
    :return: Ordered pairs of CPR numbers and SE numbers.
    """
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise AirflowFailException("eIndkomst input must be encoded as UTF-8.") from error

    logger.info("eIndkomst input decoded as UTF-8.")

    reader = csv.reader(io.StringIO(text, newline=""), delimiter=";", strict=True)
    try:
        header = next(reader)
    except (StopIteration, csv.Error) as error:
        raise AirflowFailException("eIndkomst input is missing a valid header.") from error

    header = [column.strip() for column in header]
    if header.count("CPR-NUMMER") != 1 or header.count("SE-NR") != 1:
        raise AirflowFailException("eIndkomst input header must contain one CPR-NUMMER and one SE-NR column.")

    cpr_index = header.index("CPR-NUMMER")
    se_index = header.index("SE-NR")
    pairs: list[tuple[str, str]] = []

    try:
        for row in reader:
            if not row or all(not field.strip() for field in row):
                continue

            line_number = reader.line_num
            if line_number == 2 and all(not field.strip() for field in row[:cpr_index + 1]):  # Skip second line if it is empty up to the CPR-NUMMER column
                continue

            if max(cpr_index, se_index) >= len(row):
                raise AirflowFailException(f"Missing CPR-NUMMER or SE-NR on input line {line_number}.")

            cpr = row[cpr_index].strip()
            if len(cpr) != 10 or not cpr.isascii() or not cpr.isdecimal():
                raise AirflowFailException(f"Invalid CPR-NUMMER on input line {line_number}.")

            pairs.append((cpr, row[se_index].strip()))
    except csv.Error as error:
        raise AirflowFailException(f"Invalid eIndkomst text format near line {reader.line_num}.") from error

    if not pairs:
        raise AirflowFailException("No CPR-NUMMER values found in the eIndkomst text file.")

    return list(dict.fromkeys(pairs))


def _as_list(value: Any) -> list[Any]:
    """
    Coerce an optional scalar or list value into a list.

    :param value: A single value, list of values, or None.
    :return: Empty list for None, the original list, or a single-item list.
    """
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _collect_fields(blanket: dict[str, Any], field_ids: set[str]) -> dict[str, Any]:
    """
    Collect requested field values from a single blanket.

    :param blanket: Blanket node containing fields.
    :param field_ids: SKAT field identifiers to include.
    :return: Mapping from requested field identifiers to their raw values.
    """
    own: dict[str, Any] = {}
    for field in _as_list(blanket.get("AngivelseFeltSamling")):
        if not isinstance(field, dict):
            continue
        field_id = (field.get("BlanketFeltEnhedStruktur") or {}).get("BlanketFeltNummerIdentifikator")
        if str(field_id) in field_ids:
            own[str(field_id)] = field.get("AngivelseFeltIndholdTekst")

    return own


def _iter_blankets(blanket: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield blanket
    for child in _as_list(blanket.get("UnderAngivelseSamling")):
        if isinstance(child, dict):
            yield from _iter_blankets(child)


def extract_eindkomst_rows(
    payload: dict[str, Any],
    cpr: str,
    se_number: str,
    report_fields: dict[str, str],
) -> list[dict[str, Any]]:
    """
    Extract income periods matching the requested CPR and SE number.

    :param payload: Raw response payload from IndkomstOplysningPersonHent_O.
    :param cpr: CPR number to include in the report.
    :param se_number: Employer SE number to include in the report.
    :param report_fields: Mapping from report column names to SKAT field identifiers.
    :return: Report rows for matching income periods.
    """
    if not isinstance(payload, dict):
        raise AirflowFailException("Invalid SKAT income response.")

    root = payload.get("IndkomstOplysningPersonHent_O") or {}
    person_data = root.get("IndkomstPersonUddata") or {}
    item = person_data.get("Item") or {}
    rows: list[dict[str, Any]] = []

    for person in _as_list(item.get("IndkomstOplysningPersonStruktur")):
        if not isinstance(person, dict):
            continue
        if str(person.get("PersonCivilRegistrationIdentifier", cpr)).strip() != cpr:
            continue

        for income in _as_list(person.get("IndkomstOplysningSamling")):
            if not isinstance(income, dict):
                continue

            company = (income.get("IndberetningPligtigVirksomhedStruktur") or {}).get(
                "IndberetningPligtigVirksomhed"
            ) or {}
            # Exclude income reported by other companies, only the SE number from the input belongs in the report.
            if str(company.get("VirksomhedSENummerIdentifikator", "")).strip() != se_number:
                continue

            for period in _as_list(income.get("IndkomstLoenPeriodeSamling")):
                if not isinstance(period, dict):
                    continue

                disposition_date = period.get("IndkomstPersonGruppeDispositionDato")
                disposition_date = str(disposition_date).split("T")[0] if disposition_date else None

                period_items = _as_list(
                    ((period.get("AngivelsePeriodeStruktur") or {}).get("AngivelsePeriode") or {}).get("Items")
                )
                period_dates = [str(value).split("T")[0] if value else None for value in period_items[:2]]
                start_date = period_dates[0] if period_dates else None
                end_date = period_dates[1] if len(period_dates) > 1 else None
                angivelse_periode = (
                    f"{start_date} - {end_date}" if start_date and end_date else start_date or end_date
                )

                content = period.get("AngivelseBlanketIndholdStruktur") or {}
                base_row = {
                    "CPR": cpr,
                    "SE-Nummer": company.get("VirksomhedSENummerIdentifikator"),
                    "Tjenestenummer": period.get("AnsaettelseforholdMedarbejderIdentifikator"),
                    "IndkomstPersonGruppeDispositionDato": disposition_date,
                    "AngivelsePeriode": angivelse_periode,
                }
                field_ids = set(report_fields.values())
                declarations = [
                    declaration for declaration in _as_list(content.get("AngivelseOplysningSamling"))
                    if isinstance(declaration, dict)
                ]
                if not declarations:
                    declarations = [{}]

                for declaration in declarations:
                    shared_fields: dict[str, Any] = {}
                    income_records: list[dict[str, Any]] = []
                    for blanket in _iter_blankets(declaration):
                        fields = _collect_fields(blanket, field_ids)
                        if str(blanket.get("BlanketNummerIdentifikator")) == "16001":
                            income_records.append(fields)
                        else:
                            shared_fields.update(fields)

                    for income_fields in income_records or [{}]:
                        fields = {**shared_fields, **income_fields}
                        rows.append({
                            **base_row,
                            **{column: fields.get(field_id) for column, field_id in report_fields.items()},
                        })

    return rows


def write_eindkomst_report_excel_bytes(
    rows: list[dict[str, Any]],
    columns: list[str],
    column_descriptions: dict[str, str],
) -> bytes:
    """
    Write an eIndkomst report to Excel bytes with one income worksheet.

    Rows are grouped by CPR with blank rows between groups, and identifiers
    are formatted as text.

    :param rows: Income rows to write to the report.
    :param columns: Ordered report column names.
    :param column_descriptions: Descriptions to display below column headers.
    :return: Excel file as bytes.
    """
    rows_by_cpr: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_cpr.setdefault(row["CPR"], []).append(row)

    spaced_rows: list[dict[str, Any]] = []
    for group in rows_by_cpr.values():
        if spaced_rows:
            spaced_rows.append({})
        spaced_rows.extend(group)

    frame = pd.DataFrame(spaced_rows, columns=columns)
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="Indkomstoplysninger")
        sheet = writer.sheets["Indkomstoplysninger"]
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions

        for column_index, name in enumerate(columns, start=1):
            description = column_descriptions.get(name)
            header = sheet.cell(row=1, column=column_index)

            if description:
                header.value = f"{name}\n{description}"
                header.alignment = Alignment(wrap_text=True, vertical="center")

            values = frame.iloc[:, column_index - 1].fillna("").astype(str)
            header_width = max(len(line) for line in (name, *(description.splitlines() if description else ())))
            data_width = int(values.map(len).max()) if not values.empty else 0
            sheet.column_dimensions[get_column_letter(column_index)].width = min(
                max(header_width, data_width) + 2, 255
            )

        if any(column_descriptions.get(name) for name in columns):
            sheet.row_dimensions[1].height = 32

        for row in sheet.iter_rows(min_row=2):
            for cell in row[:3]:
                cell.number_format = "@"
            for cell in row:
                if cell.data_type == "f":
                    cell.data_type = "s"

    return output.getvalue()
