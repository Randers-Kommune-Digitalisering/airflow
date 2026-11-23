import csv
import io
import logging
from typing import Any

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from openpyxl.utils import get_column_letter

logger = logging.getLogger(__name__)


def get_eindkomst_report_config() -> tuple[list[str], dict[str, str]]:
    config = Variable.get("eindkomst_report_config", deserialize_json=True)
    if not isinstance(config, dict):
        raise AirflowFailException("eIndkomst report configuration must be a JSON object.")
    fields = config.get("report_field_to_blanket_field_id")
    columns = config.get("report_columns")
    column_fields = config.get("report_column_to_field_name")

    return columns, {column: fields[name] for column, name in column_fields.items()}


def extract_cpr_se_pairs_from_txt(content: bytes) -> list[tuple[str, str]]:
    try:
        text = content.decode("utf-8-sig")
        logger.debug("eIndkomst input decoded as UTF-8.")
    except UnicodeDecodeError:
        try:
            text = content.decode("cp1252")
        except UnicodeDecodeError as error:
            raise AirflowFailException("eIndkomst input could not be decoded as UTF-8 or Windows-1252.") from error

        logger.debug("eIndkomst input decoded as Windows-1252.")

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
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _collect_fields(blanket: dict[str, Any], field_ids: set[str]) -> dict[str, list[Any]]:
    own: dict[str, Any] = {}
    for field in _as_list(blanket.get("AngivelseFeltSamling")):
        if not isinstance(field, dict):
            continue
        field_id = (field.get("BlanketFeltEnhedStruktur") or {}).get("BlanketFeltNummerIdentifikator")
        if str(field_id) in field_ids:
            own[str(field_id)] = field.get("AngivelseFeltIndholdTekst")

    collected: dict[str, list[Any]] = {}
    for child in _as_list(blanket.get("UnderAngivelseSamling")):
        if not isinstance(child, dict):
            continue
        for field_id, values in _collect_fields(child, field_ids).items():
            collected.setdefault(field_id, []).extend(values)
    for field_id, value in own.items():
        collected.setdefault(field_id, [value])
    return collected


def extract_eindkomst_rows(
    payload: dict[str, Any],
    cpr: str,
    se_number: str,
    report_fields: dict[str, str],
) -> list[dict[str, Any]]:
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

                values_by_column: dict[str, Any] = {column: None for column in report_fields}
                content = period.get("AngivelseBlanketIndholdStruktur") or {}

                for declaration in _as_list(content.get("AngivelseOplysningSamling")):
                    if not isinstance(declaration, dict):
                        continue

                    fields = _collect_fields(declaration, set(report_fields.values()))
                    for column, field_id in report_fields.items():
                        if field_id in fields:
                            values_by_column[column] = fields[field_id]

                rows.append({
                    "CPR": cpr,
                    "SE-Nummer": company.get("VirksomhedSENummerIdentifikator"),
                    "Tjenestenummer": period.get("AnsaettelseforholdMedarbejderIdentifikator"),
                    "IndkomstPersonGruppeDispositionDato": disposition_date,
                    "AngivelsePeriode": angivelse_periode,
                    **values_by_column,
                })

    return rows


def write_eindkomst_report_excel_bytes(rows: list[dict[str, Any]], columns: list[str]) -> bytes:
    frame = pd.DataFrame(rows, columns=columns)
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="Indkomstoplysninger")
        sheet = writer.sheets["Indkomstoplysninger"]

        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions

        for column_index, name in enumerate(columns, start=1):
            values = frame.iloc[:, column_index - 1].fillna("").astype(str)
            width = max(int(values.map(len).max()) if not values.empty else 0, len(name)) + 2
            sheet.column_dimensions[get_column_letter(column_index)].width = min(width, 60)

        for row in sheet.iter_rows(min_row=2):
            for cell in row[:3]:
                cell.number_format = "@"

            for cell in row:
                if cell.data_type == "f":
                    cell.data_type = "s"

    return output.getvalue()
