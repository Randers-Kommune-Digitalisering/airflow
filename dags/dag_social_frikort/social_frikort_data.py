import io
from typing import Any, Iterator

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from openpyxl.utils import get_column_letter


def get_social_frikort_report_fields() -> dict[str, str]:
    config = Variable.get("social_frikort_report_config", deserialize_json=True)
    if not isinstance(config, dict):
        raise AirflowFailException("Socialt Frikort report configuration must be a JSON object.")
    fields = config.get("report_field_to_blanket_field_id")
    if (
        not isinstance(fields, dict)
        or not fields
        or any(
            not isinstance(name, str) or not name.strip()
            or not isinstance(field_id, str) or not field_id.strip()
            for name, field_id in fields.items()
        )
    ):
        raise AirflowFailException(
            "Socialt Frikort report configuration requires a non-empty "
            "report_field_to_blanket_field_id mapping of column names to field IDs."
        )
    return fields


def social_frikort_report_columns(report_fields: dict[str, str]) -> list[str]:
    return ["cpr", "IndkomstPersonGruppeDispositionDato", "AngivelsePeriode", *report_fields]


def _coerce_to_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _iter_blanket_tree(blanket: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield blanket
    for child in _coerce_to_list(value=blanket.get("UnderAngivelseSamling")):
        if isinstance(child, dict):
            yield from _iter_blanket_tree(child)


def _extract_fields(blanket: dict[str, Any], field_ids: set[str]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for node in _iter_blanket_tree(blanket):
        for felt in _coerce_to_list(value=node.get("AngivelseFeltSamling")):
            if not isinstance(felt, dict):
                continue
            field_id = (
                (felt.get("BlanketFeltEnhedStruktur") or {})
                .get("BlanketFeltNummerIdentifikator")
            )
            if str(field_id) in field_ids:
                fields[str(field_id)] = felt.get("AngivelseFeltIndholdTekst")
    return fields


def extract_social_frikort_income_rows(
    payload: dict[str, Any], report_fields: dict[str, str]
) -> list[dict[str, Any]]:
    """Extract CPR, dates and configured income fields from a SKAT response."""
    if not isinstance(payload, dict):
        return []

    root = payload.get("IndkomstOplysningPersonHent_O") or {}
    person_data = root.get("IndkomstPersonUddata") or {}
    item = person_data.get("Item") or {}
    people = _coerce_to_list(value=item.get("IndkomstOplysningPersonStruktur"))
    rows: list[dict[str, Any]] = []

    for person in people:
        if not isinstance(person, dict):
            continue
        cpr = person.get("PersonCivilRegistrationIdentifier")
        for income_info in _coerce_to_list(value=person.get("IndkomstOplysningSamling")):
            if not isinstance(income_info, dict):
                continue
            for period in _coerce_to_list(value=income_info.get("IndkomstLoenPeriodeSamling")):
                if not isinstance(period, dict):
                    continue
                disposition_date = period.get("IndkomstPersonGruppeDispositionDato")
                disposition_date = str(disposition_date).split("T")[0] if disposition_date else None
                period_items = (
                    ((period.get("AngivelsePeriodeStruktur") or {}).get("AngivelsePeriode") or {})
                    .get("Items")
                )
                period_items = _coerce_to_list(period_items)
                period_from = str(period_items[0]).split("T")[0] if period_items and period_items[0] else None
                period_to = str(period_items[1]).split("T")[0] if len(period_items) > 1 and period_items[1] else None
                declaration_period = (
                    f"{period_from} - {period_to}"
                    if period_from and period_to
                    else period_from or period_to
                )
                blanket_content = period.get("AngivelseBlanketIndholdStruktur") or {}
                for declaration in _coerce_to_list(value=blanket_content.get("AngivelseOplysningSamling")):
                    if not isinstance(declaration, dict):
                        continue
                    values = _extract_fields(blanket=declaration, field_ids=set(report_fields.values()))
                    if values:
                        rows.append(
                            {
                                "cpr": cpr,
                                "IndkomstPersonGruppeDispositionDato": disposition_date,
                                "AngivelsePeriode": declaration_period,
                                **{
                                    field_name: values.get(field_id)
                                    for field_name, field_id in report_fields.items()
                                },
                            }
                        )

    return rows


def write_social_frikort_report_excel_bytes(
    rows: list[dict[str, Any]], report_fields: dict[str, str]
) -> bytes:
    """Create an Excel report containing only the income information worksheet."""
    report_df = pd.DataFrame(rows, columns=social_frikort_report_columns(report_fields))
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        report_df.to_excel(writer, index=False, sheet_name="Indkomstoplysninger")
        worksheet = writer.sheets["Indkomstoplysninger"]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions

        for column_index, column_name in enumerate(report_df.columns, start=1):
            values = report_df[column_name].fillna("").astype(str)
            content_width = values.map(len).max() if not values.empty else 0
            width = max(int(content_width), len(column_name)) + 2
            worksheet.column_dimensions[get_column_letter(column_index)].width = min(width, 60)

    return output.getvalue()
