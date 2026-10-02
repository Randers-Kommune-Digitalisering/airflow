from __future__ import annotations

from typing import Any, Protocol
from enum import Enum
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo
from dataclasses import dataclass

# Database
# NOTE:  Tables are created and owned in a different repo
# Any changes to the database schema should be made in the external-api repo
# https://github.com/Randers-Kommune-Digitalisering/external-api
SCHEMA = "xflow_nexus"


class HjaelpemiddelStatus(str, Enum):
    """Enumeration of possible statuses for Hjaelpemiddel processing. Also defined in other repo"""
    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class HjaelpemiddelData(Protocol):
    """Protocol for Hjaelpemiddel data received from xFlow. Reflects the structure of the data as defined in the external API."""
    cpr: str
    form_date: date
    form_doc_name: str
    form_pdf_base64: str
    receipt_pdf_base64: str
    attachment_doc_name: str
    attachments: list[dict] | None
    device_name: str
    can_collect_data: bool
    for_another: bool
    reason_text: str
    renewal_or_new_text: str
    on_behalf_of_relation: str | None
    on_behalf_of_name: str | None
    on_behalf_of_phone: str | None
    on_behalf_of_text: str | None


class StoetteTilBilData(HjaelpemiddelData, Protocol):
    type: str | None


# Nexus
# Classes for representing Nexus data
@dataclass(frozen=True)
class NexusCase:
    cpr: str
    table_name: str
    dashboard_name: str
    program_name: str
    pathway_name: str
    document_widget_name: str
    form_widget_name: str
    form_name: str
    fields: list[Field]
    documents: list[NexusDocument]
    assignment: NexusAssignment

    def __repr__(self) -> str:
        return (
            f"NexusCase(table_name={self.table_name}, dashboard_name={self.dashboard_name}, "
            f"document_widget_name={self.document_widget_name}, form_widget_name={self.form_widget_name}, "
            f"form_name={self.form_name}, pathway_name={self.pathway_name}, assignment={self.assignment}, "
            f"amount_nexus_fields={len(self.fields)}, amount_nexus_documents={len(self.documents)})"
        )


@dataclass(frozen=True)
class NexusDocument:
    name: str
    file_name: str
    mime_type: str
    file_bytes: bytes
    date: date

    def __post_init__(self) -> None:
        if not isinstance(self.date, date):
            raise TypeError("Document date must be a date")
        normalized_file_name = "_".join(self.file_name.strip().rstrip(".").split())
        if not normalized_file_name:
            raise ValueError("File name cannot be empty")
        object.__setattr__(self, "file_name", normalized_file_name)

    def __repr__(self) -> str:
        return (
            f"NexusDocument(name={self.name}, file_name={self.file_name}, mime_type={self.mime_type}, "
            f"date={self.date})"
        )


@dataclass(frozen=True)
class NexusAssignment:
    name: str
    title: str
    organization: str
    start_date: str
    due_date: str


class FieldType(Enum):
    TEXT = "text"
    TEXT_AREA = "textArea"
    DATE = "date"
    DROPDOWN = "dropDown"
    CHECK_GROUP = "checkGroup"


@dataclass
class Field:
    label: str
    field_type: FieldType
    value: Any
    require_match: bool = True

    def apply(self, item: dict) -> None:
        item_type = item.get("type")
        if item_type != self.field_type.value:
            raise TypeError(
                f"Field '{self.label}' expects Nexus item type '{self.field_type.value}', "
                f"but received '{item_type}'"
            )

        if self.field_type is FieldType.DROPDOWN:
            item["value"] = self._find_option(item, self.value)
        elif self.field_type is FieldType.CHECK_GROUP:
            values = [self.value] if isinstance(self.value, str) else self.value
            if self.require_match:
                item["value"] = [self._find_option(item, value) for value in values]
            else:
                item["value"] = [
                    option
                    for value in values
                    if (option := self._find_option(item, value, raise_if_missing=False)) is not None
                ]
        elif self.field_type is FieldType.DATE:
            local_midnight = datetime.combine(
                self.value,
                time.min,
                tzinfo=ZoneInfo("Europe/Copenhagen"),
            )
            item["value"] = (
                local_midnight.astimezone(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
        else:
            item["value"] = self.value

    @staticmethod
    def _find_option(item: dict, selected_name: str, raise_if_missing: bool = True) -> dict | None:
        option = next(
            (
                option
                for option in item.get("possibleValues", [])
                if option.get("name", "").casefold() == selected_name.casefold()
            ),
            None,
        )
        if option is None and raise_if_missing:
            available = [option.get("name") for option in item.get("possibleValues", [])]
            raise ValueError(
                f"Could not find option '{selected_name}' for field '{item.get('label')}'. "
                f"Available options: {available}"
            )
        return option
