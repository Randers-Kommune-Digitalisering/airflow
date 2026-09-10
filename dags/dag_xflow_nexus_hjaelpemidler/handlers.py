import base64
import requests

from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.utils.email import send_email_smtp
from sqlalchemy.engine import Row

from dag_xflow_nexus_hjaelpemidler.nexus import ASSISTIVE_DEVICES_ASSIGNMENT_NAME, TOP_PROGRAM_NAME, NexusClient


# Helper functions
def _decode_base64_pdf(base64_string: str) -> bytes:
    """ Decode a raw base64 string into file bytes, rejecting anything that is not a PDF. """
    try:
        file_bytes = base64.b64decode("".join(base64_string.split()), validate=True)
    except Exception as e:
        raise ValueError("Invalid base64 content") from e

    if not file_bytes.startswith(b"%PDF-"):
        raise ValueError("Unknown file type: only PDF is supported")
    return file_bytes


def _get_xflow_attachment(session: requests.Session, url: str) -> bytes:
    """ Download an attachment from xFlow. """
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.content


def _error_email_sender(row: Row, xflow_session: requests.Session, table_name: str, msg: str) -> None:
    """ Send an email when a patient is not found in Nexus or is inactive. """
    with TemporaryDirectory(prefix="xflow-not-found-") as temp_dir:
        attachment_paths = []

        form_name = f"{table_name}.pdf"
        form_path = Path(temp_dir) / form_name
        form_path.write_bytes(_decode_base64_pdf(row.form_pdf_base64))
        attachment_paths.append(str(form_path))

        for attachment in row.attachments or []:
            attachment_name = Path(attachment["title"]).name or "bilag"
            attachment_path = Path(temp_dir) / attachment_name
            attachment_path.write_bytes(
                _get_xflow_attachment(session=xflow_session, url=attachment["url"])
            )
            attachment_paths.append(str(attachment_path))

        send_email_smtp(
            from_email="Digitalisering@randers.dk",
            to=["personligehjaelpemidler@randers.dk"],
            subject=f"Fejl i Nexus: {msg}",
            html_content=(
                msg
            ),
            files=attachment_paths,
        )


# Handlers for each xFlow table
def personligt_hjaelpemiddel(
    nexus_client: NexusClient,
    xflow_session: requests.Session,
    row: Row,
    table_name: str,
) -> None:
    """ Send a 'personligt_hjaelpemiddel' application with its attachments to Nexus. Raise to mark the row as failed. """
    patient_data = nexus_client.get_patient_data(cpr=row.cpr)
    if patient_data is None:
        _error_email_sender(
            row=row,
            xflow_session=xflow_session,
            table_name=table_name,
            msg="Borger ikke fundet i Nexus"
        )
        return

    is_active = nexus_client.is_active(patient_data=patient_data, set_if_not=True)
    if not is_active:
        _error_email_sender(
            row=row,
            xflow_session=xflow_session,
            table_name=table_name,
            msg="Borger er sat som død"
        )
        return

    created_docs: list[dict] = []
    created_form = None
    created_assignment = None

    try:
        if not nexus_client.has_program(patient_data=patient_data, program_name=TOP_PROGRAM_NAME, add_if_missing=True):
            raise ValueError(f"Failed to ensure patient is enrolled in program '{TOP_PROGRAM_NAME}'")
        else:
            created_doc = nexus_client.add_assistive_device_document(
                patient_data=patient_data,
                date=row.form_date,
                name=row.form_doc_name,
                file_name=f"{row.form_doc_name}.pdf",
                file_bytes=_decode_base64_pdf(row.form_pdf_base64),
                mime_type="application/pdf"
            )
            created_docs.append(created_doc)

            for attachment in row.attachments or []:
                created_attachment = nexus_client.add_assistive_device_document(
                    patient_data=patient_data,
                    date=row.form_date,
                    name=row.attachment_doc_name,
                    file_name=attachment["title"],
                    file_bytes=_get_xflow_attachment(session=xflow_session, url=attachment["url"]),
                    mime_type=attachment["mimeType"]
                )
                created_docs.append(created_attachment)

            created_form = nexus_client.create_assistive_device_communication_form(
                patient_data=patient_data,
                application_date=row.form_date,
                application_reason=row.reason_text,
                communication_source=row.on_behalf_of_relation if row.for_another else "Borger",
                device=row.device_name,
                optional_contact_info=f"{row.on_behalf_of_text.lower()}\n{row.on_behalf_of_name} - tlf: {row.on_behalf_of_phone}" if row.for_another and row.on_behalf_of_relation else None,
                patient_understands=True,
                can_information_be_obtained=row.can_collect_data
            )

            assignment = nexus_client.get_auto_assignment(form=created_form, assignment_name=ASSISTIVE_DEVICES_ASSIGNMENT_NAME)

            if assignment.get("startDate") != row.form_date.strftime("%Y-%m-%d"):
                assignment["startDate"] = row.form_date.strftime("%Y-%m-%d")
                assignment["dueDate"] = (row.form_date + timedelta(weeks=30)).strftime("%Y-%m-%d")

            assignment["title"] = f"{row.device_name.strip() or 'Personlig hjælpemiddel'} {row.renewal_or_new_text}"

            created_assignment = nexus_client.create_assignment(assignment=assignment)
    except Exception:
        nexus_client.rollback_objects(
            created_docs=created_docs,
            created_form=created_form,
            created_assignment=created_assignment
        )
        raise


# Mapping of xFlow table names to their respective handler functions
HANDLERS = {
    "personligt_hjaelpemiddel": personligt_hjaelpemiddel,
}
