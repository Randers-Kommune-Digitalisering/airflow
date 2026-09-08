import base64
import logging
import requests

from datetime import timedelta
from sqlalchemy.engine import Row

from dag_xflow_nexus_hjaelpemidler.nexus import NexusClient, ASSISTIVE_DEVICES_ASSIGNMENT_NAME

logger = logging.getLogger(__name__)


# Helper functions
def decode_base64_pdf(base64_string: str) -> bytes:
    """ Decode a raw base64 string into file bytes, rejecting anything that is not a PDF. """
    try:
        file_bytes = base64.b64decode("".join(base64_string.split()), validate=True)
    except Exception as e:
        raise ValueError("Invalid base64 content") from e

    if not file_bytes.startswith(b"%PDF-"):
        raise ValueError("Unknown file type: only PDF is supported")
    return file_bytes


def get_xflow_attachment(session: requests.Session, url: str) -> bytes:
    """ Download an attachment from xFlow. """
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.content


# Handlers for each xFlow table
def personligt_hjaelpemiddel(nexus_client: NexusClient, xflow_session: requests.Session, row: Row) -> None:
    """ Send a 'personligt_hjaelpemiddel' application with its attachments to Nexus. Raise to mark the row as failed. """
    patient_data = nexus_client.get_patient_data(cpr=row.cpr)

    created_docs: list[dict] = []
    created_form = None
    created_assignment = None

    try:
        created_doc = nexus_client.add_assistive_device_document(
            patient_data=patient_data,
            date=row.form_date,
            name=row.form_doc_name,
            file_name=f"{row.form_doc_name}.pdf",
            file_bytes=decode_base64_pdf(row.form_pdf_base64),
            mime_type="application/pdf"
        )
        created_docs.append(created_doc)

        for attachment in row.attachments or []:
            created_attachment = nexus_client.add_assistive_device_document(
                patient_data=patient_data,
                date=row.form_date,
                name=row.attachment_doc_name,
                file_name=attachment["title"],
                file_bytes=get_xflow_attachment(session=xflow_session, url=attachment["url"]),
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
