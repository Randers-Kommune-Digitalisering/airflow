import base64
import requests

from typing import Any
from pathlib import Path
from tempfile import TemporaryDirectory
from airflow.utils.email import send_email_smtp

from dag_xflow_nexus_hjaelpemidler.constants import COMPLETED_ACTION_NAME
from dag_xflow_nexus_hjaelpemidler.models import NexusCase
from dag_xflow_nexus_hjaelpemidler.nexus import NexusClient


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


def _error_email_sender(object: Any, xflow_session: requests.Session, table_name: str, msg: str) -> None:
    """ Send an email when a patient is not found in Nexus or is inactive. """
    with TemporaryDirectory(prefix="xflow-not-found-") as temp_dir:
        attachment_paths = []

        form_name = f"{table_name}.pdf"
        form_path = Path(temp_dir) / form_name
        form_path.write_bytes(_decode_base64_pdf(object.form_pdf_base64))
        attachment_paths.append(str(form_path))

        for attachment in object.attachments or []:
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


def process_nexus_case(nexus_client: NexusClient, xflow_session: requests.Session, nexus_case: NexusCase) -> None:
    """Process a Nexus case """
    patient_data = nexus_client.get_patient(search_text=nexus_case.cpr)
    if patient_data is None:
        _error_email_sender(
            object=nexus_case,
            xflow_session=xflow_session,
            table_name=nexus_case.table_name,
            msg="Borger ikke fundet i Nexus"
        )
        return

    is_active = nexus_client.is_active(patient=patient_data, set_if_not=True)
    if not is_active:
        _error_email_sender(
            object=nexus_case,
            xflow_session=xflow_session,
            table_name=nexus_case.table_name,
            msg="Borger er sat som død"
        )
        return

    created_docs: list[dict] = []
    created_form = None
    created_assignment = None

    try:
        if not nexus_client.has_pathway(patient=patient_data, program_name=nexus_case.program_name, pathway_name=nexus_case.pathway_name, add_if_missing=True):
            raise ValueError(f"Failed to ensure patient is enrolled in pathway '{nexus_case.pathway_name}'")
        else:
            dashboard = nexus_client.get_dashboard(patient=patient_data, name=nexus_case.dashboard_name)
            doc_widget = nexus_client.get_widget(dashboard=dashboard, name=nexus_case.document_widget_name)

            for doc in nexus_case.documents:
                api_doc = nexus_client.get_document(widget=doc_widget, pathway_name=nexus_case.pathway_name)
                created_api_doc = nexus_client.create_document(document=api_doc, nexus_document=doc)
                created_docs.append(created_api_doc)

            form_widget = nexus_client.get_widget(dashboard=dashboard, name=nexus_case.form_widget_name)
            filled_form = nexus_client.get_filled_form(widget=form_widget, nexus_case=nexus_case)
            created_form = nexus_client.create_form(form=filled_form, action_name=COMPLETED_ACTION_NAME)

            api_assignment = nexus_client.get_auto_assignment(form=created_form, assignment_title=nexus_case.assignment.name, organization_name=nexus_case.assignment.organization)

            if api_assignment.get("startDate") != nexus_case.assignment.start_date:
                api_assignment["startDate"] = nexus_case.assignment.start_date
                api_assignment["dueDate"] = nexus_case.assignment.due_date

            created_assignment = nexus_client.create_assignment(assignment=api_assignment)
    except Exception:
        nexus_client.rollback_objects(
            created_docs=created_docs,
            created_form=created_form,
            created_assignment=created_assignment
        )
        raise
