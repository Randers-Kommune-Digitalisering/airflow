import requests

from pathlib import Path
from tempfile import TemporaryDirectory
from airflow.utils.email import send_email_smtp

from dag_xflow_nexus_hjaelpemidler.constants import COMPLETED_ACTION_NAME
from dag_xflow_nexus_hjaelpemidler.models import NexusCase
from dag_xflow_nexus_hjaelpemidler.nexus import NexusClient


# Helper functions
def _error_email_sender(nexus_case: NexusCase, msg: str) -> None:
    """ Send an email when a patient is not found in Nexus or is inactive. """
    with TemporaryDirectory(prefix="xflow-not-found-") as temp_dir:
        attachment_paths = []

        for document in nexus_case.documents:
            attachment_path = Path(temp_dir) / Path(document.file_name).name
            attachment_path.write_bytes(document.file_bytes)
            attachment_paths.append(str(attachment_path))

        send_email_smtp(
            from_email="Digitalisering@randers.dk",
            to=[nexus_case.error_notification_recipient],
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
            nexus_case=nexus_case,
            msg="Borger ikke fundet i Nexus"
        )
        return

    is_active = nexus_client.is_active(patient=patient_data, set_if_not=True)
    if not is_active:
        _error_email_sender(
            nexus_case=nexus_case,
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

            api_assignment["title"] = nexus_case.assignment.title

            created_assignment = nexus_client.create_assignment(assignment=api_assignment)
    except Exception:
        nexus_client.rollback_objects(
            created_docs=created_docs,
            created_form=created_form,
            created_assignment=created_assignment
        )
        raise
