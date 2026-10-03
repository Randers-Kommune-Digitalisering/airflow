import base64
import requests

from dag_xflow_nexus_hjaelpemidler.config import FORM_CONFIG_BY_TABLE, build_fields_by_table
from dag_xflow_nexus_hjaelpemidler.models import (
    HjaelpemiddelData,
    HjaelpemiddelFormData,
    NexusAssignment,
    NexusCase,
    NexusDocument
)


# helper functions for handling PDF and xFlow attachments
def _decode_base64_pdf(base64_string: str) -> bytes:
    """Decode a raw base64 string into file bytes, rejecting anything that is not a PDF."""
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


def _build_documents(
    model_object: HjaelpemiddelData,
    session: requests.Session,
) -> list[NexusDocument]:
    """Build a list of NexusDocument objects from the given HjaelpemiddelData model object."""
    documents = [
        NexusDocument(
            name=model_object.form_doc_name,
            file_name=f"{model_object.form_doc_name}.pdf",
            mime_type="application/pdf",
            file_bytes=_decode_base64_pdf(model_object.form_pdf_base64),
            date=model_object.form_date,
        ),
        NexusDocument(
            name=f"{model_object.form_doc_name} Kvittering",
            file_name="receipt.pdf",
            mime_type="application/pdf",
            file_bytes=_decode_base64_pdf(model_object.receipt_pdf_base64),
            date=model_object.form_date,
        )
    ]

    for attachment in model_object.attachments or []:
        documents.append(
            NexusDocument(
                name=model_object.attachment_doc_name,
                file_name=attachment["title"],
                mime_type=attachment["mimeType"],
                file_bytes=_get_xflow_attachment(session=session, url=attachment["url"]),
                date=model_object.form_date,
            )
        )

    return documents


def get_nexus_case(
    session: requests.Session,
    table_name: str,
    model_object: HjaelpemiddelData,
) -> NexusCase:
    """Build a NexusCase object from the given HjaelpemiddelData model object."""
    try:
        config = FORM_CONFIG_BY_TABLE[table_name]
    except KeyError as error:
        raise ValueError(
            f"No Nexus form configuration registered for table '{table_name}'"
        ) from error

    normal_deadline = config["generate_due_date"](model_object=model_object)
    extended_deadline = config["generate_extended_due_date"](model_object=model_object)

    form_data = HjaelpemiddelFormData(model_object, normal_deadline, extended_deadline)
    fields = build_fields_by_table[table_name](model_object=form_data)
    documents = _build_documents(model_object=model_object, session=session)
    assignment_title = config["generate_assignment_title"](model_object=model_object)
    assignment = NexusAssignment(
        name=config["assignment_name"],
        organization=config["assignment_organization"],
        title=assignment_title,
        start_date=model_object.form_date.strftime("%Y-%m-%d"),
        due_date=normal_deadline.strftime("%Y-%m-%d"),
    )
    return NexusCase(
        cpr=model_object.cpr,
        table_name=table_name,
        renewal_or_new_text=model_object.renewal_or_new_text,
        error_notification_recipient=config["error_notification_recipient"],
        dashboard_name=config["dashboard_name"],
        program_name=config["program_name"],
        form_name=config["form_name"],
        document_widget_name=config["document_widget_name"],
        form_widget_name=config["form_widget_name"],
        pathway_name=config["pathway_name"],
        fields=fields,
        documents=documents,
        assignment=assignment,
    )
