import base64
import html
import logging
import re
import tempfile

from datetime import datetime
from pathlib import Path

from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.utils.email import send_email
from requests.exceptions import RequestException

from dag_sd_delta.delta_client import DeltaClient, MissingImportSummaryError
from utils.custom_log import get_log_collector, get_styled_log_html
from dag_sd_delta.sd import employment_exists_on_date

logger = logging.getLogger(__name__)
_IMPORT_STATUS_PLACEHOLDER = "<!-- delta-import-status -->"


def handle_deleted_employments(
    delta_client: DeltaClient,
    deleted_employments: list[dict] | None,
) -> dict[str, str]:
    """
    Handle employments marked as deleted in SD by checking point-in-time snapshots and marking engagements as inactive in Delta.

    Args:
        delta_client (DeltaClient): An instance of the DeltaClient.
        deleted_employments (list[dict] | None): A list of dictionaries containing deleted employment details.

    Returns:
        dict[str, str]: A dictionary containing the log HTML.
    """
    log_collector = get_log_collector()
    root_logger = logging.getLogger()
    root_logger.addHandler(log_collector)

    try:
        if not deleted_employments:
            logger.info("No deleted employments to handle.")
        else:
            for deleted in deleted_employments:
                inst_id = deleted["institution_id"]
                cpr = deleted["cpr"]
                employment_id = deleted["employment_id"]
                activation_date = datetime.fromisoformat(deleted["date"]).date()

                exists_in_sd = employment_exists_on_date(
                    inst_id=inst_id,
                    cpr=cpr,
                    employment_id=employment_id,
                    effective_date=activation_date
                )
                if exists_in_sd:
                    logger.warning(f"Employment with Institution={inst_id} EmploymentIdentifier={employment_id} is marked as deleted but still exists in snapshot on activation date {activation_date}.")
                    continue
                else:
                    engagement_key = f"{inst_id}.{employment_id}.{activation_date.strftime('%Y')}.{cpr[:6]}"
                    uuid = delta_client.get_active_engagement_id(
                        engagement_key=engagement_key,
                        valid_date=activation_date
                    )
                    if uuid:
                        if delta_client.deactivate_engagement(uuid=uuid, from_date=activation_date):
                            logger.info(f"Successfully marked engagement with key {engagement_key} as inactive.")
                        else:
                            logger.error(f"Failed to mark engagement with key {engagement_key} as inactive.")
                    else:
                        logger.info(f"No engagement found in Delta for deleted employment with key {engagement_key}.")

    finally:
        root_logger.removeHandler(log_collector)

    html_prefix = "".join([
        "<h3>Handle deleted employments log summary</h3>",
        "<pre style='white-space: pre-wrap; font-family: monospace;'>",
    ])
    styled_log_lines = get_styled_log_html(log_collector=log_collector)
    return {"log_html": html_prefix + styled_log_lines + "</pre>"}


def upload_excel_file_to_delta(delta_client: DeltaClient, file_path: str | None) -> dict[str | None, str]:
    """
    Upload the generated excel file to Delta and return upload status HTML and process id.

    Args:
        delta_client (DeltaClient): An instance of the DeltaClient.
        file_path (str | None): The path to the excel file to be uploaded. If None, no upload will be performed.

    Returns:
        dict[str | None, str]: A dictionary containing the process instance ID and upload status HTML.   
    """

    if file_path is None:
        return {
            "process_instance_id": None,
            "upload_html": "<p>No file uploaded to Delta.</p>",
        }
    process_instance_id = delta_client.upload_sd_excel_file(file_path=file_path)
    logger.info("Delta upload successful. Process instance id: %s", process_instance_id)

    process_instance_url = (
        f"https://fb-prod.deltahr.kmd.dk/main/#/process/instance/{process_instance_id}"
    )

    return {
        "process_instance_id": process_instance_id,
        "upload_html": "".join([
            "<h3>Import SD Excel file status</h3>",
            _IMPORT_STATUS_PLACEHOLDER,
            f"<p><a href='{process_instance_url}'>{process_instance_url}</a></p>",
        ]),
    }


def _has_import_warnings(summary: str) -> bool:
    counts = []
    for label in ("duplicated rows", "invalid rows", "interrupted rows"):
        match = re.search(rf"\b{label}:\s*(\d+)\b", summary, flags=re.IGNORECASE)
        if match is None:
            raise ValueError(f"Missing {label} count in Delta import summary")
        counts.append(int(match.group(1)))
    return any(count > 0 for count in counts)


def is_sd_excel_import_finished(process_instance_id: str | None) -> PokeReturnValue:
    """Check one Delta import status; the sensor reschedules while it is pending."""
    if process_instance_id is None:
        return PokeReturnValue(is_done=True, xcom_value={"status": "NO_UPLOAD"})

    try:
        status = DeltaClient(BaseHook.get_connection("delta_prod")).status_sd_excel_file(
            process_instance_id
        )
        if status["status"] == "FINISHED":
            summary = status["summary"]
            return PokeReturnValue(
                is_done=True,
                xcom_value={
                    "status": "FINISHED",
                    "summary": summary,
                    "has_warnings": _has_import_warnings(summary),
                },
            )
    except MissingImportSummaryError as exc:
        logger.info("Delta import summary not available yet for process %s: %s", process_instance_id, exc)
        return PokeReturnValue(is_done=False)
    except (ValueError, RequestException) as exc:
        logger.warning("Delta import status check failed for process %s: %s", process_instance_id, exc)
        return PokeReturnValue(is_done=True, xcom_value={"status": "ERROR", "message": str(exc)})

    return PokeReturnValue(is_done=False)


def prepare_delta_report(
    process_instance_id: str | None,
    subject: str,
    html_content: str,
    attachment_path: str | None,
) -> dict[str, str | None]:
    """Carry the existing email content and attachment across Airflow tasks."""
    attachment = Path(attachment_path) if attachment_path else None
    return {
        "process_instance_id": process_instance_id,
        "subject": subject,
        "html_content": html_content,
        "attachment_name": attachment.name if attachment else None,
        "attachment_data": base64.b64encode(attachment.read_bytes()).decode("ascii") if attachment else None,
    }


def send_delta_report(source_task_id: str, **context) -> None:
    """Send the prepared report after the status sensor finishes or times out."""
    ti = context["ti"]
    report = ti.xcom_pull(task_ids=source_task_id)
    if report is None:
        logger.warning("No Delta report available because %s did not complete", source_task_id)
        return

    result = ti.xcom_pull(task_ids="wait_for_sd_excel_import")
    if result is None and report["process_instance_id"] is not None:
        message = "Delta import status timed out after 30 minutes."
    elif result and result["status"] == "ERROR":
        message = f"Delta import status check failed: {result['message']}"
    else:
        message = None

    html_content = report["html_content"]
    if report["process_instance_id"] is not None:
        if _IMPORT_STATUS_PLACEHOLDER not in html_content:
            raise ValueError("Missing Delta import status section in report")
        if message:
            logger.warning("%s", message)
            status_html = f"<p><strong>{html.escape(message)}</strong></p>"
        elif result and result["status"] == "FINISHED":
            summary = html.escape(result["summary"])
            if result["has_warnings"]:
                summary = f"<strong>Warning: {summary}</strong>"
            status_html = f"<pre style='white-space: pre-wrap; font-family: monospace;'>{summary}</pre>"
        else:
            raise ValueError("Missing Delta import polling result")
        html_content = html_content.replace(_IMPORT_STATUS_PLACEHOLDER, status_html, 1)

    email_kwargs = {
        "to": ["delta@randers.dk"],
        "subject": report["subject"],
        "html_content": html_content,
    }
    if report["attachment_name"] is None:
        send_email(**email_kwargs)
        return

    with tempfile.TemporaryDirectory() as directory:
        attachment = Path(directory) / Path(report["attachment_name"]).name
        attachment.write_bytes(base64.b64decode(report["attachment_data"], validate=True))
        send_email(**email_kwargs, files=[str(attachment)])
