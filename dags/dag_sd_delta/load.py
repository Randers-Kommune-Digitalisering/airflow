import logging

from datetime import datetime

from dag_sd_delta.delta_client import DeltaClient
from utils.custom_log import get_log_collector, get_styled_log_html
from dag_sd_delta.sd import employment_exists_on_date

logger = logging.getLogger(__name__)


def handle_deleted_employments(
    delta_client: DeltaClient,
    deleted_employments: list[dict] | None,
) -> dict[str, str]:
    """Handle employments marked as deleted in SD by checking point-in-time snapshots and marking engagements as inactive in Delta."""
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
                    return
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
    styled_log_lines = get_styled_log_html(log_collector)
    return {"log_html": html_prefix + styled_log_lines + "</pre>"}


def upload_excel_file_to_delta(delta_client: DeltaClient, file_path: str | None) -> dict[str | None, str]:
    """Upload the generated excel file to Delta and return upload status HTML and process id."""

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
            f"<p><a href='{process_instance_url}'>{process_instance_url}</a></p>",
        ]),
    }
