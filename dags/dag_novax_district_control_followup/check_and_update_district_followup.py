import logging
import datetime

from dag_novax_district_control.clients.novax_client import get_upcoming_due_dates  # , update_novax_userdata

logger = logging.getLogger(__name__)


def check_and_update_district_followup() -> None:
    """
    Retrieves and updates user, address and district information
    for any patients with an upcoming due date based on their addresses.
    """
    # Set dates to retrieve upcoming due dates (next 7 days)
    today = datetime.datetime.now().date()
    from_date = today
    to_date = today + datetime.timedelta(days=7)

    logger.info(f"Retrieving pregnancy journals with due dates from {from_date} to {to_date}")
    res = get_upcoming_due_dates(from_date=from_date, to_date=to_date)

    logger.info(f"Found {len(res)} entries with upcoming due dates")
    for entry in res:
        logger.info(f"Processing entry with name ID: {entry.navnid}")
        logger.info(entry.to_dict())

    return


# Task wrapper for Airflow
def check_and_update_district_followup_task(**kwargs):
    check_and_update_district_followup()
