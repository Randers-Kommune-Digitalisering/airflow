
import logging
import datetime

from dag_novax_district_control.clients.novax_client import get_pregnancy_journals, update_novax_userdata
from dag_novax_district_control.novax_utils import parse_address, parse_journal_data
from dag_novax_district_control.clients.db_client import (
    get_last_run_info,
    create_novax_run_record,
    update_novax_run_record,
    create_novax_record,
    get_processed_nameids_in_period,
)
from dag_novax_district_control.clients.district_map_client import DataforsyningClient, DistrictMapClient
from dag_novax_district_control.clients.cpr_client import CPRClient

dataforsyning_client = DataforsyningClient()
map_client = DistrictMapClient()
cpr_client = CPRClient()

logger = logging.getLogger(__name__)


def _as_date(value):
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return None


async def check_and_update_district(from_date=None, to_date=None) -> None:
    """
    Retrieves and updates user, address and district information
    for any new patients based on their addresses.

    :param from_date: The start date to filter records from.
    :param to_date: The end date to filter records to.
    """
    # Get last run dates and status if from_date and to_date not set
    today = datetime.datetime.now().date()
    last_run_date = None
    last_run_completed = None
    novax_run_id = None
    if not from_date:
        last_run_info = get_last_run_info()
        last_run_date = last_run_info['last_run_end_date']
        last_run_completed = last_run_info['completed']
        novax_run_id = last_run_info.get('id', None)

        # DB stores timestamps; ensure we're comparing dates
        last_run_date = _as_date(last_run_date)

        # If last run was not completed, rerun from last start date
        if last_run_completed is False:
            logger.info("Last run was not completed successfully. Attempting to rerun from last start date.")
            last_run_date = _as_date(last_run_info['last_run_start_date'])

        # If no last run date, default to yesterday
        if not last_run_date:
            last_run_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()

        # Check if last run date is today
        if last_run_date and last_run_date >= today:
            logger.info("Last run date is today or in the future. No action needed.")
            return

    start_date = from_date or last_run_date
    end_date = to_date or today

    # Create DB history record for this run
    # Skip creation only when resuming a previously failed run (last_run_completed is False).
    if last_run_completed is not False:
        novax_run_id = create_novax_run_record(start_date=start_date, end_date=end_date)
        if novax_run_id:
            logger.info(f"Created Novax run record with ID: {novax_run_id}")
        else:
            logger.error("Failed to create Novax run record. Exiting.")
            return
    logger.info(f"Starting check_and_update_district from {start_date} to {end_date}")

    # Get data from Novax and parse to UserData (+Address) objects
    res = get_pregnancy_journals(from_date=start_date, to_date=end_date)
    if not res:
        logger.info(f"No data found for the period from {start_date} to {end_date}. Exiting.")
        return

    # Filter out patients already updated in this period based on NovaxRecord entries.
    # Only applied when we're re-running a period that already has completed runs.
    already_processed_nameids = get_processed_nameids_in_period(start_date, end_date)
    if already_processed_nameids:
        before = len(res)
        res = [entry for entry in res if str(entry.navnid) not in already_processed_nameids]
        skipped = before - len(res)
        logger.info(
            f"Re-run detected: skipping {skipped} already-processed patients in {start_date}..{end_date}."
        )

    logger.info(f"Retrieved {len(res)} records from Novax for the period from {start_date} to {end_date}")
    if not res:
        if novax_run_id is not None:
            update_novax_run_record(run_id=novax_run_id, completed=True)
        logger.info("No remaining records after filtering; exiting.")
        return

    # Process each UserData entry
    entry_status = []
    for entry in res:
        # Parse journal note to dict
        entry.parsed_journal = parse_journal_data(entry.journal, journal_date=entry.timestamp)
        entry.journal = None  # Clear raw journal text to save space/logging

        # Look up current address from CPR
        cpr_info = cpr_client.lookup_address(entry.cpr)
        if cpr_info and cpr_info.get('aktuelAdresse'):
            entry.new_address = parse_address(f"{cpr_info['aktuelAdresse']['standardadresse']}, {cpr_info['aktuelAdresse']['postnummer']}")
        else:
            logger.warning(f"No address found for CPR: {entry.cpr}, using journal data if available.")
            # Parse address from journal data if no CPR address is found
            entry.new_address = parse_address(entry.parsed_journal.get('address', None))

        # Look up address details for district info in Dataforsyning
        address_info = dataforsyning_client.lookup_address(entry.new_address.full_address)
        if address_info and address_info.get('adgangsadresse', {}).get('x') and address_info.get('adgangsadresse', {}).get('y'):
            entry.new_district = map_client.get_district(address_info['adgangsadresse']['x'], address_info['adgangsadresse']['y'])
        else:
            logger.warning(f"Address not found in Dataforsyning: {entry.new_address.full_address}")

        # Check new phone number from journal data
        entry.new_tlf_nr = entry.parsed_journal.get('phone', None)

        # Update Novax with new address, phone number, district if changed + due date
        update_success = update_novax_userdata(
            navnid=entry.navnid,
            due_date=entry.parsed_journal.get('calculated_due_date', None),  # TODO: Use actual due date when available
            new_district=entry.new_district,
            new_address=entry.new_address.full_address if entry.new_address else None,
            new_tlf_nr=entry.new_tlf_nr
        )

        # Update database with results for entry
        entry_success = create_novax_record(nameid=entry.navnid, success=True, runid=novax_run_id)
        entry_status.append(update_success and entry_success is not None)

    # Update run completion status
    success = all(entry_status)
    update_success = update_novax_run_record(run_id=novax_run_id, completed=success)
    if success and update_success:
        logger.info(f"Successfully completed check_and_update_district for run ID: {novax_run_id}")
    else:
        logger.error(f"Errors occurred during check_and_update_district for run ID: {novax_run_id}")
        raise Exception(f"check_and_update_district failed for run ID: {novax_run_id}")
    return


# Synchronous wrapper for Airflow
def check_and_update_district_task(**kwargs):
    """
    Synchronous wrapper to run the async check_and_update_district function for Airflow compatibility.
    """
    import asyncio
    asyncio.run(check_and_update_district())
