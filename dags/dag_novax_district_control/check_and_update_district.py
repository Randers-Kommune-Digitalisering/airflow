
import logging
from dag_novax_district_control.clients.novax_client import get_pregnancy_journals, update_novax_userdata
from dag_novax_district_control.novax_utils import parse_address, parse_journal_data
from dag_novax_district_control.run_utils import determine_date_range
from dag_novax_district_control.clients.district_map_client import DataforsyningClient, DistrictMapClient
from dag_novax_district_control.clients.cpr_client import CPRClient

logger = logging.getLogger(__name__)


def check_and_update_district() -> None:
    """
    Retrieves and updates user, address and district information
    for any new patients based on their addresses.
    """
    # Initialize clients
    dataforsyning_client = DataforsyningClient()
    map_client = DistrictMapClient()
    cpr_client = CPRClient()

    # Determine date range for processing
    # Start date is inclusive, end date is exclusive
    start_date, end_date = determine_date_range()
    if start_date is None or end_date is None:
        logger.info("No date range determined for processing. Exiting.")
        return

    # Get data from Novax and parse to UserData (+Address) objects
    logger.info(f"Starting check_and_update_district from {start_date} to {end_date}")
    res = get_pregnancy_journals(from_date=start_date, to_date=end_date)
    if not res:
        logger.info(f"No data found for the period from {start_date} to {end_date}. Exiting.")
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
        update_success = True
        update_novax_userdata(
            navnid=entry.navnid,
            due_date=entry.parsed_journal.get('calculated_due_date', None),  # TODO: Use actual due date when available
            new_district=entry.new_district,
            new_address=entry.new_address.full_address if entry.new_address else None,
            new_tlf_nr=entry.new_tlf_nr
        )
        entry_status.append(update_success)
        if update_success:
            logger.info(f"Updated Novax userdata for navnid {entry.navnid}: {entry.to_dict()}")
        else:
            logger.error(f"Failed to update Novax userdata for navnid {entry.navnid}: {entry.to_dict()}")

    # Log final status
    success = all(entry_status)
    if success and update_success:
        logger.info(f"Successfully completed check_and_update_district")
    else:
        logger.error(f"Errors occurred during check_and_update_district")
        raise Exception(f"check_and_update_district failed")
    return
