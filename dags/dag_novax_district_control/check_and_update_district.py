
import logging
from dag_novax_district_control.clients.novax_client import get_pregnancy_journals, update_novax_userdatas_batch
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
        logger.info("No date range determined for processing. Exiting as failed run.")
        raise Exception("check_and_update_district failed: no date range")
        return

    # Get data from Novax and parse to UserData (+Address) objects
    logger.info(f"Starting check_and_update_district from {start_date} to {end_date}")
    res = get_pregnancy_journals(from_date=start_date, to_date=end_date)
    if not res:
        logger.info(f"No data found for the period from {start_date} to {end_date}. Exiting.")
        return

    # Process each UserData entry
    update_requests_by_navnid: dict = {}
    skipped_navnids: set = set()
    for entry in res:
        if entry.journal is None:
            logger.warning(f"No journal data for navnid: {entry.navnid}, skipping entry.")
            skipped_navnids.add(entry.navnid)
            continue

        # Parse journal note to dict
        entry.parsed_journal = parse_journal_data(entry.journal, journal_date=entry.timestamp)
        entry.journal = None  # Clear raw journal text to save space/logging

        # Look up current address from CPR
        cpr_info = cpr_client.lookup_address(entry.cpr)
        if cpr_info and cpr_info.get('aktuelAdresse'):

            # Check if address has changed
            parsed_new_address = parse_address(f"{cpr_info['aktuelAdresse'].get('standardadresse', '')}, {cpr_info['aktuelAdresse'].get('postnummer', '')}")
            if parsed_new_address:
                if (
                    not entry.current_address or
                    entry.current_address.street != parsed_new_address.street or
                    entry.current_address.number != parsed_new_address.number or
                    entry.current_address.postal_code != parsed_new_address.postal_code
                ):
                    logger.info(f"Address change detected for navnid: {entry.navnid}. Updating address.")
                    entry.new_address = parsed_new_address

        else:
            # Parse address from journal data if no CPR address is found
            logger.warning(f"No address found for navnid: {entry.navnid}, using journal data if available.")
            parsed_new_address = parse_address(entry.parsed_journal.get('address', None))
            if parsed_new_address:
                if (
                    not entry.current_address or
                    entry.current_address.street != parsed_new_address.street or
                    entry.current_address.number != parsed_new_address.number or
                    entry.current_address.postal_code != parsed_new_address.postal_code
                ):
                    logger.info(f"Address change detected for navnid: {entry.navnid}. Updating address.")
                    entry.new_address = parsed_new_address

        # Look up address details for district info in Dataforsyning
        address_to_lookup = entry.new_address if entry.new_address is not None else entry.current_address

        if address_to_lookup:
            address_info = dataforsyning_client.lookup_address(address_to_lookup.full_address)
            if address_info and address_info.get('adgangsadresse', {}).get('x') and address_info.get('adgangsadresse', {}).get('y'):
                new_district = map_client.get_district(address_info['adgangsadresse']['x'], address_info['adgangsadresse']['y'])

                # Check if district has changed
                if new_district and new_district != entry.current_district:
                    logger.info(f"District change detected for navnid: {entry.navnid}. Updating district from {entry.current_district} to {new_district}.")
                    entry.new_district = new_district
                elif new_district is None:
                    logger.warning(f"District not found for navnid: {entry.navnid} at address: {address_to_lookup.full_address}")
            else:
                logger.warning(f"Address not found in Dataforsyning: {address_to_lookup.full_address}")
        else:
            logger.warning(f"No valid address to look up district for navnid: {entry.navnid}")

        # Check new phone number from journal data
        new_tlf_nr = entry.parsed_journal.get('phone', None)
        if new_tlf_nr and new_tlf_nr != entry.current_tlf_nr:
            logger.info(f"Phone number change detected for navnid: {entry.navnid}. Updating phone number from {entry.current_tlf_nr} to {new_tlf_nr}.")
            entry.new_tlf_nr = new_tlf_nr

        # Get due date from journal data
        due_date = entry.parsed_journal.get('due_date', entry.parsed_journal.get('calculated_due_date', None))

        # Prepare update payload
        update_payload = {
            "navnid": entry.navnid,
            "due_date": due_date,
            "new_district": entry.new_district,
            "new_address": entry.new_address.full_address if entry.new_address else None,
            "new_tlf_nr": entry.new_tlf_nr
        }

        # Merge objects with identical NAVNID if any (prefer non-None values)
        existing = update_requests_by_navnid.get(entry.navnid)
        if existing is None:
            update_requests_by_navnid[entry.navnid] = update_payload
        else:
            for key, value in update_payload.items():
                if key == "navnid":
                    continue
                if value is not None:
                    existing[key] = value

    # Perform single Novax batch update
    if update_requests_by_navnid:
        update_results = update_novax_userdatas_batch(list(update_requests_by_navnid.values()))
    else:
        update_results = {}

    # Log update results per entry
    entry_status = []
    for entry in res:
        if entry.navnid in skipped_navnids:
            logger.warning(f"Skipped navnid {entry.navnid}: missing journal data (no update attempted).")
            continue

        update_success = bool(update_results.get(entry.navnid))
        entry_status.append(update_success)

        if update_success:
            updated_properties = []
            if entry.new_address:
                updated_properties.append(f"address: {entry.new_address.full_address}")
            if entry.new_district:
                updated_properties.append(f"district: {entry.new_district}")
            if entry.new_tlf_nr:
                updated_properties.append(f"phone number: {entry.new_tlf_nr}")
            if due_date := entry.parsed_journal.get('due_date', entry.parsed_journal.get('calculated_due_date', None)):
                updated_properties.append(f"due date: {due_date}")
            if updated_properties:
                logger.info(f"Successfully updated Novax userdata for navnid {entry.navnid} with changes: {', '.join(updated_properties)}")
            else:
                logger.info(f"No updates needed for navnid {entry.navnid}")
        else:
            logger.error(f"Failed to update Novax userdata for navnid {entry.navnid}")

    # Log final status
    if skipped_navnids:
        logger.info(f"Skipped {len(skipped_navnids)} entr{'y' if len(skipped_navnids) == 1 else 'ies'} due to missing journal data.")

    success = all(entry_status)
    if success:
        logger.info("Successfully completed check_and_update_district")
    else:
        logger.error("Errors occurred during check_and_update_district")
        raise Exception("check_and_update_district failed: some updates failed")
    return
