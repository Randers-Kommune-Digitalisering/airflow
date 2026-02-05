
import logging
from dag_novax_district_control.clients.novax_client import get_pregnancy_journals, update_novax_userdatas_batch
from dag_novax_district_control.novax_utils import parse_address, parse_journal_data
from dag_novax_district_control.run_utils import determine_date_range
from dag_novax_district_control.clients.district_map_client import DataforsyningClient, DistrictMapDBClient
from dag_novax_district_control.clients.cpr_client import CPRClient

logger = logging.getLogger(__name__)


def check_and_update_district() -> None:
    """
    Retrieves and updates user, address and district information
    for any new patients based on their addresses.
    """
    # Initialize clients
    dataforsyning_client = DataforsyningClient()
    district_db_client = DistrictMapDBClient()
    cpr_client = CPRClient()

    # Determine date range for processing
    # Start date is inclusive, end date is exclusive
    date_range = determine_date_range()
    if date_range is None:
        logger.info("No new date range to process. Exiting.")
        return
    start_date, end_date = date_range

    # Get data from Novax and parse to UserData (+Address) objects
    logger.info(f"Starting check_and_update_district from {start_date} to {end_date}")
    res = get_pregnancy_journals(from_date=start_date, to_date=end_date)
    if not res:
        logger.info(f"No data found for the period from {start_date} to {end_date}. Exiting.")
        return

    # Filter out dublicates based on (navnid, timestamp) - keep latest entry per navnid
    unique_entries: dict[str, any] = {}
    for entry in res:
        existing = unique_entries.get(entry.navnid)
        if existing is None or entry.timestamp > existing.timestamp:
            unique_entries[entry.navnid] = entry
    res = list(unique_entries.values())

    # Process each UserData entry
    skipped_navnids: set[str] = set()
    points_by_navnid: dict[str, tuple[float, float]] = {}
    address_by_navnid: dict[str, str] = {}
    for entry in res:
        if entry.journal is None:
            logger.warning(f"No journal data for navnid: {entry.navnid}, skipping entry.")
            skipped_navnids.add(entry.navnid)
            continue

        # Parse journal note to dict
        try:
            entry.parsed_journal = parse_journal_data(entry.journal, journal_date=entry.timestamp)
        except Exception as e:
            logger.error(f"Error parsing journal data for navnid {entry.navnid}: {e}")
            skipped_navnids.add(entry.navnid)
            continue
        entry.journal = None  # Clear raw journal text to save space/logging

        # Look up current address from CPR
        cpr_info = cpr_client.lookup_address(entry.cpr)

        parsed_new_address = None
        if cpr_info and cpr_info.get('aktuelAdresse'):
            # Try CPR address first
            cpr_address_str = f"{cpr_info['aktuelAdresse'].get('standardadresse', '')}, {cpr_info['aktuelAdresse'].get('postnummer', '')}"
            try:
                parsed_new_address = parse_address(cpr_address_str)
            except Exception as e:
                logger.warning(f"Error parsing CPR address for navnid {entry.navnid}: {e}")

            # Fallback to journal address if CPR address is present but unparsable
            if parsed_new_address is None:
                logger.warning(f"CPR address present but could not be parsed for navnid: {entry.navnid}, using journal data if available.")
        else:
            logger.warning(f"No address found in CPR for navnid: {entry.navnid}, using journal data if available.")

        # Fallback to journal address if no CPR address is found
        if parsed_new_address is None and entry.parsed_journal.get('address', None):
            try:
                parsed_new_address = parse_address(entry.parsed_journal.get('address'))
            except Exception as e:
                logger.warning(f"Error parsing journal address for navnid {entry.navnid}: {e}")

        # Check if address has changed
        if parsed_new_address:
            if (
                not entry.current_address or
                entry.current_address.street != parsed_new_address.street or
                entry.current_address.number != parsed_new_address.number or
                entry.current_address.postal_code != parsed_new_address.postal_code
            ):
                entry.new_address = parsed_new_address

        # Look up address details for district info in Dataforsyning
        address_to_lookup = entry.new_address if entry.new_address is not None else entry.current_address

        if address_to_lookup:
            address_info = dataforsyning_client.lookup_address(address_to_lookup.full_address)
            if address_info and address_info.get('adgangsadresse', {}).get('x') and address_info.get('adgangsadresse', {}).get('y'):
                x = address_info['adgangsadresse']['x']
                y = address_info['adgangsadresse']['y']
                points_by_navnid[entry.navnid] = (x, y)
                address_by_navnid[entry.navnid] = address_to_lookup.full_address
            else:
                logger.warning(f"Address not found in Dataforsyning: {address_to_lookup.full_address}")
        else:
            logger.warning(f"No valid address to look up district for navnid: {entry.navnid}")

        # Check new phone number from journal data
        new_tlf_nr = entry.parsed_journal.get('phone', None)
        if new_tlf_nr and new_tlf_nr != entry.current_tlf_nr:
            if len(new_tlf_nr) != 8 and all(char.isdigit() for char in new_tlf_nr):  # Basic sanity check for phone number length and format
                logger.warning(f"Unusual phone number '{new_tlf_nr}' for navnid {entry.navnid}, skipping phone update.")
            else:
                entry.new_tlf_nr = new_tlf_nr

        # Get due date from journal data
        entry.new_due_date = entry.parsed_journal.get('due_date', entry.parsed_journal.get('calculated_due_date', None))

    # Get districts for all address coordinates in batch
    keyed_points = [(navnid, x, y) for navnid, (x, y) in points_by_navnid.items()]
    districts_by_navnid = district_db_client.get_district_names_by_key(keyed_points)

    # Build update request for each entry
    update_requests_by_navnid: dict[str, dict] = {}
    for entry in res:
        if entry.navnid in skipped_navnids:
            continue
        detected_changes: list[str] = []

        # Check if district has changed
        if entry.navnid in points_by_navnid:
            new_district = districts_by_navnid.get(entry.navnid)
            if new_district and new_district != entry.current_district:
                entry.new_district = new_district
            elif new_district is None:
                address = address_by_navnid.get(entry.navnid, "<unknown>")
                logger.warning(f"District not found for navnid: {entry.navnid} at address: {address}")

        # Log detected changes
        if entry.new_address is not None:
            detected_changes.append("address")
        if entry.new_district is not None and entry.new_district != entry.current_district:
            detected_changes.append("district")
        if entry.new_tlf_nr is not None and entry.new_tlf_nr != entry.current_tlf_nr:
            detected_changes.append("phone")
        if entry.new_due_date is not None:
            detected_changes.append("due date")
        if detected_changes:
            logger.info(f"Detected changes for navnid {entry.navnid}: {', '.join(detected_changes)}")

        # Prepare update payload
        update_payload = {
            "navnid": entry.navnid,
            "due_date": entry.new_due_date,
            "new_district": entry.new_district,
            "new_address": entry.new_address.full_address if entry.new_address else None,
            "new_tlf_nr": entry.new_tlf_nr
        }

        # Skip update if there are no changes (i.e., all update fields are None)
        if all(value is None for key, value in update_payload.items() if key != "navnid"):
            continue

        # Add to update requests
        update_requests_by_navnid[entry.navnid] = update_payload

    # Perform single Novax batch update
    return  # TODO: Remove return statement; we don't want to update DB while testing
    if update_requests_by_navnid:
        update_results = update_novax_userdatas_batch(list(update_requests_by_navnid.values()))
    else:
        update_results = {}

    # Log update results per entry
    updated_navnids = set(update_requests_by_navnid.keys())
    entry_status = []
    for entry in res:
        if entry.navnid in skipped_navnids:
            logger.warning(f"Missing journal data for navnid {entry.navnid} (no update attempted).")
            continue
        if entry.navnid not in updated_navnids:
            logger.info(f"No changes detected for navnid {entry.navnid} (no update attempted).")
            continue

        update_success = bool(update_results.get(entry.navnid))
        entry_status.append(update_success)
        if update_success:
            logger.info(f"Successfully updated Novax userdata for navnid {entry.navnid}")
        else:
            logger.error(f"Failed to update Novax userdata for navnid {entry.navnid}")

    # Log final status
    if skipped_navnids:
        logger.info(f"Skipped {len(skipped_navnids)} entr{'y' if len(skipped_navnids) == 1 else 'ies'} due to missing journal data.")

    # If nothing was attempted, treat the run as successful.
    success = all(entry_status) if entry_status else True
    if success:
        logger.info("Successfully completed check_and_update_district")
    else:
        logger.error("Errors occurred during check_and_update_district")
        raise Exception("check_and_update_district failed: some updates failed")
    return
