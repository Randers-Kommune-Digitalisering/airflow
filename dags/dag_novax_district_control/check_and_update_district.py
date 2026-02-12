
import logging
from dag_novax_district_control.clients.novax_client import get_pregnancy_journals, update_novax_userdatas_batch
from dag_novax_district_control.novax_utils import Address, parse_address, parse_journal_data, to_int_or_none
from dag_novax_district_control.run_utils import determine_date_range
from dag_novax_district_control.clients.district_map_client import DataforsyningClient, DistrictMapDBClient
from dag_novax_district_control.clients.cpr_client import CPRClient
from airflow.models import Variable
from typing import Any

logger = logging.getLogger(__name__)


DRY_RUN = Variable.get("NOVAX_DRY_RUN", default_var="True").lower() == "true"  # Set to True to log intended updates without making changes, False to perform updates
DEFAULT_MUNICIPALITY_CODE = int(Variable.get("NOVAX_DEFAULT_MUNICIPALITY_CODE", default_var="730"))  # Default municipality code to use if not found in Dataforsyning - corresponds to Randers municipality


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
    raw_entries = get_pregnancy_journals(from_date=start_date, to_date=end_date)
    if not raw_entries:
        logger.info(f"No data found for the period from {start_date} to {end_date}. Exiting.")
        return

    # Filter out duplicates based on (navnid, timestamp) - keep latest entry per navnid
    latest_entries_by_navnid: dict[str, Any] = {}
    for entry in raw_entries:
        existing = latest_entries_by_navnid.get(entry.navnid)
        if existing is None or entry.timestamp > existing.timestamp:
            latest_entries_by_navnid[entry.navnid] = entry
    entries = list(latest_entries_by_navnid.values())

    # Process each UserData entry
    skipped_navnids: set[str] = set()
    points_by_navnid: dict[str, tuple[float, float]] = {}
    address_by_navnid: dict[str, Address] = {}

    for entry in entries:
        # Parse journal note to dict
        try:
            entry.parsed_journal = parse_journal_data(entry.journal, journal_date=entry.timestamp)
        except Exception:
            logger.exception(f"Error parsing journal data for navnid {entry.navnid}")
            skipped_navnids.add(entry.navnid)
            continue
        entry.journal = None  # Clear raw journal text to save space/logging

        # Look up current address from CPR
        cpr_info = cpr_client.lookup_address(entry.cpr)

        parsed_new_address = None
        if cpr_info and cpr_info.get('aktuelAdresse'):
            # Try CPR address first
            std_addr = cpr_info['aktuelAdresse'].get('standardadresse', '')
            postnummer = cpr_info['aktuelAdresse'].get('postnummer', '')
            cpr_address_str = ", ".join([str(p).strip() for p in (std_addr, postnummer) if str(p).strip()])
            try:
                parsed_new_address = parse_address(cpr_address_str)
                adressebeskyttelse = cpr_info.get('adressebeskyttelse', {})
                parsed_new_address.is_protected = adressebeskyttelse.get('beskyttet', False) is True  # Check if address is protected
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
                entry.current_address.postal_code != parsed_new_address.postal_code or
                entry.current_address.is_protected != parsed_new_address.is_protected
            ):
                entry.new_address = parsed_new_address

        # Look up address details for district info + vejkode in Dataforsyning
        address_to_lookup = entry.new_address if entry.new_address is not None else entry.current_address

        if address_to_lookup:
            address_info = dataforsyning_client.lookup_address(address_to_lookup.address_dataforsyningen_lookup)
            if address_info and address_info.get('adgangsadresse', {}).get('x') is not None and address_info.get('adgangsadresse', {}).get('y') is not None:
                # Store coordinates for district lookup later
                x = address_info['adgangsadresse']['x']
                y = address_info['adgangsadresse']['y']
                points_by_navnid[entry.navnid] = (x, y)

                # Store address details for potential update and logging
                address_to_lookup.street_code = address_info['adgangsadresse'].get('vejkode')
                address_by_navnid[entry.navnid] = address_to_lookup

                # Also check if municipality code has changed based on Dataforsyning info
                looked_up_code = address_info['adgangsadresse'].get('kommunekode')
                looked_up_code_int = to_int_or_none(looked_up_code)
                if looked_up_code_int is not None and entry.current_municipality_code != looked_up_code_int:
                    entry.new_municipality_code = looked_up_code_int
            else:
                logger.warning(f"Address not found in Dataforsyning: {address_to_lookup.address_dataforsyningen_lookup}")
                entry.new_address = None  # Clear new address to avoid updating to an unknown address - will still update other fields if applicable
        else:
            logger.warning(f"No valid address to look up district for navnid: {entry.navnid}")

        # Check new phone number from journal data
        new_tlf_nr = entry.parsed_journal.get('phone', None)
        if new_tlf_nr and new_tlf_nr != entry.current_tlf_nr:
            if not (new_tlf_nr.isdigit() and len(new_tlf_nr) == 8):
                logger.warning(f"Unusual phone number '{new_tlf_nr}' for navnid {entry.navnid}, skipping phone update.")
            else:
                entry.new_tlf_nr = new_tlf_nr

        # Get due date from journal data - will override existing due date if present
        journal_due_date = entry.parsed_journal.get('due_date', None) or entry.parsed_journal.get('calculated_due_date', None)
        if journal_due_date and journal_due_date != entry.current_due_date:
            entry.new_due_date = journal_due_date

    # Get districts for all address coordinates in batch
    points_for_district_lookup = [(navnid, x, y) for navnid, (x, y) in points_by_navnid.items()]
    districts_by_navnid = district_db_client.get_district_names_by_key(points_for_district_lookup)

    # Build update request for each entry
    update_requests_by_navnid: dict[str, dict] = {}
    for entry in entries:
        if entry.navnid in skipped_navnids:
            continue  # Skip entries with unparsable journal data
        detected_changes: list[str] = []

        # Check if district has changed
        if entry.navnid in points_by_navnid:
            new_district = districts_by_navnid.get(entry.navnid)
            if new_district and new_district != entry.current_district:
                entry.new_district = new_district
            elif new_district is None:
                address = address_by_navnid.get(entry.navnid, "<unknown>")  # For logging purposes
                logger.warning(f"District not found for navnid: {entry.navnid} at address: {address}")

        # Check if new municipality has been set - if not, set to default if current municipality is different from default
        if entry.new_municipality_code is None and entry.current_municipality_code != DEFAULT_MUNICIPALITY_CODE:
            entry.new_municipality_code = DEFAULT_MUNICIPALITY_CODE

        # Log detected changes
        if entry.new_address is not None:
            detected_changes.append("address" + (" (protected)" if entry.new_address.is_protected else ""))
        if entry.new_district is not None and entry.new_district != entry.current_district:
            detected_changes.append("district")
        if entry.new_tlf_nr is not None and entry.new_tlf_nr != entry.current_tlf_nr:
            detected_changes.append("phone")
        if entry.new_due_date is not None:
            detected_changes.append("due date")
        if entry.new_municipality_code is not None:
            detected_changes.append("municipality code")
        if detected_changes == []:
            detected_changes.append("none")
        if detected_changes:
            logger.info(f"Detected changes for navnid {entry.navnid}: {', '.join(detected_changes)}")

        # Prepare update payload
        # None values in the payload will be ignored by the update function
        # Besides new values, new pregnancies will always get allocated to 'Gravid til fordeling' (id: 'FIKTIV')
        update_payload = {
            "navnid": entry.navnid,
            "due_date": entry.new_due_date,
            "new_district": entry.new_district,
            "new_address": entry.new_address,
            "new_tlf_nr": entry.new_tlf_nr,
            "new_municipality_code": entry.new_municipality_code
        }

        # Add to update requests
        update_requests_by_navnid[entry.navnid] = update_payload

    # Return if DRY_RUN is enabled - log what would be updated without making changes
    if DRY_RUN:
        attempted_updates = len(update_requests_by_navnid)
        logger.info(f"DRY_RUN enabled: would update {attempted_updates} entr{'y' if attempted_updates == 1 else 'ies'} (skipped {len(skipped_navnids)} due to unparsable journal data) for {start_date} to {end_date}.")
        return

    # Perform single Novax batch update
    if update_requests_by_navnid:
        update_results = update_novax_userdatas_batch(list(update_requests_by_navnid.values()))
    else:
        update_results = {}

    # Log update results per entry
    entry_status = []
    for entry in entries:
        if entry.navnid in skipped_navnids:
            logger.warning(f"Skipped update for navnid {entry.navnid} (unparsable journal data).")
            continue

        update_success = bool(update_results.get(entry.navnid))
        entry_status.append(update_success)
        if update_success:
            logger.info(f"Successfully updated Novax userdata for navnid {entry.navnid}")
        else:
            logger.error(f"Failed to update Novax userdata for navnid {entry.navnid}")

    # Log final status
    if skipped_navnids:
        logger.info(f"Skipped {len(skipped_navnids)} entr{'y' if len(skipped_navnids) == 1 else 'ies'} due to unparsable journal data.")

    # If nothing was attempted, treat the run as successful.
    success = all(entry_status) if entry_status else True
    if success:
        logger.info("Successfully completed check_and_update_district")
    else:
        logger.error("Errors occurred during check_and_update_district")
        raise Exception("check_and_update_district failed: some updates failed")
    return
