import logging
import datetime
from dag_novax_district_control.clients.novax_client import get_upcoming_due_dates, update_novax_userdata
from dag_novax_district_control.novax_utils import parse_address
from dag_novax_district_control.clients.district_map_client import DataforsyningClient, DistrictMapClient
from dag_novax_district_control.clients.cpr_client import CPRClient

logger = logging.getLogger(__name__)


def check_and_update_district_followup() -> None:
    """
    Retrieves and updates user, address and district information
    for any patients with an upcoming due date based on their addresses.
    """
    # Initialize clients
    dataforsyning_client = DataforsyningClient()
    map_client = DistrictMapClient()
    cpr_client = CPRClient()

    # Get data from Novax and parse to UserData (+Address) objects
    today = datetime.datetime.now().date()
    logger.info(f"Retrieving pregnancy journals with due dates from {today} onwards")
    res = get_upcoming_due_dates(from_date=today)
    logger.info(f"Found {len(res)} entries with upcoming due dates")

    # Process each UserData entry
    entry_status = []
    for entry in res:
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
            logger.warning(f"No address found for navnid: {entry.navnid}, using journal data if available.")
            # Parse address from journal data if no CPR address is found
            entry.new_address = parse_address(entry.parsed_journal.get('address', None))

        # Look up address details for district info in Dataforsyning
        address_to_lookup = entry.new_address if entry.new_address is not None else entry.current_address
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

        # Update Novax with new address, phone number, district if changed + due date
        update_success = update_novax_userdata(
            navnid=entry.navnid,
            new_district=entry.new_district,
            new_address=entry.new_address.full_address if entry.new_address else None
        )
        entry_status.append(update_success)

        # Log update result
        if update_success:
            updated_properties = []
            if entry.new_address:
                updated_properties.append(f"address: {entry.new_address.full_address}")
            if entry.new_district:
                updated_properties.append(f"district: {entry.new_district}")
            if updated_properties:
                logger.info(f"Successfully updated Novax userdata for navnid {entry.navnid} with changes: {', '.join(updated_properties)}")
            else:
                logger.info(f"No updates needed for navnid {entry.navnid}")

    # Log final status
    success = all(entry_status)
    if success and update_success:
        logger.info("Successfully completed check_and_update_district")
    else:
        logger.error("Errors occurred during check_and_update_district")
        raise Exception("check_and_update_district failed")
    return
