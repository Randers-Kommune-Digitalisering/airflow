
import logging
import datetime

from dag_novax_district_control.clients.novax_client import get_pregnancy_journals  # , get_test_data, get_test_data_move, test_connection
from dag_novax_district_control.novax_utils import parse_journal_data
from dag_novax_district_control.clients.district_map_client import DataforsyningClient, DistrictMapClient
from dag_novax_district_control.clients.cpr_client import CPRClient

dataforsyning_client = DataforsyningClient()
map_client = DistrictMapClient()
cpr_client = CPRClient()

logger = logging.getLogger(__name__)


def check_and_update_district(from_date=None, to_date=None) -> None:
    """
    Retrieves and updates user, address and district information
    for any new patients based on their addresses.

    :param from_date: The start date to filter records from.
    :param to_date: The end date to filter records to.
    """
    # Get last run dates and status if from_date and to_date not set
    if (not from_date and not to_date):
        # last_run_info = get_last_run_info()
        last_run_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
        today = datetime.datetime.now().date()
        logger.info(f"Starting check_and_update_district from {last_run_date} to {today}")

        # Check if last run date is today
        if last_run_date >= today:
            logger.info("Last run date is today or in the future. No action needed.")
            return

    # Get data from Novax and parse to UserData (+Address) objects
    res = get_pregnancy_journals(from_date=from_date or last_run_date, to_date=to_date or today)
    if not res:
        logger.info(f"No data found for the period from {from_date or last_run_date} to {to_date or today}. Exiting.")
        return

    logger.info(f"Retrieved {len(res)} records from Novax for the period from {from_date or last_run_date} to {to_date or today}")
    for entry in res:
        # Parse journal note to dict
        entry.parsed_journal = parse_journal_data(entry.journal, journal_date=entry.timestamp)
        entry.journal = None  # Clear raw journal text to save space/logging
        logger.info(f"UserData: {entry.to_dict()}")

    # TODO: Parse address from journal data if present
    # TODO: Check journal data for address changes, phone changes and update accordingly
    # TODO: If no address is noted, use CPR to get current address
    # TODO: Check address in Dataforsyning and get district
    # TODO: Update Novax with new address, phone number, district if changed

    # Iterate through each UserData entry and update district info
    # for entry in res:
    #     if entry.get('CPR'):
    #         entry['cpr_info'] = cpr_client.lookup_address([entry['CPR']])

    #     if entry.get('parsed_address'):
    #         # Look up address details in Dataforsyning
    #         address_info = dataforsyning_client.lookup_address(entry['parsed_address']['full_address'])
    #         entry['dataforsyning_info'] = address_info

    #         # if address_info and address_info.get('adgangsadresse', {}).get('x') and address_info.get('adgangsadresse', {}).get('y'):
    #         #     entry['new_district'] = map_client.get_district(address_info['adgangsadresse']['x'], address_info['adgangsadresse']['y'])

    #     else:
    #         entry['new_district'] = None
    #         logger.warning(f"No parsed address for entry: {entry}")

    # logger.info(f"Completed check_and_update_district for CPR: {cpr}")
    # logger.info(f"Results: {res}")
    # return res.to_dict() if res else {}
