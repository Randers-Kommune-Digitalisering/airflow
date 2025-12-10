
import logging
import datetime

from dag_novax_district_control.clients.novax_client import get_pregnancy_journals  # , get_test_data, get_test_data_move, test_connection
# from dag_novax_district_control.novax_utils import parse_address
from dag_novax_district_control.clients.district_map_client import DataforsyningClient, DistrictMapClient
# from dag_novax_district_control.clients.cpr_client import CPRClient

dataforsyning_client = DataforsyningClient()
map_client = DistrictMapClient()
# cpr_client = CPRClient()

logger = logging.getLogger(__name__)


def check_and_update_district(from_date=None, to_date=None) -> None:
    """
    Retrieves and updates user, address and district information
    for any new patients based on their addresses.

    :param from_date: The start date to filter records from.
    :param to_date: The end date to filter records to.
    """
    # Get last run dates and status
    # last_run_info = get_last_run_info()
    last_run_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    today = datetime.datetime.now().date()

    logger.info(f"Starting check_and_update_district from {last_run_date} to {today}")

    # Get data from Novax and parse to UserData (+Address) objects
    res = get_pregnancy_journals(from_date=last_run_date, to_date=today)
    if not res:
        logger.info(f"No data found for the period from {last_run_date} to {today}. Exiting.")
        return

    logger.info(f"Retrieved {len(res)} records from Novax for the period from {last_run_date} to {today}")
    for entry in res:
        logger.info(f"UserData: {entry.to_dict()}")

    #     logger.error(f"Task failed for CPR: {cpr}")
    #     raise Exception(f"Check and update district failed for CPR: {cpr}")

    # for entry in res:
    #     # if entry.get('CPR'):
    #     #     entry['cpr_info'] = cpr_client.lookup_address([entry['CPR']])

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
