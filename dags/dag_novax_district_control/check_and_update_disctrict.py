
import logging
from airflow import DAG
# from airflow.operators.python import PythonOperator

from dag_novax_district_control.clients.novax_client import get_test_data, get_test_data_move, test_connection
from dag_novax_district_control.novax_utils import parse_address
from dag_novax_district_control.clients.district_map_client import DataforsyningClient, DistrictMapClient
# from dag_novax_district_control.clients.cpr_client import CPRClient

dataforsyning_client = DataforsyningClient()
map_client = DistrictMapClient()
# cpr_client = CPRClient()

logger = logging.getLogger(__name__)


def check_and_update_district(cpr=None) -> None:
    logger.info(f"Starting check_and_update_district for CPR: {cpr}")
    # return True

    test_connection()
    # Test database connection
    if not test_connection():
        logger.error("Database connection test failed.")
        raise Exception("Database connection test failed.")
    logger.info("Database connection test succeeded.")


    # res = get_test_data(cpr=cpr)
    # if not res:
    #     logger.info(f"No data found for CPR: {cpr}")

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
    