import requests
import logging

from airflow.hooks.base import BaseHook

from utils.token_provider import BearerAuth

logger = logging.getLogger(__name__)


class CPRClient:
    def __init__(self):
        cpr_hook = BaseHook.get_connection('cpr_replica_prod')
        cpr_session = requests.Session()
        cpr_session.auth = BearerAuth(
            token_url=cpr_hook.extra_dejson.get('token_url'),
            client_id=cpr_hook.login,
            client_secret=cpr_hook.password
        )

        self.session = cpr_session
        self.base_url = cpr_hook.host

    def get_address_uuid_and_protected_status(self, cpr_number: str) -> dict:
        """
        Get the address UUID and protected status for a given CPR number.

        :param cpr_number: The CPR number to look up (format: 6 digits, a dash, and 4 digits, e.g. "123456-7890")
        :return: A dictionary containing 'address_uuid' and 'protected_status'
        """
        # Fails if CPR replica is down/broken or if the CPR number is invalid.
        # We want it to fail hard in those cases, so we can fix the underlying issue.
        endpoint = f'/PersonBaseDataExtendedService/lookup/address/{cpr_number}'
        res = self.session.get(f"{self.base_url}{endpoint}", timeout=10)
        res.raise_for_status()
        data = res.json()

        address_uuid = data['aktuelAdresse'].get('adresseUUID', '')
        protected = data.get('adressebeskyttelse', {}).get('beskyttet')

        if not isinstance(protected, bool) or not isinstance(address_uuid, str) or not len(address_uuid) == 36:
            raise ValueError(f"Unexpected response format for CPR {cpr_number[:6]}-XXXX: {data}")

        return {
            'address_uuid': address_uuid,
            'is_protected_address': protected
        }
