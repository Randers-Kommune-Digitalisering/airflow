
import requests
from airflow.hooks.base import BaseHook

from utils.token_provider import BearerAuth


class CPRClient():
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

    def lookup_address(self, cpr_number: str) -> dict:
        endpoint = f'/PersonBaseDataExtendedService/lookup/address/{cpr_number}'
        res = self.session.get(f"{self.base_url}{endpoint}")
        res.raise_for_status()
        return res.json()
