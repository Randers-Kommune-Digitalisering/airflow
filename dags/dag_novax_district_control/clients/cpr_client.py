
import re
import requests
import logging
from airflow.hooks.base import BaseHook
from utils.token_provider import BearerAuth

logger = logging.getLogger(__name__)


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

    def lookup_address(self, cpr_number: str) -> dict | None:
        endpoint = f'/PersonBaseDataExtendedService/lookup/address/{cpr_number}'
        try:
            res = self.session.get(f"{self.base_url}{endpoint}")
        except requests.RequestException as e:
            logger.warning(f"Network error while looking up address for CPR {cpr_number[:6]}-XXXX: {e}")
            return None

        if res.status_code != 200:
            logger.warning(f"Failed to lookup address for CPR {cpr_number[:6]}-XXXX: HTTP {res.status_code}")
            return None

        data = res.json()
        std = data.get('aktuelAdresse', {}).get('standardadresse')

        if isinstance(std, str) and std:
            # Remove leading zeros from house numbers
            data['aktuelAdresse']['standardadresse'] = re.sub(
                r'(\s)0+(\d+[A-Za-z]?)\b',
                r'\1\2',
                std
            )

        return data
