
import re
import requests
import logging
import time
from typing import Any
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

        extra = cpr_hook.extra_dejson or {}
        connect_timeout = extra.get("connect_timeout_seconds")
        read_timeout = extra.get("read_timeout_seconds")

        self.timeout: tuple[float, float] = (
            float(connect_timeout) if connect_timeout is not None else 5.0,
            float(read_timeout) if read_timeout is not None else 30.0,
        )

    def _get_with_retry(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        retries: int = 3,
        delay_seconds: int = 5,
        timeout: float | tuple[float, float] | None = None,
    ) -> requests.Response:
        attempt = 0
        while True:
            try:
                response = self.session.get(url, params=params, timeout=timeout or self.timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else None
                if status_code is not None and status_code not in [200, 400] and attempt < retries:
                    attempt += 1
                    time.sleep(delay_seconds)
                    continue
                raise
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                if attempt < retries:
                    attempt += 1
                    time.sleep(delay_seconds)
                    continue
                raise

    def lookup_address(self, cpr_number: str) -> dict | None:
        endpoint = f'/PersonBaseDataExtendedService/lookup/address/{cpr_number}'
        try:
            res = self._get_with_retry(f"{self.base_url}{endpoint}")
        except requests.RequestException as e:
            logger.warning(f"Network error while looking up address for CPR {cpr_number[:6]}-XXXX: {e}")
            return None
        if res.status_code != 200:
            logger.warning(f"Failed to lookup address for CPR {cpr_number[:6]}-XXXX: HTTP {res.status_code}")
            return None
        try:
            data = res.json()
        except ValueError as e:
            logger.warning(f"Invalid JSON response while looking up address for CPR {cpr_number[:6]}-XXXX: {e}")
            return None
        std = data.get('aktuelAdresse', {}).get('standardadresse')

        if isinstance(std, str) and std:
            # Remove leading zeros from house numbers
            data['aktuelAdresse']['standardadresse'] = re.sub(
                r'(\s)0+(\d+[A-Za-z]?)\b',
                r'\1\2',
                std
            )

        return data
