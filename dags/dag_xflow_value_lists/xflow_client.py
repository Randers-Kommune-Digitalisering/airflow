"""Validate X-Flow destinations and create or replace their value lists."""

import ipaddress
import logging
import re
from urllib.parse import urlsplit

import requests
from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)
HOST_PATTERN = re.compile(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}")
TIMEOUT = (10, 30)  # (connect timeout, read timeout) in seconds


class XFlowClient:
    def __init__(self) -> None:
        """Configure the client from the X-Flow Airflow connection."""
        connection = BaseHook.get_connection("xflow_value_lists")
        if not connection.password:
            raise ValueError("xflow Connection requires a password")

        base_url = connection.host
        _https_url(base_url)
        parsed_base = urlsplit(base_url)
        if not re.fullmatch(r"api\.[a-z0-9-]+\.xflow\.dk", parsed_base.hostname or "") or parsed_base.path not in ("", "/"):
            raise ValueError("xflow Connection host must be an X-Flow API base URL")

        self._base_url = base_url.rstrip("/")
        self._token = connection.password

    def replace_value_lists(self, prepared: list[tuple[str, list[str]]], created_by: str) -> None:
        """
        Replace each X-Flow list with its complete set of prepared values.

        :param prepared: Pairs of X-Flow list IDs and ordered values to send.
        :param created_by: Creator recorded on each value-list item.
        :return: None.
        :raises ValueError: If X-Flow redirects a replacement request.
        """
        with requests.Session() as session:
            session.headers.update({"publicApiToken": self._token})
            for list_id, values in prepared:
                items = [
                    {"value": value, "key": str(index), "oprettetAf": created_by, "indeks": str(index)}
                    for index, value in enumerate(values, start=1)
                ]
                response = session.put(
                    f"{self._base_url}/ValueList/{list_id}/Values",
                    json={"ValueListData": items},
                    timeout=TIMEOUT,
                    allow_redirects=False,
                )
                try:
                    if 300 <= response.status_code < 400:
                        raise ValueError("X-Flow redirects are not allowed")

                    response.raise_for_status()
                finally:
                    response.close()

                logger.info(f"Updated X-Flow value list:{list_id} with {len(values)} items")

    def create_value_lists(self, prepared: list[dict[str, str]]) -> None:
        """
        Create X-Flow lists unless a search finds an exact name match.

        :param prepared: List definitions with name and description strings.
        :return: None.
        :raises ValueError: If X-Flow redirects or returns an invalid search response.
        """
        with requests.Session() as session:
            session.headers.update({"publicApiToken": self._token})
            for entry in prepared:
                name = entry["name"]
                response = session.get(  # Check if a value list with the given name already exists
                    f"{self._base_url}/ValueList/Search",
                    params={"name": name},
                    timeout=TIMEOUT,
                    allow_redirects=False,
                )
                try:
                    if response.status_code == 404:
                        matches = []
                    else:
                        _check_response(response)
                        matches = response.json()
                        if not isinstance(matches, list) or any(
                            not isinstance(item, dict) or not isinstance(item.get("name"), str)
                            for item in matches
                        ):
                            raise ValueError("Invalid X-Flow value-list search response")
                finally:
                    response.close()

                if any(item["name"] == name for item in matches):
                    logger.info(f"X-Flow value list already exists: {name}, skipping creation")
                    continue

                logger.info(f"No X-Flow value list found with the configured name: {name}, creating it")
                response = session.post(  # Create the value list if name does not already exist
                    f"{self._base_url}/ValueList",
                    json={"name": name, "valueType": "Text", "description": entry["description"]},
                    timeout=TIMEOUT,
                    allow_redirects=False,
                )
                try:
                    _check_response(response)
                finally:
                    response.close()

                logger.info(f"Created X-Flow value list {name}")


def _check_response(response: requests.Response) -> None:
    if 300 <= response.status_code < 400:
        raise ValueError("X-Flow redirects are not allowed")
    response.raise_for_status()


def _https_url(url: str, allowed_hosts: set[str] | None = None) -> str:
    """
    Validate an HTTPS URL and, when provided, its allowed hostname.

    :param url: URL to validate.
    :param allowed_hosts: Exact DNS hostnames permitted for the URL.
    :return: The validated URL.
    :raises ValueError: If the URL is invalid or its hostname is not permitted.
    """
    if not isinstance(url, str):
        raise ValueError("URL must be a string")

    try:
        parsed = urlsplit(url)
        port = parsed.port
        host = parsed.hostname
    except ValueError as exc:
        raise ValueError("Invalid HTTPS URL") from exc

    if parsed.scheme != "https" or not host:
        raise ValueError("URL must use HTTPS and have a hostname")

    if parsed.username is not None or parsed.password is not None:
        raise ValueError("URL credentials are not allowed")

    if parsed.fragment or parsed.query or port not in (None, 443):
        raise ValueError("URL cannot have a query, fragment or non-443 port")

    hostname = _hostname(host)
    if allowed_hosts is not None and hostname not in allowed_hosts:
        raise ValueError("URL host is not in allowed_url_hosts")

    return url


def _hostname(host: str) -> str:
    """
    Normalize and validate a DNS hostname.

    :param host: Hostname to validate.
    :return: The lowercase hostname.
    :raises ValueError: If the host is not a valid DNS hostname.
    """
    if not isinstance(host, str) or not HOST_PATTERN.fullmatch(host.lower()):
        raise ValueError("Allowed hosts must be DNS hostnames, without a scheme or port")

    try:
        ipaddress.ip_address(host)
    except ValueError:
        return host.lower()

    raise ValueError("IP addresses are not allowed")
