"""Validate configured sources and replace X-Flow value lists."""

import ipaddress
import json
import logging
import re
from urllib.parse import urlsplit
from uuid import UUID

import requests

from airflow.models import Variable
from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)
HOST_PATTERN = re.compile(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}")
TIMEOUT = (10, 30)  # (connect timeout, read timeout) in seconds
MAX_SOURCE_BYTES = 1_000_000  # Maximum allowed size for source content in bytes
SOURCE_FIELDS = {"values", "url", "airflow"}  # Allowed source types for value lists


def sync_value_lists(config_text: str) -> None:
    """
    Resolve configured sources and replace the corresponding X-Flow lists.

    :param config_text: JSON runtime configuration for the value-list sync.
    :return: None.
    :raises ValueError: If configuration, sources or the X-Flow connection is invalid.
    """
    entries, allowed_hosts, created_by = _config(config_text=config_text)
    # Resolve sources before syncing lists, so invalid sources won't result in a partial sync
    prepared = []
    with requests.Session() as source_session:
        for entry in entries:
            origin = entry["origin"]
            # Entries provide values inline, through an Airflow Variable, or from an approved HTTPS URL
            if origin == "values":
                values = entry["values"]

            elif origin == "airflow":
                source = Variable.get(entry["airflow"], default_var=None)
                if source is None:
                    raise ValueError("Source Airflow Variable is missing")

                values = _values(_json(source))

            else:
                values = _url_values(url=_https_url(entry["url"], allowed_hosts), session=source_session)

            prepared.append((str(UUID(entry["list_id"])), values))

    # Validate the destination connection only after all source lists are ready
    connection = BaseHook.get_connection("xflow_value_lists")
    if not connection.password:
        raise ValueError("xflow Connection requires a password")

    base_url = connection.host
    _https_url(url=base_url)
    parsed_base = urlsplit(base_url)
    if not re.fullmatch(r"api\.[a-z0-9-]+\.xflow\.dk", parsed_base.hostname or "") or parsed_base.path not in ("", "/"):
        raise ValueError("xflow Connection host must be an X-Flow API base URL")

    with requests.Session() as destination_session:
        destination_session.headers.update({"publicApiToken": connection.password})
        for list_id, values in prepared:
            # X-Flow replaces the complete list; each item needs its positional key and one-based index
            items = [
                {"value": value, "key": str(index), "oprettetAf": created_by, "indeks": str(index)}
                for index, value in enumerate(values, start=1)
            ]
            response = destination_session.put(
                f"{base_url.rstrip('/')}/ValueList/{list_id}/Values",
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

            logger.info("Updated X-Flow value list %s with %s items", list_id, len(values))


def _config(config_text: str) -> tuple[list[dict], set[str], str]:
    """
    Validate the runtime configuration and its permitted source hosts.

    :param config_text: JSON runtime configuration for the value-list sync.
    :return: Sync entries, normalized allowed hosts and creator attribution.
    :raises ValueError: If the configuration or any list entry is invalid.
    """
    config = _json(text=config_text)
    if not isinstance(config, dict) or set(config) != {"sync", "allowed_url_hosts", "created_by"}:
        raise ValueError("Configuration must contain only sync, allowed_url_hosts and created_by")

    hosts = config["allowed_url_hosts"]
    entries = config["sync"]
    created_by = config["created_by"]
    if not isinstance(hosts, list) or not isinstance(entries, list) or not entries:
        raise ValueError("allowed_url_hosts must be an array and sync a non-empty array")

    if not isinstance(created_by, str) or not created_by.strip():
        raise ValueError("created_by must be a non-blank string")

    allowed_hosts = {_hostname(host=host) for host in hosts}
    seen_ids = set()
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("origin"), str) or entry["origin"] not in SOURCE_FIELDS:
            raise ValueError("Each sync entry must have a supported origin")

        origin = entry["origin"]
        if set(entry) != {"list_id", "origin", origin}:
            raise ValueError("Each sync entry must specify exactly one source")

        try:
            list_id = str(UUID(entry["list_id"]))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("Each list_id must be a UUID") from exc

        if list_id in seen_ids:
            raise ValueError("Duplicate list_id")

        seen_ids.add(list_id)
        if origin == "values":
            _values(data=entry["values"])

        elif origin == "url":
            _https_url(url=entry["url"], allowed_hosts=allowed_hosts)

        elif not isinstance(entry["airflow"], str) or not entry["airflow"].strip():
            raise ValueError("airflow must name a source Variable")

    return entries, allowed_hosts, created_by


def _url_values(url: str, session: requests.Session) -> list[str]:
    """
    Fetch an external JSON value array without following redirects.

    :param url: Approved HTTPS URL to fetch.
    :param session: Requests session used for source retrieval.
    :return: Validated non-empty list of strings from the response.
    :raises ValueError: If the response redirects, is too large or contains invalid values.
    """
    # Stream response to enforce the size limit even if the server omits Content-Length
    response = session.get(url, timeout=TIMEOUT, allow_redirects=False, stream=True)
    try:
        if 300 <= response.status_code < 400:
            raise ValueError("Source URL redirects are not allowed")

        response.raise_for_status()
        content = bytearray()
        for chunk in response.iter_content(chunk_size=8192):
            content.extend(chunk)
            if len(content) > MAX_SOURCE_BYTES:
                raise ValueError("Source URL exceeds size limit")

        return _values(_json(content.decode("utf-8")))
    finally:
        response.close()


def _json(text: str) -> object:
    """
    Parse JSON and normalize decoding errors.

    :param text: JSON document to parse.
    :return: The decoded JSON value.
    :raises ValueError: If the text is not valid JSON.
    """
    try:
        return json.loads(text)
    except (ValueError, TypeError) as exc:
        raise ValueError("Invalid JSON in value-list configuration or source") from exc


def _values(data: object) -> list[str]:
    """
    Validate a non-empty array of non-blank strings.

    :param data: Decoded source value to validate.
    :return: The validated list of strings.
    :raises ValueError: If data is not a non-empty list of non-blank strings.
    """
    if not isinstance(data, list) or not data or any(
        not isinstance(value, str) or not value.strip() for value in data
    ):
        raise ValueError("Value-list sources must be non-empty JSON arrays of non-blank strings")

    return data


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
