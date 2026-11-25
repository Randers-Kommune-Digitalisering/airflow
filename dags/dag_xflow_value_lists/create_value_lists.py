"""Prepare value-list definitions from configured sources for creation."""

import requests
from airflow.models import Variable

from dag_xflow_value_lists.sync_value_lists import _json, _url_json
from dag_xflow_value_lists.xflow_client import XFlowClient, _hostname, _https_url


def create_value_lists(config_text: str) -> None:
    """
    Resolve configured sources and create X-Flow value lists with new names.

    :param config_text: JSON runtime configuration for value-list creation.
    :return: None.
    :raises ValueError: If the configuration, a source or the X-Flow connection is invalid.
    """
    xflow_client = XFlowClient()
    config = _json(text=config_text)
    if not isinstance(config, dict) or set(config) != {"create", "allowed_url_hosts"}:
        raise ValueError("Creation configuration must contain only create and allowed_url_hosts")

    entries = config["create"]
    hosts = config["allowed_url_hosts"]
    if not isinstance(entries, list) or not entries or not isinstance(hosts, list):
        raise ValueError("create must be a non-empty array and allowed_url_hosts an array")

    allowed_hosts = {_hostname(host) for host in hosts}
    prepared = []
    with requests.Session() as source_session:
        for entry in entries:
            if not isinstance(entry, dict) or entry.get("origin") not in ("values", "url", "airflow"):
                raise ValueError("Each create entry must have a supported origin")
            origin = entry["origin"]
            if set(entry) != {"origin", origin}:
                raise ValueError("Each create entry must specify exactly one source")

            if origin == "values":
                definition = entry["values"]
            elif origin == "airflow":
                if not isinstance(entry["airflow"], str) or not entry["airflow"].strip():
                    raise ValueError("airflow must name a source Variable")
                source = Variable.get(entry["airflow"], default_var=None)
                if source is None:
                    raise ValueError("Source Airflow Variable is missing")
                definition = _json(text=source)
            else:
                definition = _url_json(url=_https_url(entry["url"], allowed_hosts), session=source_session)

            if (
                not isinstance(definition, dict)
                or set(definition) != {"name", "description"}
                or not isinstance(definition["name"], str)
                or not definition["name"].strip()
                or not isinstance(definition["description"], str)
            ):
                raise ValueError("Creation source must contain a non-blank name and a string description")
            prepared.append(definition)

    names = [entry["name"] for entry in prepared]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate value-list name in creation configuration")

    xflow_client.create_value_lists(prepared=prepared)
