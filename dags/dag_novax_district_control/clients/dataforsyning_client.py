import logging
import requests

from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class DataforsyningClient:
    def __init__(self):
        connection = BaseHook.get_connection("dataforsyningen")
        self.base_url = connection.host
        self.session = requests.Session()

    def get_address_by_id(self, adresse_id: str) -> dict | None:
        """
        Connects to Dataforsyning API to get address information by adresse_id.

        :param adresse_id: The unique identifier for the address.
        :return: A dictionary containing:
            full_address(str),
            number_floor(str),
            street_code(int),
            town_name(str|None),
            postal_code(str),
            municipality_code(int),
            coordinates(tuple[float, float])
            OR None if the lookup returns an unexpected number of results.
        """
        # Fails if Dataforsyning is down/broken or if the adresse_id is invalid.
        # NOTE: If the API returns 0 or >1 results, we do not fail the entire job.
        # We treat that as "skip address + district lookup for this user".
        endpoint = '/adresser/autocomplete'
        params = {
            'id': adresse_id,
            'type': 'adresser',
            'side': 1,
            'per_side': 2,
            'noformat': 1,
            'srid': 25832,
            'kommunekode': 730
        }

        url = f"{self.base_url}{endpoint}"
        results = self.session.get(url, params=params, timeout=10)
        results.raise_for_status()
        data = results.json()
        if len(data) != 1:
            logger.error(
                "Dataforsyning lookup for adresse_id=%s returned %s result(s); expected exactly 1. Skipping address/district lookup for this user.",
                adresse_id,
                len(data),
            )
            return None

        full_address = data[0].get('tekst', '')

        number = data[0].get('adresse', {}).get('husnr')
        floor = data[0].get('adresse', {}).get('etage')
        door = data[0].get('adresse', {}).get('dør')

        number_floor = f"{number}"
        if floor:
            number_floor += f", {floor}"
        if door:
            number_floor += f" {door}"

        street_code = int(data[0].get('adresse', {}).get('vejkode'))
        town_name = data[0].get('adresse', {}).get('supplerendebynavn')  # optional field that may be empty
        postal_code = int(data[0].get('adresse', {}).get('postnr'))
        municipality_code = int(data[0].get('adresse', {}).get('kommunekode'))
        coordinates = (float(data[0].get('adresse', {}).get('x', 0)), float(data[0].get('adresse', {}).get('y', 0)))

        if not all([full_address, number_floor, street_code, postal_code, municipality_code,
                    coordinates and coordinates[0] and coordinates[1]]):
            raise ValueError(f"A required address field is missing in response for adresse_id {adresse_id}")

        return {
            "full_address": full_address,
            "number_floor": number_floor,
            "street_code": street_code,
            "town_name": town_name,  # optional
            "postal_code": postal_code,
            "municipality_code": municipality_code,
            "coordinates": coordinates
        }
