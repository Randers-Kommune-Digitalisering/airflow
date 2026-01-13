from airflow.hooks.base import BaseHook
import requests
import time


class DataforsyningClient:
    def __init__(self):
        connection = BaseHook.get_connection("dataforsyningen")
        self.base_url = connection.host
        self.session = requests.Session()

    def _get_with_retry(self, url: str, params: dict, retries: int = 3, delay_seconds: int = 5) -> requests.Response:
        attempt = 0
        while True:
            try:
                response = self.session.get(url, params=params)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if response.status_code not in [200, 400] and attempt < retries:
                    attempt += 1
                    time.sleep(delay_seconds)
                    continue
                raise

    def lookup_address(self, query: str) -> dict:
        """
        Connects to Dataforsyning API to lookup (search) address information.

        :param query: The full address query string.
        """
        endpoint = '/adgangsadresser/autocomplete'
        params = {
            'q': query,
            'type': 'adgangsadresse',
            'side': 1,
            'per_side': 1,
            'noformat': 1,
            'srid': 25832,
            'kommunekode': 730
        }
        url = f"{self.base_url}{endpoint}"
        response = self._get_with_retry(url, params=params)
        results = response.json()
        return results[0] if results and len(results) > 0 else None

    def get_address_info(self, address_id) -> dict:
        """
        Connects to Dataforsyning API to get detailed address information by ID.

        :param address_id: The ID of the address to lookup.
        """
        endpoint = f'/adgangsadresser/{address_id}'
        params = {
            'format': 'geojson',
            'struktur': 'nestet'
        }
        url = f"{self.base_url}{endpoint}"
        response = self._get_with_retry(url, params=params)
        return response.json()


class DistrictMapClient:
    def __init__(self):
        connection = BaseHook.get_connection("district_map")
        self.base_url = connection.host
        self.session = requests.Session()

    def get_district(self, coordinate_x, coordinate_y) -> dict:
        """
        Connects to District Map API to get district information based on coordinates.

        :param coordinate_x: The X coordinate (easting) of the address.
        :param coordinate_y: The Y coordinate (northing) of the address.
        """
        endpoint = '/spatialmap'
        headers = {
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'http://kort.randers.dk',
            'Referer': 'http://kort.randers.dk/'
        }
        params = {
            'page': 'widget.spatialquery',
            'profile': 'ekstern_all_widget',
            'layers': 'theme-sundhedsplejedistrikter_rk_geom'
        }
        body = f"geometri=POINT+({coordinate_x}+{coordinate_y})"
        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, params=params, data=body, headers=headers)
        response.raise_for_status()
        result = response.json()
        labels = self.find_labels(result)
        return labels[0] if labels else None

    def find_labels(self, obj):
        labels = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "label":
                    labels.append(v)
                else:
                    labels.extend(self.find_labels(v))
        elif isinstance(obj, list):
            for item in obj:
                labels.extend(self.find_labels(item))
        return labels
