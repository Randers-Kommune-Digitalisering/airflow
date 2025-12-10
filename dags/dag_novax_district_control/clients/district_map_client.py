from airflow.hooks.base import BaseHook
import requests

DATAFORSYNING_API_URL = 'https://api.dataforsyningen.dk'
MAP_API_URL = 'http://sandkasse-srv.randers.dk:10000/'


class DataforsyningClient:
    def __init__(self, conn_id='dataforsyning_default'):
        connection = BaseHook.get_connection(conn_id)
        self.base_url = connection.host or DATAFORSYNING_API_URL
        self.session = requests.Session()

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
            'per_side': 104,
            'noformat': 1,
            'srid': 25832,
            'kommunekode': 730
        }
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
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
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()


class DistrictMapClient:
    def __init__(self, conn_id='district_map_default'):
        connection = BaseHook.get_connection(conn_id)
        self.base_url = connection.host or MAP_API_URL
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
