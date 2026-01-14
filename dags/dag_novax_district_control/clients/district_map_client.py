from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
import requests
import time


def _rows_to_dicts(cursor, rows: list[tuple]) -> list[dict]:
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


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


class DistrictMapDBClient:
    def __init__(self):
        self._hook = PostgresHook(postgres_conn_id="gis_db")
        self._table: str = "nye_tabeller.sundhedsplejedistrikter_rk_all"
        self._geometry_column: str = "wkb_geometry"
        self._srid: int = 25832
        self._max_values_page_size: int = 100

    def get_district_rows_by_key(self, keyed_points: list[tuple[str, float, float]]) -> dict[str, dict | None]:
        """
        Connects to PostGIS DB to perform a batch lookup of districts for many points.

        :param keyed_points: A list of tuples (key, x, y) where key is an identifier for the point.
        :returns: A dict mapping from key to district row (as dict) or None if not found.
        """
        if not keyed_points:
            return {}

        sql = f"""
            WITH pts AS (
                SELECT * FROM (VALUES %s) AS v(key, x, y)
            )
            SELECT
                pts.key,
                d.*
            FROM pts
            LEFT JOIN LATERAL (
                SELECT d.*
                FROM {self._table} d
                WHERE ST_Contains(
                    d.{self._geometry_column},
                    ST_SetSRID(ST_MakePoint(pts.x, pts.y), {self._srid})
                )
                LIMIT 1
            ) d ON TRUE
            ORDER BY pts.key;
        """
        conn = self._hook.get_conn()
        cursor = conn.cursor()
        try:
            # Use execute_values for efficient batch fetching
            rows = execute_values(
                cursor,
                sql,
                keyed_points,
                page_size=min(len(keyed_points), self._max_values_page_size),
                fetch=True,
            )
            dict_rows = _rows_to_dicts(cursor, rows)
        finally:
            try:
                cursor.close()
            finally:
                conn.close()

        # Initialize result dict with keys from keyed_points and default None values
        result: dict[str, dict | None] = {key: None for key, _, _ in keyed_points}
        for row in dict_rows:
            key = row.get("key")
            if key is None:
                continue
            row.pop("key", None)
            # Assign row to result dict
            result[str(key)] = row if row else None
        return result

    def get_district_names_by_key(self, keyed_points: list[tuple[str, float, float]]) -> dict[str, str | None]:
        """
        Batch lookup of district names (distriktnavn) for many points.

        :param keyed_points: A list of tuples (key, x, y) where key is an identifier for the point (navnid).
        :returns: A dict mapping from key to district name (str) or None if not found.
        """
        # Get district rows by key
        rows_by_key = self.get_district_rows_by_key(keyed_points)
        result: dict[str, str | None] = {}

        # Extract district names from rows
        for key, row in rows_by_key.items():
            name = None
            if row is not None:
                name = row.get("distriktnavn")
            result[key] = str(name) if name is not None else None
        return result
