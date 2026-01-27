from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
import requests
import time
from sqlalchemy import text
from sqlalchemy.orm import Session


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
            except requests.exceptions.HTTPError:
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
        self._engine = None

    def _get_engine(self):
        if self._engine is None:
            self._engine = self._hook.get_sqlalchemy_engine()
        return self._engine

    def get_district_names_by_key(self, keyed_points: list[tuple[str, float, float]]) -> dict[str, str | None]:
        sql = text(f"""
                WITH pts AS (
                    SELECT * FROM (VALUES {",".join(["(:k{}, :x{}, :y{})".format(i,i,i) for i in range(len(keyed_points))])})
                    AS v(key, x, y)
                )
                SELECT
                    pts.key,
                    d.distriktnavn
                FROM pts
                LEFT JOIN LATERAL (
                    SELECT distriktnavn
                    FROM {self._table}
                    WHERE ST_Contains(
                        {self._table}.{self._geometry_column},
                        ST_SetSRID(ST_MakePoint(pts.x, pts.y), :srid)
                    )
                    LIMIT 1
                ) d ON TRUE
                ORDER BY pts.key;
            """)

        params = {"srid": self._srid}
        for i, (k, x, y) in enumerate(keyed_points):
            params[f"k{i}"] = k
            params[f"x{i}"] = x
            params[f"y{i}"] = y

        with Session(self._get_engine()) as session:
            rows = session.execute(sql, params).all()
            return dict(rows)
