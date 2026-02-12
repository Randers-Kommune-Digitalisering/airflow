from airflow.hooks.base import BaseHook
import logging
import requests
import time
from typing import Any
from sqlalchemy.orm import Session
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base
from geoalchemy2 import Geometry
from shapely import wkb
from shapely.geometry import Point, multipolygon


Base = declarative_base()
logger = logging.getLogger(__name__)


class DataforsyningClient:
    def __init__(self):
        connection = BaseHook.get_connection("dataforsyningen")
        self.base_url = connection.host
        self.session = requests.Session()

        extra = connection.extra_dejson or {}
        connect_timeout = extra.get("connect_timeout_seconds")
        read_timeout = extra.get("read_timeout_seconds")

        self.timeout: tuple[float, float] = (
            float(connect_timeout) if connect_timeout is not None else 5.0,
            float(read_timeout) if read_timeout is not None else 30.0,
        )

    def _get_with_retry(
        self,
        url: str,
        params: dict[str, Any],
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

    def lookup_address(self, query: str) -> dict | None:
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
        try:
            response = self._get_with_retry(url, params=params)
        except requests.RequestException as e:
            logger.warning(f"Error while looking up address '{query}': {e}")
            return None
        if response.status_code != 200:
            logger.warning(f"Failed to lookup address '{query}': HTTP {response.status_code}")
            return None
        try:
            results = response.json()
        except ValueError as e:
            logger.warning(f"Invalid JSON response while looking up address '{query}': {e}")
            return None
        return results[0] if results and len(results) > 0 else None

    def get_address_info(self, address_id) -> dict | None:
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
        try:
            response = self._get_with_retry(url, params=params)
        except requests.RequestException as e:
            logger.warning(f"Error while getting address info for ID '{address_id}': {e}")
            return None
        if response.status_code != 200:
            logger.warning(f"Failed to get address info for ID '{address_id}': HTTP {response.status_code}")
            return None
        try:
            return response.json()
        except ValueError as e:
            logger.warning(f"Invalid JSON response while getting address info for ID '{address_id}': {e}")
            return None


class District(Base):
    __tablename__ = 'sundhedsplejedistrikter_rk_all'
    __table_args__ = {'schema': 'nye_tabeller'}
    distriktnavn = Column(String, primary_key=True)
    wkb_geometry = Column(Geometry(geometry_type='MULTIPOLYGON', srid=25832))


class DistrictItem:
    def __init__(self, name: str, geom: multipolygon.MultiPolygon):
        self.name = name
        self.geom = geom


class DistrictMapDBClient:
    def __init__(self):
        with Session(PostgresHook(postgres_conn_id="gis_db").get_sqlalchemy_engine()) as session:
            districts = session.query(District).all()
            self._districts: list[DistrictItem] = [DistrictItem(d.distriktnavn, wkb.loads(bytes(d.wkb_geometry.data))) for d in districts]

    def get_district_names_by_key(self, points: list[tuple[str, float, float]]) -> dict[str, str | None]:
        """
        Returns a dictionary mapping keys to district names based on the provided points.

        :param points: A list of tuples containing (key, x, y) coordinates, where key is a unique identifier (i.e. navnid).
        :return: A dictionary mapping each key to the corresponding district name or None if not found
        """
        result = {}
        for key, x, y in points:
            pt = Point(x, y)
            result[key] = next((d.name for d in self._districts if d.geom.contains(pt)), None)
        return result
