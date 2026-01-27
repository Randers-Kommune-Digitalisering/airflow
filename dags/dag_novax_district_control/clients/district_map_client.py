from airflow.hooks.base import BaseHook
import requests
import time
from sqlalchemy.orm import Session
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from shapely import wkb
from shapely.geometry import Point, multipolygon


Base = declarative_base()


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


class District(Base):
    __tablename__ = 'sundhedsplejedistrikter_rk_all'
    __table_args__ = {'schema': 'nye_tabeller'}
    distriktnavn = Column(String, primary_key=True)
    wkb_geometry = Column(Geometry(geometry_type='MULTIPOLYGON', srid=25832))


class DistrictItem():
    def __init__(self, name: str, geom: multipolygon.MultiPolygon):
        self.name = name
        self.geom = geom


class DistrictMapDBClient:
    def __init__(self):
        with Session(PostgresHook(postgres_conn_id="gis_db").get_sqlalchemy_engine()) as session:
            districts = session.query(District).all()
            self._districts: list[DistrictItem] = [DistrictItem(d.distriktnavn, wkb.loads(bytes(d.wkb_geometry.data))) for d in districts]

    def get_district_names_by_key(self, points: list[tuple[str, float, float]]) -> dict[str, str | None]:
        result = {}
        for key, x, y in points:
            pt = Point(x, y)
            result[key] = next((d.name for d in self._districts if d.geom.contains(pt)), None)
        return result
