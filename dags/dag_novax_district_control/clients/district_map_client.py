import logging
from sqlalchemy.orm import Session
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base
from geoalchemy2 import Geometry
from shapely import wkb
from shapely.geometry import Point, multipolygon


Base = declarative_base()
logger = logging.getLogger(__name__)


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

    def get_district_name_for_point(self, x: float, y: float) -> str:
        """
        Returns the district name for a single (x, y) point.

        :param x: X coordinate
        :param y: Y coordinate
        :return: The district name
        """
        # Fails if no district is found for the point, or if the geometry data is invalid.
        # We want it to fail hard if address coordinates are not in any district, so we can fix the underlying issue.
        pt = Point(x, y)
        district_name = next((d.name for d in self._districts if d.geom.contains(pt)), None)
        if district_name is None:
            raise ValueError(f"No district found for point ({x}, {y})")
        return district_name
