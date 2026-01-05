import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

from dag_gis_to_dalux.dalux_data import (
    dalux_update_building,
    dalux_update_building_polygon,
)

logger = logging.getLogger(__name__)


def process_gis_to_dalux() -> None:
    gis_db = PostgresHook(postgres_conn_id="gis_db")
    dalux_hook = HttpHook(http_conn_id="dalux_api")

    sql = """
        SELECT id, sbsys, kaldenavn, institutionsnavn, servicedistrikt,
               kl_anvendelsesomraade, kl_ejerskab, bbr_bygningsnummer,
               ST_AsText(wkb_geometry) AS wkt
        FROM nye_tabeller.v_kommunale_bygninger_rk_dalux
    """

    records = gis_db.get_records(sql)
    logger.info(f"Fetched {len(records)} buildings from GIS DB")

    for row in records:
        (
            building_id,
            sbsys,
            alt_name,
            inst_name,
            service_district,
            usage_area,
            ownership,
            bbr_number,
            wkt,
        ) = row

        if not building_id:
            logger.warning("Skipping row without building_id")
            continue

        if inst_name:
            inst_name = inst_name.strip()

        if service_district:
            service_district = service_district.replace("Distrikt", "").strip()

        dalux_update_building(
            http_hook=dalux_hook,
            building_id=building_id,
            data_fields={"alternativeName": alt_name, "name": bbr_number},
            user_fields={
                "SBSYS": sbsys,
                "Institutionsnavn": inst_name,
                "Servicedistrikt": service_district,
                "KL Anvendelsesområde": usage_area,
                "KL Ejerskab": ownership,
            },
        )

        if wkt:
            dalux_update_building_polygon(
                http_hook=dalux_hook, building_id=building_id, wkt=wkt
            )

    logger.info("Completed moving GIS data into Dalux FM")
