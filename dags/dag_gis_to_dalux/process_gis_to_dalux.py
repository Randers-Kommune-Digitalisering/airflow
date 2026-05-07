import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

from dag_gis_to_dalux.dalux_data import (
    dalux_create_building,
    dalux_list_buildings,
    dalux_building_index_by_estate_and_bbr_number,
    dalux_update_building,
    dalux_update_building_polygon,
    parse_building_address,
)

logger = logging.getLogger(__name__)


def process_gis_to_dalux() -> None:
    gis_db = PostgresHook(postgres_conn_id="gis_db")
    dalux_hook = HttpHook(http_conn_id="dalux_api")

    sql = """
        SELECT
            id,
            estate_id,
            sbsys,
            kaldenavn,
            institutionsnavn,
            servicedistrikt,
            kl_anvendelsesomraade,
            kl_ejerskab,
            kl_bda_areal,
            bbr_bygningsnummer,
            ejendom_adresse,
            ST_AsText(wkb_geometry) AS wkt
        FROM nye_tabeller.v_kommunale_bygninger_rk_dalux
        WHERE id is NULL AND estate_id = 102 AND bbr_bygningsnummer = '19'
    """
    records = gis_db.get_records(sql)
    logger.info(f"Fetched {len(records)} buildings from GIS DB")

    # Get all Dalux buildings and build a lookup (estateId, bbr_number) -> buildingId for deduplication and matching.
    dalux_items = dalux_list_buildings(http_hook=dalux_hook)
    dalux_index = dalux_building_index_by_estate_and_bbr_number(building_items=dalux_items)
    logger.info(f"Fetched {len(dalux_items)} buildings from Dalux for dedupe")

    for row in records:
        (
            building_id,
            estate_id,
            sbsys,
            alt_name,
            inst_name,
            service_district,
            usage_area,
            ownership,
            kl_bda_areal,
            bbr_number,
            building_address,
            wkt,
        ) = row

        kl_bda_areal_int = None
        if kl_bda_areal is not None and str(kl_bda_areal).strip() != "":
            try:
                kl_bda_areal_int = int(str(kl_bda_areal).strip())
            except ValueError:
                logger.warning(f"Invalid kl_bda_areal (not int)")

        if inst_name:
            inst_name = inst_name.strip()

        cleaned_service_district = None
        if service_district:
            cleaned_service_district = service_district.replace("Distrikt", "").strip()

        # Decide which Dalux buildingId to use for this GIS record:
        effective_building_id = None

        if building_id:
            effective_building_id = int(building_id)
        else:
            # GIS needs to be matched to Dalux building via (estate_id, bbr_number)
            if not estate_id or not bbr_number:
                logger.warning(
                    "Skipping row without estate_id or bbr_bygningsnummer (cannot dedupe/create safely)"
                )
                continue

            key = (str(estate_id).strip(), str(bbr_number).strip())
            effective_building_id = dalux_index.get(key)

            if effective_building_id:
                logger.info(
                    f"Matched existing Dalux building via (estate_id, bbr number): {key} -> {effective_building_id}"
                )
            else:
                address = parse_building_address(building_address=building_address)

                new_dalux_building_id = dalux_create_building(
                    http_hook=dalux_hook,
                    bbr_number=str(bbr_number).strip(),
                    alternative_name=alt_name,
                    estate_id=str(estate_id).strip(),
                    address=address,
                )

                if not new_dalux_building_id:
                    logger.error(f"Failed to create building for key={key}")
                    continue

                effective_building_id = new_dalux_building_id
                dalux_index[key] = effective_building_id  # avoid creating duplicates if same estate_id + bbr_number appears again in the loop

        # Run existing update + polygon logic on the effective_building_id we ended up with
        dalux_update_building(
            http_hook=dalux_hook,
            building_id=effective_building_id,
            data_fields={"alternativeName": alt_name, "name": bbr_number}, # Name in Dalux FM is BBR number
            # Needs to match Userdefined fields in Dalux FM for Buildings:
            user_fields={
                "Ny SBSYS": sbsys,
                "Ny Institutionsnavn": inst_name,
                "Ny Servicedistrikt": cleaned_service_district,
                "Ny KL Anvendelsesområde": usage_area,
                "Ny KL Ejerskab": ownership,
                "Ny KL BDA areal": kl_bda_areal_int,
            },
        )

        if wkt:
            dalux_update_building_polygon(
                http_hook=dalux_hook, building_id=effective_building_id, wkt=wkt
            )

    logger.info("Completed moving GIS data into Dalux FM")