import logging
from typing import Any
from airflow.providers.http.hooks.http import HttpHook
from pyproj import Transformer
import re

logger = logging.getLogger(__name__)


def _parse_polygon_wkt(wkt: str) -> list[tuple[float, float]]:
    """
    Parse a WKT POLYGON string into a list of (x, y) coordinate tuples.

    :param wkt: WKT POLYGON string.
    :return: List of (x, y) tuples.
    """
    try:
        wkt = wkt.strip()
        if wkt.upper().startswith("POLYGON"):
            start = wkt.find("((")
            end = wkt.find("))")
            if start != -1 and end != -1:
                coords = wkt[start + 2: end]
            else:
                start = wkt.find("(")
                end = wkt.find(")")
                coords = wkt[start + 1: end]
        else:
            coords = wkt

        points: list[tuple[float, float]] = []
        for pair in coords.split(","):
            pair = pair.strip().replace("(", "").replace(")", "")
            x, y = map(float, pair.split())
            points.append((x, y))
        return points

    except Exception as e:
        logger.error(f"Failed to parse WKT polygon: {e}")
        return []


def _utm32_to_latlon(x: float, y: float) -> tuple[float, float]:
    """
    Convert UTM zone 32N (EPSG:25832) coordinates into WGS84 (EPSG:4326).

    :param x: UTM32 x coordinate.
    :param y: UTM32 y coordinate.
    :return: (lat, lon) tuple.
    """
    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(x, y)
    return lat, lon


def _get_dalux_headers(http_hook: HttpHook) -> dict:
    """
    Build HTTP headers for Dalux FM API using the API key from Airflow connection.

    :param http_hook: Airflow HttpHook for the Dalux FM API.
    :return: Dictionary with HTTP headers.
    """
    conn = http_hook.get_connection(http_hook.http_conn_id)
    api_key = conn.extra_dejson.get("api_key")
    return {"X-API-Key": api_key}


def dalux_update_building(
    http_hook: HttpHook,
    building_id: int,
    data_fields: dict[str, str | None] | None = None,
    user_fields: dict[str, Any] | None = None,
) -> bool:
    """
    Update building fields in Dalux (core data + userDefinedFields).

    :param http_hook: Airflow HttpHook for the Dalux FM API.
    :param building_id: Dalux building ID to update.
    :param data_fields: Mapping of core building fields to update. Use None to skip a field.
    :param user_fields: Mapping of user-defined field names to values. Values are sent as
                        integer if the value is an int, otherwise as text.
    :return: True if the update succeeded (HTTP 200/204) or if there was nothing to update,
             otherwise False.
    """
    try:
        patch_data: dict[str, Any] = {}

        # Core fields
        if data_fields:
            for key, value in data_fields.items():
                if value is not None:
                    patch_data[key] = value

        # User defined fields (only the ones we explicitly want to set)
        udf_items: list[dict[str, Any]] = []
        if user_fields:
            for name, val in user_fields.items():
                if val is None:
                    continue

                if isinstance(val, int):
                    udf_items.append({"name": name, "values": [{"integer": val}]}) # for kl_bda_areal(int in dalux)
                else:
                    udf_items.append({"name": name, "values": [{"text": str(val)}]}) # for the rest of the fields as text

        if udf_items:
            patch_data["userDefinedFields"] = {"items": udf_items}

        # Nothing to update
        if not patch_data:
            logger.info(f"No updates to apply for building {building_id}")
            return True

        payload = {"data": patch_data}

        headers = _get_dalux_headers(http_hook=http_hook)
        http_hook.method = "PATCH"

        res = http_hook.run(
            endpoint=f"/api/3.0/buildings/{building_id}",
            headers=headers,
            json=payload,
        )

        if res.status_code not in (200, 204):
            logger.error(
                f"Dalux update failed for building {building_id}: {res.status_code} {res.text}"
            )
            return False

        logger.info(
            f"Updated building {building_id} data_fields: {data_fields}, userFields: {user_fields}"
        )
        return True

    except Exception as e:
        logger.error(f"Error updating building {building_id}: {e}")
        return False


def dalux_update_building_polygon(
    http_hook: HttpHook, building_id: int, wkt: str
) -> bool:
    """
    Update building outer polygon in Dalux.

    :param http_hook: Airflow HttpHook with Dalux connection.
    :param building_id: Dalux building ID.
    :param wkt: WKT POLYGON string.
    :return: True if update succeeded, False otherwise.
    """
    try:
        if not wkt:
            logger.warning(
                f"Building {building_id} has no geometry, skipping polygon update"
            )
            return False

        points = _parse_polygon_wkt(wkt)
        coords = []
        for x, y in points[:-1]:
            lat, lng = _utm32_to_latlon(x, y)
            coords.append({"lat": lat, "lng": lng})

        payload = {
            "data": {
                "OuterPolygon": {"data": {"coordinates": coords}},
                "InnerPolygons": [],
            },
            "links": [],
        }

        headers = _get_dalux_headers(http_hook=http_hook)
        http_hook.method = "PATCH"
        http_hook.run(
            endpoint=f"/api/2.0/buildings/{building_id}/polygon",
            headers=headers,
            json=payload,
        )
        logger.info(f"Updated polygon for building {building_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to update polygon for building {building_id}: {e}")
        return False


def dalux_create_building(
    http_hook: HttpHook,
    bbr_number: str,
    alternative_name: str | None,
    estate_id: str,
    address: dict[str, str] | None = None,
    owned: bool | None = None,
) -> int | None:
    """
    Create a new building in Dalux FM.

    :param http_hook: Airflow HttpHook for the Dalux FM API.
    :param bbr_number: Building BBR number.
    :param alternative_name: Alternative name for the building.
    :param estate_id: External estate_id used in Dalux (estateRef.estateId).
    :param address: Address payload for Dalux (typically keys: road/number/zipCode/city).
    :param owned: boolean for the owned flag.
    :return: The new Dalux buildingId if creation succeeded, otherwise None.
    """
    try:
        payload: dict[str, Any] = {"data": {"name": str(bbr_number)}}

        if alternative_name is not None:
            payload["data"]["alternativeName"] = alternative_name

        payload["data"]["estateRef"] = {"estateId": str(estate_id)}

        if address:
            payload["data"]["address"] = address

        if owned is not None:
            payload["data"]["owned"] = owned

        headers = _get_dalux_headers(http_hook=http_hook)
        http_hook.method = "POST"

        res = http_hook.run(
            endpoint="/api/3.0/buildings",
            headers=headers,
            json=payload,
        )

        if res.status_code not in (200, 201):
            logger.error(f"Dalux create failed: {res.status_code} {res.text}")
            return None

        js = res.json()

        new_id = js.get("data", {}).get("buildingId")
        if not new_id:
            logger.error(f"Dalux create succeeded but no id in response: {js}")
            return None
        
        logger.info(f"Created new Dalux building with building_id {new_id} for estate_id={estate_id}, bbr_number={bbr_number}")

        return int(new_id)

    except Exception as e:
        logger.error(f"Error creating building in Dalux: {e}")
        return None



def dalux_list_buildings(http_hook: HttpHook, limit: int = 100) -> list[dict[str, Any]]:
    """
    List all buildings from Dalux FM (paged).

    :param http_hook: Airflow HttpHook for the Dalux FM API.
    :param limit: Page size per request.
    :return: List of building items (payload entries) collected across pages.
    """
    headers = _get_dalux_headers(http_hook=http_hook)
    http_hook.method = "GET"

    all_items: list[dict[str, Any]] = []
    bookmark: str | None = "0"

    while bookmark is not None:
        res = http_hook.run(
            endpoint="/api/2.0/buildings",
            headers=headers,
            data={"bookmark": bookmark, "limit": min(limit, 100)},
        )
        if res.status_code != 200:
            logger.error(f"Dalux list buildings failed: {res.status_code} {res.text}")
            break

        payload = res.json()
        items = payload.get("items", []) or []
        all_items.extend(items)

        metadata = payload.get("metadata", {}) or {}
        next_bookmark = metadata.get("nextBookmark")

        if not next_bookmark:
            break

        if str(next_bookmark) == str(bookmark):
            logger.warning("Dalux paging stuck (nextBookmark == bookmark); aborting")
            break

        bookmark = str(next_bookmark)

    return all_items


def dalux_building_index_by_estate_and_bbr_number(
    building_items: list[dict[str, Any]],
) -> dict[tuple[str, str], int]:
    """
    Build a lookup index from Dalux building list items.

    Creates mapping (estateId, bbr_number) -> buildingId.

    :param building_items: Items returned by dalux_list_buildings (each item is expected to contain
                           a "data" object with "buildingId", "name", and "estateRef.estateId").
    :return: Dictionary mapping (estateId, bbr_number) to buildingId.
    """
    index: dict[tuple[str, str], int] = {}

    for item in building_items or []:
        data = (item or {}).get("data", {}) or {}

        building_id = data.get("buildingId")
        bbr_number = data.get("name")
        estate_id = (data.get("estateRef", {}) or {}).get("estateId")

        if not building_id or not bbr_number or not estate_id:
            continue

        key = (str(estate_id).strip(), str(bbr_number).strip())
        index[key] = int(building_id)

    return index


_ADDRESS_RE = re.compile(
    r"^\s*"
    r"(?P<road>.+?)" # "Test Skolevej"
    r"\s+"
    r"(?P<number>\d+[A-Za-zÆØÅæøå]?)" # "18" / "18A"
    r"\s*,\s*"
    r"(?P<zip>\d{4})" # fx "8960"
    r"\s+"
    r"(?P<city>.+?)" # fx "Randers SØ"
    r"\s*$"
)


def parse_building_address(building_address: str | None) -> dict[str, str] | None:
    """
    Parse an address string into a Dalux address dict.

    Examples:
      'Testvej 18A, 8920 Randers NV'

    :param building_address: Address string in the format road number, zip city.
    :return: Dict with keys road, number, zipCode, city; or None if parsing fails.
    """
    if not building_address:
        return None

    addr = " ".join(building_address.strip().split())
    m = _ADDRESS_RE.match(addr)
    if not m:
        return None

    return {
        "road": m.group("road").strip(),
        "number": m.group("number").strip(),
        "zipCode": m.group("zip").strip(),
        "city": m.group("city").strip(),
    }