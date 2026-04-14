import logging
from typing import Optional, List, Tuple, Dict, Any
from airflow.providers.http.hooks.http import HttpHook
from pyproj import Transformer

logger = logging.getLogger(__name__)


def _parse_polygon_wkt(wkt: str) -> List[Tuple[float, float]]:
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

        points: List[Tuple[float, float]] = []
        for pair in coords.split(","):
            pair = pair.strip().replace("(", "").replace(")", "")
            x, y = map(float, pair.split())
            points.append((x, y))
        return points

    except Exception as e:
        logger.error(f"Failed to parse WKT polygon: {e}")
        return []


def _utm32_to_latlon(x: float, y: float) -> Tuple[float, float]:
    """
    Convert UTM zone 32N (EPSG:25832) coordinates into WGS84 (EPSG:4326).

    :param x: UTM32 x coordinate.
    :param y: UTM32 y coordinate.
    :return: (lat, lon) tuple.
    """
    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(x, y)
    return lat, lon


def _get_dalux_headers(http_hook: HttpHook) -> Dict:
    """
    Build HTTP headers for Dalux FM API using the API key from Airflow connection.

    :param http_hook: Airflow HttpHook for the Dalux FM API.
    :return: Dictionary with HTTP headers.
    """
    conn = http_hook.get_connection(http_hook.http_conn_id)
    api_key = conn.extra_dejson.get("api_key")
    return {"X-API-Key": api_key}


def _dalux_get_building(
    http_hook: HttpHook, building_id: int
) -> Optional[Dict[str, Any]]:
    """
    Fetch full building payload (data + links) from Dalux.

    :param http_hook: Airflow HttpHook with Dalux connection.
    :param building_id: Dalux building ID.
    :return: Dict with building data and links, or None if error.
    """
    try:
        headers = _get_dalux_headers(http_hook)
        http_hook.method = "GET"
        res = http_hook.run(
            endpoint=f"/api/2.0/buildings/{building_id}", headers=headers
        )
        js = res.json()
        return {"data": js.get("data"), "links": js.get("links", [])}
    except Exception as e:
        logger.error(f"Failed to fetch building {building_id}: {e}")
        return None


def dalux_update_building(
    http_hook: HttpHook,
    building_id: int,
    data_fields: Optional[Dict[str, Optional[str]]] = None,
    user_fields: Optional[Dict[str, Optional[str]]] = None,
) -> bool:
    """
    Update building fields in Dalux (data + userDefinedFields).

    :param http_hook: Airflow HttpHook with Dalux connection.
    :param building_id: Dalux building ID.
    :param data_fields: Dict of core fields to update.
    :param user_fields: Dict of user defined fields to update.
    :return: True if update succeeded, False otherwise.
    """
    try:
        payload = _dalux_get_building(http_hook, building_id)
        if not payload:
            return False

        # Update core fields
        if data_fields:
            for k, v in data_fields.items():
                if v is not None:
                    payload["data"][k] = v

        # Update user defined fields
        if user_fields:
            udf_items = payload["data"].get("userDefinedFields", {}).get("items", [])

            def upsert(name: str, value: Any):
                for item in udf_items:
                    if item.get("name") == name:
                        item["values"] = [{"text": str(value)}]
                        return
                udf_items.append({"name": name, "values": [{"text": str(value)}]})

            for key, val in user_fields.items():
                if val is not None:
                    upsert(key, val)

            payload["data"]["userDefinedFields"] = {"items": udf_items}

        headers = _get_dalux_headers(http_hook)
        http_hook.method = "PATCH"
        http_hook.run(
            endpoint=f"/api/3.0/buildings/{building_id}", headers=headers, json=payload
        )
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

        headers = _get_dalux_headers(http_hook)
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
