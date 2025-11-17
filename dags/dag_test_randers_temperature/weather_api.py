import logging

from airflow.providers.http.hooks.http import HttpHook

logger = logging.getLogger(__name__)


def get_current_temperature(latitude: float, longitude: float) -> dict:
    hook = HttpHook(method="GET", http_conn_id="open_meteo_api")
    response = hook.run(
        endpoint="v1/forecast",
        data={"latitude": latitude, "longitude": longitude, "current": "temperature_2m"},
    )
    return response.json()
