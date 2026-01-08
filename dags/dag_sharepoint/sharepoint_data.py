import logging

from typing import List, Optional, Dict, Any
from msgraph.graph_service_client import GraphServiceClient

logger = logging.getLogger(__name__)

SHAREPOINT_FIELDS: List[str] = [
    "Title",
    "Fase",
    "Uddybning",
    "Forvaltning",
    "Spor",
    "Status",
    "Status_x002d_uddybning",
    "Afdeling_x002f_delomr_x00e5_de",
    "Projektleder0",
    "Projektejer0",
    "Program_x002f_overordnetindsats_",
    "Teknologi",
]


async def ms_graph_get_sharepoint_list_items_async(
    client: GraphServiceClient,
    site_id: str,
    list_id: str,
    fields: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve all SharePoint list items from Microsoft Graph API.

    :param client: GraphServiceClient instance.
    :param site_id: SharePoint site ID.
    :param list_id: SharePoint list ID.
    :param fields: List of field names to retrieve (optional).
    :return: List of dictionaries with SharePoint list fields.
    """

    if fields:
        select_fields = ",".join(fields)
        url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            f"?$expand=fields($select={select_fields})"
        )
    else:
        url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
            "?$expand=fields"
        )

    all_items = []

    while url:
        resp = await client.sites.with_url(url).get()
        all_items.extend(resp.value or [])
        url = resp.additional_data.get("@odata.nextLink")

    result = []
    for item in all_items:
        fields_data = item.additional_data.get("fields", {})
        result.append(fields_data)

    return result


def transform_sharepoint_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform and rename fields from SharePoint list items.

    :param items: List of dictionaries with SharePoint list fields.
    :return: List of transformed dictionaries.
    """
    rename_map = {
        "Status_x002d_uddybning": "Status - uddybning",
        "Afdeling_x002f_delomr_x00e5_de": "Afdeling/delområde",
        "Program_x002f_overordnetindsats_": "Program eller konkret indsats",
    }
    filtered_items = []
    for item in items:
        fields_data = item.copy()
        for key in ["Forvaltning", "Spor", "Afdeling_x002f_delomr_x00e5_de"]:
            if key in fields_data and isinstance(fields_data[key], list):
                fields_data[key] = ", ".join(fields_data[key])
        if "Teknologi" in fields_data and isinstance(
            fields_data["Teknologi"], (set, list)
        ):
            fields_data["Teknologi"] = ", ".join(fields_data["Teknologi"])
        projektleder = fields_data.get("Projektleder0")
        projektejer = fields_data.get("Projektejer0")

        def extract_user_info(user):
            if isinstance(user, list) and user:
                user = user[0]
            if isinstance(user, dict):
                return user.get("LookupValue"), user.get("Email")
            return None, None

        projektleder_name, projektleder_email = extract_user_info(projektleder)
        projektejer_name, projektejer_email = extract_user_info(projektejer)

        renamed_fields = {}
        for k, v in fields_data.items():
            if k not in ("Projektleder0", "Projektejer0"):
                renamed_fields[rename_map.get(k, k)] = v
        renamed_fields["Projektleder_Name"] = projektleder_name
        renamed_fields["Projektleder_Email"] = projektleder_email
        renamed_fields["Projektejer_Name"] = projektejer_name
        renamed_fields["Projektejer_Email"] = projektejer_email
        filtered_items.append(renamed_fields)
    return filtered_items
