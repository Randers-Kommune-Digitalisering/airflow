import logging
from urllib.parse import quote

import httpx
from msgraph.graph_service_client import GraphServiceClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dag_meddb_person_check.model import PersonSkoleAD

logger = logging.getLogger(__name__)


async def skole_ad_get_by_email(session: AsyncSession, email: str) -> dict | None:
    """
    Search for a user in Skole-AD by their email.

    :param session: The SQLAlchemy session to use for the query.
    :type session: Session
    :param email: The email to search for.
    :type email: str
    :return: A dictionary containing the keys: name, email, unit, username
    :rtype: dict
    """

    result = await session.execute(
        select(PersonSkoleAD).where(PersonSkoleAD.Mail == email)
    )
    user = result.scalars().first()
    if not user:
        return None

    return {
        "name": user.Navn,
        "email": user.Mail,
        "unit": user.Skole,
        "username": user.DQnummer,
    }


async def ms_graph_get_user_by_email_alias(client: GraphServiceClient, email_alias: str) -> dict:
    """
    Search for a user in Microsoft Graph by their email alias.

    :param email_alias: The email alias to search for.
    :type email_alias: str
    :return: A dictionary containing the keys: name, email, unit, username
    :rtype: dict
    """
    filter_expr = f"proxyAddresses/any(p:p eq 'smtp:{email_alias}')"
    filter_q = quote(filter_expr, safe="()':=")
    select_q = "mail,onPremisesSamAccountName,displayName,officeLocation"
    url = (
        "https://graph.microsoft.com/v1.0/users"
        f"?$select={select_q}&$filter={filter_q}"
    )

    resp = await client.users.with_url(url).get()
    users = resp.value or []

    if not users:
        return None
    if len(users) == 1:
        user = users[0]
        return {
            "name": user.display_name,
            "email": user.mail,
            "unit": user.office_location,
            "username": user.on_premises_sam_account_name
        }
    else:
        logger.warning(f"Multiple users found in MS Graph for email alias '{email_alias}'")
        return None


async def delta_get_by_email(client: httpx.AsyncClient, base_url: str, email: str) -> dict | None:
    """
    Query Delta system for persons matching the given email.

    :param client: An initialized httpx.AsyncClient (with Authorization header if needed).
    :type client: httpx.AsyncClient
    :param email: The email to search for.
    :type email: str
    :return: A dictionary containing the keys: name, email, unit, username.
    :rtype: dict
    """
    graph_query = {
        "graphQueries": [
            {
                "computeAvailablePages": True,
                "graphQuery": {
                    "structure": {
                        "alias": "person",
                        "userKey": "APOS-Types-Person",
                        "relations": [
                            {
                                "alias": "user",
                                "userKey": "APOS-Types-User-TypeRelation-Person",
                                "typeUserKey": "APOS-Types-User",
                                "direction": "IN"
                            },
                            {
                                "alias": "emp",
                                "userKey": "APOS-Types-Engagement-TypeRelation-Person",
                                "typeUserKey": "APOS-Types-Engagement",
                                "direction": "IN",
                                "attributes": [
                                    {
                                        "alias": "email",
                                        "userKey": "APOS-Types-Engagement-Attribute-Email"
                                    }
                                ],
                                "relations": [
                                    {
                                        "alias": "adm",
                                        "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                        "typeUserKey": "APOS-Types-AdmUnit",
                                        "direction": "OUT"
                                    }
                                ]
                            }
                        ]
                    },
                    "criteria": {
                        "type": "AND",
                        "criteria": [
                            {
                                "type": "MATCH",
                                "operator": "LIKE",
                                "left": {
                                    "source": "DEFINITION",
                                    "alias": "person.emp.email"
                                },
                                "right": {
                                    "source": "STATIC",
                                    "value": f"%{email}%"
                                }
                            }
                        ]
                    },
                    "projection": {
                        "identity": True,
                        "state": True,
                        "incomingTypeRelations": [
                            {
                                "userKey": "APOS-Types-User-TypeRelation-Engagement",
                                "projection": {
                                    "identity": True
                                }
                            },
                            {
                                "userKey": "APOS-Types-User-TypeRelation-Person",
                                "projection": {
                                    "identity": True
                                }
                            },
                            {
                                "userKey": "APOS-Types-Engagement-TypeRelation-Person",
                                "projection": {
                                    "identity": True,
                                    "state": True,
                                    "attributes": [
                                        "APOS-Types-Engagement-Attribute-Email"
                                    ],
                                    "typeRelations": [
                                        {
                                            "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                            "projection": {
                                                "identity": True
                                            }
                                        }
                                    ]
                                }
                            }
                        ]

                    }
                },
                "validDate": "NOW",
                "limit": 10
            }
        ]
    }
    url = base_url.rstrip("/") + "/api/object/graph-query"
    response = await client.post(url, json=graph_query)
    response.raise_for_status()
    response_json = response.json()

    graph_results = response_json.get("graphQueryResult") or []
    if not graph_results:
        return None

    instances = graph_results[0].get("instances") or []
    results = []

    for inst in instances:
        name = inst.get("identity", {}).get("name", None)
        email = None
        afdeling = None
        username = None

        for ref in inst.get("inTypeRefs", []):
            if ref.get("userKey") == "APOS-Types-Engagement-TypeRelation-Person":
                target = ref.get("targetObject", {})
                for attr in target.get("attributes", []):
                    if attr.get("userKey") == "APOS-Types-Engagement-Attribute-Email":
                        email = attr.get("value")
                for tref in target.get("typeRefs", []):
                    if tref.get("userKey") == "APOS-Types-Engagement-TypeRelation-AdmUnit":
                        afdeling = tref.get("targetObject", {}).get("identity", {}).get("name")
            elif ref.get("userKey") == "APOS-Types-User-TypeRelation-Person":
                username = ref.get("targetObject", {}).get("identity", {}).get("userKey")

        if not (afdeling and email):
            continue
        results.append({
            "name": name,
            "email": email,
            "unit": afdeling,
            "username": username
        })

    if not results:
        return None
    elif len(results) == 1:
        return results[0]
    else:
        logger.warning(f"Multiple users found in Delta for email '{email}'")
        return None
