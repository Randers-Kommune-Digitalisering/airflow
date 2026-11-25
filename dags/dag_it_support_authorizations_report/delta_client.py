from datetime import date

from airflow.hooks.base import BaseHook
from rkdigi.token_session import ManagedOAuth2Session


class DeltaClient:
    """Look up engagements and usernames for the IT support authorizations report."""

    def __init__(self, hook: BaseHook):
        self._session = ManagedOAuth2Session(
            token_url=hook.extra_dejson["token_url"],
            client_id=hook.login,
            client_secret=hook.password,
        )
        self._graph_query_url = f"{hook.host.rstrip('/')}/api/object/graph-query"

    def get_engagement_by_los_and_cpr(
        self, los: str | None, cpr: str, valid_date: date, active_only: bool = True
    ) -> list[dict]:
        """Return engagements on a date, optionally filtering by LOS and active state."""
        criteria = [{
            "type": "MATCH",
            "operator": "EQUAL",
            "left": {"source": "DEFINITION", "alias": "emp.per.cpr"},
            "right": {"source": "STATIC", "value": str(cpr)},
        }]
        relations = [{
            "alias": "per",
            "userKey": "APOS-Types-Engagement-TypeRelation-Person",
            "typeUserKey": "APOS-Types-Person",
            "direction": "OUT",
            "attributes": [{
                "alias": "cpr",
                "userKey": "APOS-Types-Person-Attribute-CPR",
            }],
        }]
        if los is not None:
            criteria.append({
                "type": "MATCH",
                "operator": "EQUAL",
                "left": {"source": "DEFINITION", "alias": "emp.adm.ou"},
                "right": {"source": "STATIC", "value": str(los)},
            })
            relations.insert(0, {
                "alias": "adm",
                "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                "typeUserKey": "APOS-Types-Engagement",
                "direction": "OUT",
                "attributes": [{
                    "alias": "ou",
                    "userKey": "APOS-Types-AdministrativeUnit-Attribute-OUDescription",
                }],
            })
        if active_only:
            criteria.append({
                "type": "MATCH",
                "operator": "EQUAL",
                "left": {"source": "DEFINITION", "alias": "emp.$state"},
                "right": {"source": "STATIC", "value": "STATE_ACTIVE"},
            })

        query = {
            "graphQueries": [{
                "graphQuery": {
                    "structure": {
                        "alias": "emp",
                        "userKey": "APOS-Types-Engagement",
                        "relations": relations,
                    },
                    "criteria": {"type": "AND", "criteria": criteria},
                    "projection": {
                        "incomingTypeRelations": [{
                            "userKey": "APOS-Types-User-TypeRelation-Engagement",
                            "projection": {"identity": True},
                        }],
                    },
                },
                "validDate": valid_date.strftime("%Y-%m-%d"),
                "limit": 5,
            }],
        }

        res = self._session.post(self._graph_query_url, json=query)
        res.raise_for_status()
        payload = res.json()
        if "graphQueryResult" not in payload:
            raise ValueError("Unexpected Delta response format")

        return [
            {
                "user": next(
                    (
                        ref.get("targetObject", {}).get("identity", {}).get("userKey")
                        for ref in instance.get("inTypeRefs", [])
                        if ref.get("userKey") == "APOS-Types-User-TypeRelation-Engagement"
                    ),
                    None,
                )
            }
            for instance in payload["graphQueryResult"][0].get("instances", [])
        ]
