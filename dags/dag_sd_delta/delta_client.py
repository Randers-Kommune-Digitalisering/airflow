import logging
from datetime import date
from pathlib import Path

from airflow.hooks.base import BaseHook
from rkdigi.token_session import ManagedOAuth2Session


logger = logging.getLogger(__name__)


class DeltaClient:
    _instance: "DeltaClient | None" = None

    def __init__(
        self,
        hook: BaseHook
    ):
        """
        Client for Delta API (singleton)
        """
        if getattr(self, "_initialized", False):
            return

        self.session = ManagedOAuth2Session(
            token_url=hook.extra_dejson["token_url"],
            client_id=hook.login,
            client_secret=hook.password
        )
        self._graph_query_url = f"{hook.host.rstrip('/')}/api/object/graph-query"
        self._query_url = f"{hook.host.rstrip('/')}/api/object/query"
        self._update_url = f"{hook.host.rstrip('/')}/api/object/update"
        self._sd_integration_url = f"{hook.host.rstrip('/')}/integration-sd/import/run-process"
        self._initialized = True

    def __new__(cls, hook: BaseHook):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_active_engagement_id(self, engagement_key: str, valid_date: date) -> str | None:
        """Fetches active engagement uuid from Delta by key. Returns None if no active engagement found."""
        query = {
            "queries": [
                {
                    "criteria": {
                        "identity": {
                            "objUserKey": engagement_key
                        }
                    },
                    "typeFilter": {
                        "userKey": "APOS-Types-Engagement"
                    },
                    "resultLimit": {
                        "scopeLimitList": [
                            "STATE",
                            "IDENTITY_PROPERTIES"
                        ],
                        "limit": 10,
                        "offset": 0
                    },
                    "validDate": valid_date.strftime("%Y-%m-%d")
                }
            ]
        }

        res = self.session.post(self._query_url, json=query)
        res.raise_for_status()
        if "queryResults" not in res.json():
            raise ValueError(f"Unexpected response format: {res.text}")

        payload = res.json()
        active_uuids: list[str] = []
        for instance in payload["queryResults"][0].get("instances", []):
            if instance.get("state") != "STATE_ACTIVE":
                continue
            uuid = (instance.get("identity") or {}).get("uuid")
            if uuid:
                active_uuids.append(uuid)

        if len(active_uuids) > 1:
            raise ValueError(
                f"Multiple active engagements found for key {engagement_key}: {active_uuids}"
            )
        elif len(active_uuids) == 1:
            return active_uuids[0]
        return None

    def deactivate_engagement(self, uuid: str, from_date: date) -> bool:
        query = {
            "transaction": "ALL",
            "objectUpdateList": [
                {
                    "scope": {
                        "flags": [
                            "STATE"
                        ]
                    },
                    "instance": {
                        "validityInterval": {
                            "from": from_date.strftime("%Y-%m-%d"),
                            "to": "PLUS_INF"
                        },
                        "objTypeUserKey": "APOS-Types-Engagement",
                        "identity": {
                            "uuid": uuid

                        },
                        "state": "STATE_INACTIVE"
                    }
                }
            ]
        }

        res = self.session.post(self._update_url, json=query)
        res.raise_for_status()
        res_json = res.json()

        if res_json.get('result', {}).get('code', None) == 'OK':
            return True
        else:
            logger.error(f"Failed to deactivate engagement {uuid}: {res.text}")
            return False

    def upload_sd_excel_file(self, file_path: str) -> str:
        """Uploads an Excel file from a local path to Delta and returns the process instance id."""
        path = Path(file_path)
        file_content = path.read_bytes()
        multipart_form_data = {
            'file': (path.name, file_content, 'application/vnd.ms-excel')
        }

        res = self.session.post(
            self._sd_integration_url,
            files=multipart_form_data
        )
        res.raise_for_status()
        res_json = res.json()

        if res_json.get("result", {}).get("code") != "OK":
            raise ValueError(f"Unexpected Delta upload response: {res_json}")

        process_instance_id = (res_json.get("processInstanceDto") or {}).get("id")
        if not process_instance_id:
            raise ValueError(f"Missing process instance id in Delta upload response: {res_json}")

        return str(process_instance_id)

    def get_engagement_by_los_and_cpr(self, los: str, cpr: str, valid_date: date) -> list[dict]:
        """Fetches engagement from Delta by LOS and CPR. Returns None if no active engagement found."""
        query = {
            "graphQueries": [
                {
                    "graphQuery": {
                        "structure": {
                            "alias": "emp",
                            "userKey": "APOS-Types-Engagement",
                            "relations": [
                                {
                                    "alias": "adm",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                    "typeUserKey": "APOS-Types-Engagement",
                                    "direction": "OUT",
                                    "attributes": [
                                        {
                                            "alias": "ou",
                                            "userKey": "APOS-Types-AdministrativeUnit-Attribute-OUDescription"
                                        }
                                    ]
                                },
                                {
                                    "alias": "per",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-Person",
                                    "typeUserKey": "APOS-Types-Person",
                                    "direction": "OUT",
                                    "attributes": [
                                        {
                                            "alias": "cpr",
                                            "userKey": "APOS-Types-Person-Attribute-CPR"
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
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "emp.adm.ou"
                                    },
                                    "right": {
                                        "source": "STATIC",
                                        "value": str(los)
                                    }
                                },
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "emp.per.cpr"
                                    },
                                    "right": {
                                        "source": "STATIC",
                                        "value": str(cpr)
                                    }
                                },
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "emp.$state"
                                    },
                                    "right": {
                                        "source": "STATIC",
                                        "value": "STATE_ACTIVE"
                                    }
                                }
                            ]
                        },
                        "projection": {
                            "identity": True,
                            "state": True,
                            "attributes": [
                                "APOS-Types-Engagement-Attribute-SDUnitCode"
                            ],
                            "typeRelations": [
                                {
                                    "userKey": "APOS-Types-Engagement-TypeRelation-Person",
                                    "projection": {
                                        "attributes": [
                                            "APOS-Types-Person-Attribute-CPR"
                                        ]
                                    }
                                }
                            ],
                            "incomingTypeRelations": [
                                {
                                    "userKey": "APOS-Types-User-TypeRelation-Engagement",
                                    "projection": {
                                        "identity": True
                                    }
                                }
                            ]
                        }
                    },
                    "validDate": valid_date.strftime("%Y-%m-%d"),
                    "limit": 5
                }
            ]
        }

        res = self.session.post(self._graph_query_url, json=query)
        res.raise_for_status()
        if "graphQueryResult" not in res.json():
            raise ValueError(f"Unexpected response format: {res.text}")

        payload = res.json()
        employments = []
        for instance in payload["graphQueryResult"][0].get("instances", []):
            institution_id = instance.get("identity", {}).get("userKey", "").split(".")[0]
            employment_id = instance.get("identity", {}).get("userKey", "").split(".")[1]
            department_id = next(
                (
                    attr.get("value") 
                    for attr in instance.get("attributes", [])
                    if attr.get("userKey") == "APOS-Types-Engagement-Attribute-SDUnitCode"
                ),
                None
            )
            user = next(
                (
                    ref.get("targetObject", {}).get("identity", {}).get("userKey")
                    for ref in instance.get("inTypeRefs", [])
                    if ref.get("userKey") == "APOS-Types-User-TypeRelation-Engagement"
                ),
                None,
            )
            employments.append({
                "institution_id": institution_id,
                "employment_id": employment_id,
                "department_id": department_id,
                "user": user
            })
        return employments
