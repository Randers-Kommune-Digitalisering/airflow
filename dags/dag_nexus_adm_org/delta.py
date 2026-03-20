from airflow.providers.http.hooks.http import BaseHook

from rkdigi.token_session import ManagedOAuth2Session


class DeltaClient():
    def __init__(self, delta_hook: BaseHook, top_uuid: str):
        self.top_uuid = top_uuid
        self.session = ManagedOAuth2Session(
            token_url=delta_hook.extra_dejson.get("token_url"),
            client_id=delta_hook.login,
            client_secret=delta_hook.password
        )
        self.graph_query_url = f"{delta_hook.host.rstrip('/')}/api/object/graph-query"

    def _get_adm_org_units_recursive(self, instances: dict, adm_org_list: list):
        for adm in instances:
            if 'identity' in adm:
                if 'uuid' in adm['identity']:
                    adm_org_list.append(adm['identity']['uuid'])
            if 'childrenObjects' in adm:
                for child in adm['childrenObjects']:
                    self._get_adm_org_units_recursive([child], adm_org_list)

    def _check_has_employees_and_add_sub_adm_org_units(self, adm_org_list):
        adm_org_dict = {}
        for adm_org in adm_org_list:
            graph_query = {
                "graphQueries": [
                    {
                        "parameterMap": {
                            "admUuid": adm_org
                        },
                        "computeAvailablePages": False,
                        "graphQuery": {
                            "structure": {
                                "alias": "adminUnit",
                                "userKey": "APOS-Types-AdministrativeUnit",
                                "relations": [
                                    {
                                        "alias": "employee",
                                        "title": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                        "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                        "typeUserKey": "APOS-Types-Engagement",
                                        "direction": "IN",
                                        "relations": [
                                            {
                                                "alias": "adm",
                                                "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                                "typeUserKey": "APOS-Types-Engagement",
                                                "direction": "IN"
                                            }
                                        ]
                                    }
                                ]
                            },
                            "parameters": [
                                {
                                    "key": "admUuid",
                                    "name": "Admin Org uuid"
                                }
                            ],
                            "criteria": {
                                "type": "AND",
                                "criteria": [
                                    {
                                        "type": "EXIST",
                                        "alias": "adminUnit.employee"
                                    },
                                    {
                                        "type": "MATCH",
                                        "operator": "EQUAL",
                                        "left": {
                                            "source": "DEFINITION",
                                            "alias": "adminUnit.$uuid"
                                        },
                                        "right": {
                                            "source": "PARAMETER",
                                            "key": "admUuid"
                                        }
                                    },
                                    {
                                        "type": "MATCH",
                                        "operator": "EQUAL",
                                        "left": {
                                            "source": "DEFINITION",
                                            "alias": "adminUnit.$state"
                                        },
                                        "right": {
                                            "source": "STATIC",
                                            "value": "STATE_ACTIVE"
                                        }
                                    }
                                ]
                            },
                            "projection": {
                                "children": {
                                    "children": {
                                    }
                                }
                            }
                        },
                        "validDate": "NOW",
                        "offset": 0,
                        "limit": 1
                    }
                ]
            }
            res = self.session.post(self.graph_query_url, json=graph_query)
            res.raise_for_status()
            data = res.json()
            if len(data['graphQueryResult'][0]['instances']) > 0:
                sub_adm_orgs = []
                self._get_adm_org_units_recursive(data['graphQueryResult'][0]['instances'], sub_adm_orgs)
                sub_adm_orgs = [e for e in sub_adm_orgs if e != adm_org]
                adm_org_dict[adm_org] = sub_adm_orgs

        # Deletes adm. org. units with sub adm. org. units with employees
        keys_to_remove = []
        for key, value in adm_org_dict.items():
            for sub_adm_org in value:
                if sub_adm_org in adm_org_dict.keys() and key not in keys_to_remove:
                    keys_to_remove.append(key)
                    break

        for key in keys_to_remove:
            adm_org_dict.pop(key)

        return adm_org_dict

    def get_adm_org_list(self):
        graph_query = {
            "graphQueries": [
                {
                    "parameterMap": {
                        "admUuid": self.top_uuid
                    },
                    "computeAvailablePages": False,
                    "graphQuery": {
                        "structure": {
                            "alias": "adm",
                            "userKey": "APOS-Types-AdministrativeUnit",
                            "relations": [
                                {
                                    "alias": "adm",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                    "typeUserKey": "APOS-Types-Engagement",
                                    "direction": "IN",
                                    "relations": [
                                        {
                                            "alias": "adm",
                                            "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                            "typeUserKey": "APOS-Types-Engagement",
                                            "direction": "IN",
                                            "relations": [
                                                {
                                                    "alias": "adm",
                                                    "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                                    "typeUserKey": "APOS-Types-Engagement",
                                                    "direction": "IN",
                                                    "relations": [
                                                        {
                                                            "alias": "adm",
                                                            "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                                            "typeUserKey": "APOS-Types-Engagement",
                                                            "direction": "IN",
                                                            "relations": [
                                                                {
                                                                    "alias": "adm",
                                                                    "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                                                    "typeUserKey": "APOS-Types-Engagement",
                                                                    "direction": "IN"
                                                                }
                                                            ]
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                        "parameters": [
                            {
                                "key": "admUuid",
                                "name": "Admin Org uuid"
                            }
                        ],
                        "criteria": {
                            "type": "AND",
                            "criteria": [
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "adm.$uuid"
                                    },
                                    "right": {
                                        "source": "PARAMETER",
                                        "key": "admUuid"
                                    }
                                },
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "adm.$state"
                                    },
                                    "right": {
                                        "source": "STATIC",
                                        "value": "STATE_ACTIVE"
                                    }
                                }
                            ]
                        },
                        "projection": {
                            "children": {
                                "children": {
                                    "children": {
                                        "children": {
                                            "children": {}
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "validDate": "NOW",
                    "offset": 0,
                    "limit": 1
                }
            ]
        }

        res = self.session.post(self.graph_query_url, json=graph_query)
        res.raise_for_status()
        data = res.json()
        adm_org_list = []
        self._get_adm_org_units_recursive(data['graphQueryResult'][0]['instances'], adm_org_list)

        final_adm_org_list = self._check_has_employees_and_add_sub_adm_org_units(adm_org_list)

        return final_adm_org_list
