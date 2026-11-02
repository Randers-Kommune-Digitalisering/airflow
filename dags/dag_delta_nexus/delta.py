from datetime import date, datetime, timezone

from rkdigi.token_session import ManagedOAuth2Session


class DeltaClient:
    """Query Delta engagement history and Nexus-related user details without Airflow."""

    def __init__(self, base_url: str, token_url: str, client_id: str, client_secret: str):
        self.base_url = base_url.rstrip("/")
        self.session = ManagedOAuth2Session(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
        )

    def _get_engagement_history(self, time_filter: dict) -> list[dict]:
        """Return engagement registrations for a validFrom date or from timestamp filter."""
        history_query = {
            "queryList": [
                {
                    **time_filter,
                    "includeAuthor": False,
                    "objType": "APOS-Types-Engagement",
                    "scopeList": ["BASIC", "STATE", "TYPE_RELATIONS"],
                }
            ],
            "transaction": "ALL",
        }

        self.session._acquire_token()
        response = self.session.post(f"{self.base_url}/api/object/history", json=history_query, timeout=300)
        response.raise_for_status()
        return response.json()["queryResultList"][0]["registrationList"]

    def get_engagement_uuids_valid_from(self, changes_date: date) -> list[str]:
        """Return engagement UUIDs effective on the date; registrations may repeat UUIDs."""
        registrations = self._get_engagement_history({"validFrom": changes_date.isoformat()})
        return [
            reg["objectUuid"] for reg in registrations
            if reg.get("validityDate") == changes_date.isoformat() and reg.get("objectUuid")
        ]

    def get_engagement_uuids_changed_since(self, from_time: datetime, valid_date: date) -> list[str]:
        """Return UUIDs changed since an aware instant and effective by valid_date; duplicates may occur."""
        if from_time.tzinfo is None or from_time.utcoffset() is None:
            raise ValueError("from_time must be timezone-aware")
        timestamp = from_time.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        registrations = self._get_engagement_history({"from": timestamp})
        return [
            reg["objectUuid"] for reg in registrations
            if reg.get("validityDate") and date.fromisoformat(reg["validityDate"]) <= valid_date and reg.get("objectUuid")
        ]

    def get_nexus_user_uuids_for_engagements(self, engagement_uuids: list[str], valid_date: date, prod: bool = True) -> list[str]:
        """
        Return unique UUIDs of users linked to active engagements with Nexus roles.

        :param engagement_uuids: Engagement UUIDs to look up in Delta.
        :param valid_date: Date on which the engagement and role graph is queried.
        :param prod: If True, match role names for Nexus prod, else match for Review
        :return: User UUIDs, with each matching user included once.
        """
        if not engagement_uuids:
            return []

        graph_query = {
            "graphQueries": [
                {
                    "computeAvailablePages": False,
                    "graphQuery": {
                        "structure": {
                            "alias": "employee",
                            "userKey": "APOS-Types-Engagement"
                        },
                        "criteria": {
                            "type": "OR",
                            "criteria": [
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {"source": "DEFINITION", "alias": "employee.$uuid"},
                                    "right": {"source": "STATIC", "value": uuid}
                                }
                                for uuid in engagement_uuids
                            ]
                        },
                        "projection": {
                            "state": True,
                            "incomingTypeRelations": [
                                {
                                    "userKey": "APOS-Types-User-TypeRelation-Engagement",
                                    "projection": {
                                        "state": True,
                                        "identity": True,
                                        "incomingTypeRelations": [
                                            {
                                                "userKey": "APOS-Types-AssignedIdmRole-TypeRelation-User",
                                                "projection": {
                                                    "state": True,
                                                    "typeRelations": [
                                                        {
                                                            "userKey": "APOS-Types-AssignedIdmRole-TypeRelation-IdmRoles",
                                                            "projection": {
                                                                "identity": True
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    },
                    "validDate": valid_date.isoformat(),
                    "offset": 0,
                    "limit": len(engagement_uuids) + 1
                }
            ]
        }
        res = self.session.post(self.base_url + "/api/object/graph-query", json=graph_query)
        res.raise_for_status()
        json_data = res.json()
        if json_data.get("result", {}).get("code") != "OK":
            raise ValueError("Delta graph query failed")

        results = json_data.get("graphQueryResult")
        if not isinstance(results, list) or len(results) != 1:
            raise ValueError("Expected one Delta graph query result")

        query_result = results[0]
        if query_result.get("result", {}).get("code") != "OK":
            raise ValueError("Delta graph query failed")

        instances = query_result.get("instances")
        if not isinstance(instances, list):
            raise ValueError("Delta graph query returned no instances list")

        user_uuids = []
        for instance in instances:
            if instance.get("state") != "STATE_ACTIVE":
                continue
            for user_ref in instance.get("inTypeRefs", []):
                if user_ref.get("refObjTypeUserKey") != "APOS-Types-User":
                    continue
                user = user_ref.get("targetObject") or {}
                if user.get("state") != "STATE_ACTIVE":
                    continue
                user_uuid = user.get("identity", {}).get("uuid")
                if not user_uuid:
                    continue
                has_matching_role = False
                for assigned_role_ref in user.get("inTypeRefs", []):
                    if assigned_role_ref.get("refObjTypeUserKey") != "APOS-Types-AssignedIdmRole":
                        continue
                    assigned_role = assigned_role_ref.get("targetObject") or {}
                    for role_ref in assigned_role.get("typeRefs", []):
                        if role_ref.get("refObjTypeUserKey") != "APOS-Types-IdmRole":
                            continue
                        role = role_ref.get("targetObject") or {}
                        role_name = role.get("identity", {}).get("name") or ""
                        normalized_name = role_name.casefold()
                        if "nexus" in normalized_name and ("review" not in normalized_name if prod else "review" in normalized_name):
                            has_matching_role = True
                            break
                    if has_matching_role:
                        break
                if has_matching_role:
                    user_uuids.append(user_uuid)

        return list(dict.fromkeys(user_uuids))

    def get_user_details(self, user_uuids: list[str], valid_date: date) -> list[dict[str, object]]:
        """
        Fetch users with their person and all linked engagements on the valid date.

        :param user_uuids: UUIDs of users to query in Delta.
        :param valid_date: Date for the user and engagement graph.
        :return: Records for users with active engagements, including name, attributes, adm units, and job function.
        """
        if not user_uuids:
            return []

        graph_query = {
            "graphQueries": [
                {
                    "computeAvailablePages": False,
                    "graphQuery": {
                        "structure": {
                            "alias": "user",
                            "userKey": "APOS-Types-User"
                        },
                        "criteria": {
                            "type": "OR",
                            "criteria": [
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {"source": "DEFINITION", "alias": "user.$uuid"},
                                    "right": {"source": "STATIC", "value": uuid}
                                }
                                for uuid in user_uuids
                            ]
                        },
                        "projection": {
                            "state": True,
                            "identity": True,
                            "attributes": [
                                "APOS-Types-User-Attribute-UserPrincipalName",
                                "APOS-Types-User-Attribute-ADGUID",
                                "APOS-Types-User-Attribute-UserName"
                            ],
                            "typeRelations": [
                                {
                                    "userKey": "APOS-Types-User-TypeRelation-Person",
                                    "projection": {
                                        "state": True,
                                        "identity": True,
                                        "attributes": [
                                            "APOS-Types-Person-Attribute-SurnameAndName"
                                        ],
                                        "incomingTypeRelations": [
                                            {
                                                "userKey": "APOS-Types-Engagement-TypeRelation-Person",
                                                "projection": {
                                                    "state": True,
                                                    "identity": True,
                                                    "typeRelations": [
                                                        {
                                                            "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                                            "projection": {
                                                                "identity": True,
                                                                "children": {
                                                                    "identity": True,
                                                                    "children": {"identity": True}
                                                                }
                                                            }
                                                        },
                                                        {
                                                            "userKey": "APOS-Types-Engagement-TypeRelation-Jobfunctions",
                                                            "projection": {"state": True, "identity": True}
                                                        },
                                                        {
                                                            "userKey": "APOS-Types-Engagement-TypeRelation-AdditionalAssociation",
                                                            "projection": {
                                                                "state": True,
                                                                "typeRelations": [
                                                                    {
                                                                        "userKey": "APOS-Types-AdditionalAssociation-TypeRelation-AdmUnit",
                                                                        "projection": {
                                                                            "identity": True,
                                                                            "children": {
                                                                                "identity": True,
                                                                                "children": {"identity": True}
                                                                            }
                                                                        }
                                                                    }
                                                                ]
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    },
                    "validDate": valid_date.isoformat(),
                    "offset": 0,
                    "limit": len(user_uuids) + 1
                }
            ]
        }
        res = self.session.post(self.base_url + "/api/object/graph-query", json=graph_query)
        res.raise_for_status()
        json_data = res.json()
        if json_data.get("result", {}).get("code") != "OK":
            raise ValueError("Delta graph query failed")

        results = json_data.get("graphQueryResult")
        if not isinstance(results, list) or len(results) != 1:
            raise ValueError("Expected one Delta graph query result")

        query_result = results[0]
        if query_result.get("result", {}).get("code") != "OK":
            raise ValueError("Delta graph query failed")

        instances = query_result.get("instances")
        if not isinstance(instances, list):
            raise ValueError("Delta graph query returned no instances list")

        user_details = []
        for instance in instances:
            adm_units = {}
            direct_primary = None
            association_primary = None
            first_adm_unit = None
            job_function: str | None = None
            name: str | None = None
            has_active_engagement = False
            for person_ref in instance.get("typeRefs", []):
                if person_ref.get("refObjTypeUserKey") != "APOS-Types-Person":
                    continue
                person = person_ref.get("targetObject") or {}
                for attribute in person.get("attributes", []):
                    if attribute.get("userKey") == "APOS-Types-Person-Attribute-SurnameAndName" and name is None:
                        name = attribute.get("value") or None
                for engagement_ref in person.get("inTypeRefs", []):
                    if engagement_ref.get("refObjTypeUserKey") != "APOS-Types-Engagement":
                        continue
                    engagement = engagement_ref.get("targetObject") or {}
                    if engagement.get("state") != "STATE_ACTIVE":
                        continue
                    has_active_engagement = True
                    for relation in engagement.get("typeRefs", []):
                        target = relation.get("targetObject") or {}
                        if relation.get("refObjTypeUserKey") == "APOS-Types-Jobfunction":
                            if job_function is None and target.get("state") == "STATE_ACTIVE":
                                job_function = target.get("identity", {}).get("userKey") or None
                            continue
                        is_additional_association = relation.get("refObjTypeUserKey") == "APOS-Types-AdditionalAssociation"
                        if relation.get("refObjTypeUserKey") == "APOS-Types-AdministrativeUnit":
                            adm_roots = [target]
                        elif is_additional_association:
                            adm_roots = [
                                adm_ref.get("targetObject") or {}
                                for adm_ref in target.get("typeRefs", [])
                                if adm_ref.get("refObjTypeUserKey") == "APOS-Types-AdministrativeUnit"
                            ]
                        else:
                            continue

                        for adm_root in adm_roots:
                            has_leaf_child = False
                            for child in adm_root.get("childrenObjects", []):
                                if child.get("childrenObjects", []):
                                    continue
                                identity = child.get("identity") or {}
                                if identity.get("uuid") and identity.get("name"):
                                    has_leaf_child = True
                                    adm_units[identity["uuid"]] = {
                                        "uuid": identity["uuid"],
                                        "name": identity["name"]
                                    }
                            parent_identity = adm_root.get("identity") or {}
                            if parent_identity.get("uuid") and parent_identity.get("name"):
                                parent = {"uuid": parent_identity["uuid"], "name": parent_identity["name"]}
                                if has_leaf_child:
                                    adm_units[parent["uuid"]] = parent
                                if first_adm_unit is None:
                                    first_adm_unit = parent
                                if has_leaf_child and is_additional_association and association_primary is None:
                                    association_primary = parent
                                elif has_leaf_child and not is_additional_association and direct_primary is None:
                                    direct_primary = parent
            if not has_active_engagement:
                continue
            user_details.append({
                "user": {
                    attribute["userKey"].rsplit("-", 1)[-1]: attribute.get("value")
                    for attribute in instance.get("attributes", [])
                    if attribute.get("userKey")
                },
                "adm_units": list(adm_units.values()),
                "primary_adm_unit": association_primary or direct_primary or first_adm_unit,
                "name": name,
                "job_function": job_function
            })
        return user_details
