import logging
from datetime import date
from collections import defaultdict
from airflow.hooks.base import BaseHook

from rkdigi.token_session import ManagedOAuth2Session


logger = logging.getLogger(__name__)


class DeltaClient:
    _created = False

    def __init__(
        self,
        hook: BaseHook,
        adm_org_dict: dict,
        position_types_to_import: list,
        job_functions_to_import: list,
        changes_date: date
    ):
        """
        Client for fetching employee changes from Delta and filter for changes relevant for Nexus.

        :param hook: Airflow connection with creds to connect to Delta.
        :param adm_org_dict: Dictionary with administrative units (arbejdsplads) relevant for Nexus users.
        :param position_types_to_import: List of position types (stillingsbetegnelse) relevant for Nexus employees.
        :param job_functions_to_import: List of job functions (jobfunktion) relevant for Nexus substitutes (vikarer).
        :param changes_date: Date to get changes for. Format: YYYY-MM-DD.
            (the day the changes are valid from (inclusive), not the day the changes were made)
        """
        if DeltaClient._created:
            raise Exception("DeltaClient can only be created once.")
        DeltaClient._created = True

        self.session = ManagedOAuth2Session(
            token_url=hook.extra_dejson["token_url"],
            client_id=hook.login,
            client_secret=hook.password
        )
        self._adm_org_dict = adm_org_dict
        self._position_types_to_import = position_types_to_import
        self._job_functions_to_import = job_functions_to_import
        self._changes_date = changes_date
        self._graph_query_url = f"{hook.host.rstrip('/')}/api/object/graph-query"
        self._query_url = f"{hook.host.rstrip('/')}/api/object/query"
        self._history_url = f"{hook.host.rstrip('/')}/api/object/history"

    def _filter_relevant_time_or_type_of_change(self, employee_delta_dict: dict, changes_date: date) -> bool:
        """
        Filter for changes that are relevant for Nexus permissions, which are changes in state, job function, position or organizational unit (arbejdsplads) connected to the employee.

        :param employee_delta_dict: Dictionary containing delta changes information. In the form:
            {
                .. other keys ..
                "objectType": "APOS-Types-Engagement",
                "operation": <"UPDATE" or "OPRETTET">,
                "validityDate": <date in format YYYY-MM-DD>,
                "stateBiList": [],
                "typeRefBiList": [],
                "closedStateBiList": [],
                "closedTypeRefBiList": []
            }
        :param changes_date: Date to filter changes by.
        :return: bool (True if the changes  of relevant type, False otherwise).
        """
        changes_to_look_for = [
            'APOS-Types-Engagement-TypeRelation-AdmUnit',  # AdmUnit (arbejdsplads)
            'APOS-Types-Engagement-TypeRelation-Position',  # Position (stillingsbetegnelse)
            'APOS-Types-Engagement-TypeRelation-AdditionalAssociation',  # AdditionalAssociation (forhold ved intern vikar, der indeholder arbejdsplads)
            'APOS-Types-Engagement-TypeRelation-Jobfunctions'  # Jobfunctions (jobfunktion, brugt for vikarer), ekstra stillingsbetegnelse
        ]

        # Engagement (employment) has changed state
        state_change = True if employee_delta_dict.get('stateBiList', []) else False
        # Engagement (employment) has gotten a relevant type added on the changes date
        added_on_date = [obj for obj in employee_delta_dict.get('typeRefBiList', []) if obj.get('validityInterval', {}).get('from', '') == changes_date.strftime("%Y-%m-%d") and obj.get('value', {}).get('userKey') in changes_to_look_for]
        # Engagement (employment) has gotten a relevant type removed on the changes date
        removed_on_date = [obj for obj in employee_delta_dict.get('closedTypeRefBiList', []) if obj.get('value', {}).get('userKey') in changes_to_look_for]
        return any([state_change, added_on_date, removed_on_date])

    def _unpack_employee_details(self, employee_details_response: dict, requested_uuids: list[str]) -> list[dict]:
        """
        Unpack employee details response from Delta graph query into a more accessible format.
        Employees / engagements without a user are filters out. Employees / engagements with multiple users will be associated with the first user found.
        :param employee_details_response: Response from Delta graph query for employee details.
        :param requested_uuids: List of UUIDs that were requested.
        :return: List of dictionaries with employee details in a more accessible format.
        """
        instances = employee_details_response['graphQueryResult'][0]['instances']

        if len(instances) != len(requested_uuids):
            returned_uuids = {instance.get('identity', {}).get('uuid') for instance in instances}
            requested_set = set(requested_uuids)
            missing = requested_set - returned_uuids
            extra = returned_uuids - requested_set
            raise ValueError(
                'Number of returned engagements from graph query does not match number of requested engagements.\n'
                f'Number requested: {len(requested_uuids)}, number returned: {len(instances)}.\n' + (f'Missing UUIDs: {missing}\n' if missing else '') + (f'Extra UUIDs: {extra}\n' if extra else '')
            )

        unpacked_details = []
        for instance in instances:
            uuid, state = instance.get('identity', {}).get('uuid', None), instance.get('state', None)
            if not state or not uuid:
                raise ValueError(f'Missing state or uuid in employee details response. UUID: {uuid}, state: {state}.')
            upn, user, cpr, name, org, postion, jobs_add, jobs_remove, aa_orgs_add, aa_orgs_remove = None, None, None, None, None, None, [], [], [], []
            for type_ref in instance.get('typeRefs', []) + instance.get('inTypeRefs', []):
                if type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-AdditionalAssociation':
                    if type_ref.get('targetObject', {}).get('state', '') == 'STATE_ACTIVE':
                        aa_type_refs = type_ref.get('targetObject', {}).get('typeRefs', [])
                        if aa_type_refs:
                            aa_orgs_add.append(aa_type_refs[0].get('targetObject', {}).get('identity', {}).get('uuid', None))
                    elif type_ref.get('targetObject', {}).get('state', '') == 'STATE_INACTIVE':
                        aa_type_refs = type_ref.get('targetObject', {}).get('typeRefs', [])
                        if aa_type_refs:
                            aa_orgs_remove.append(aa_type_refs[0].get('targetObject', {}).get('identity', {}).get('uuid', None))
                elif type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-Jobfunction':
                    if type_ref.get('targetObject', {}).get('state', '') == 'STATE_ACTIVE':
                        jobs_add.append(type_ref.get('targetObject', {}).get('identity', {}).get('userKey', None))
                    elif type_ref.get('targetObject', {}).get('state', '') == 'STATE_INACTIVE':
                        jobs_remove.append(type_ref.get('targetObject', {}).get('identity', {}).get('userKey', None))
                elif type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-AdministrativeUnit':
                    org = type_ref.get('targetObject', {}).get('identity', {}).get('uuid', None)
                elif type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-PositionType':
                    postion = type_ref.get('targetObject', {}).get('identity', {}).get('userKey', None)
                elif type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-Person':
                    cpr = type_ref.get('targetObject', {}).get('identity', {}).get('userKey', None)
                    name = type_ref.get('targetObject', {}).get('identity', {}).get('name', None)
                elif type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-User':
                    if user:
                        logger.warning(
                            f'Multiple users found for employee: {uuid}, just using the first one found. User 1: {user}.'
                            f' Discarded User: {type_ref.get("targetObject", {}).get("identity", {}).get("userKey", None)}'
                        )
                        # TODO: raise exception ? (NB: users not associated with Nexus are also handled here)
                        continue
                    else:
                        user = type_ref.get('targetObject', {}).get('identity', {}).get('userKey', None)
                        for att in type_ref.get('targetObject', {}).get('attributes', []):
                            if att.get('userKey', '') == 'APOS-Types-User-Attribute-UserPrincipalName':
                                upn = att.get('value', None)

            unpacked_details.append({'uuid': uuid, 'state': state, 'user': user, 'upn': upn, 'cpr': cpr, 'name': name, 'position': postion, 'org': org, 'jobs_add': jobs_add, 'jobs_remove': jobs_remove, 'aa_orgs_add': aa_orgs_add, 'aa_orgs_remove': aa_orgs_remove})

        return unpacked_details

    def _get_engagements_details(self, engagement_uuids: list[str]) -> list[dict]:
        """
        Get details for engagements with given UUIDs.

        :param engagement_uuids: List of engagement UUIDs to get details for.
        :return: List of dictionaries with engagement details.
        """
        graph_query = {
            "graphQueries": [
                {
                    "computeAvailablePages": False,
                    "graphQuery": {
                        "structure": {
                            "alias": "employee",
                            "userKey": "APOS-Types-Engagement",
                            "relations": [
                                {
                                    "alias": "aa",
                                    "title": "APOS-Types-Engagement-TypeRelation-AdditionalAssociation",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-AdditionalAssociation",
                                    "typeUserKey": "APOS-Types-AdditionalAssociation",
                                    "direction": "OUT",
                                    "relations": [
                                        {
                                            "alias": "aaAdmOrg",
                                            "title": "APOS-Types-AdditionalAssociation-TypeRelation-AdmUnit",
                                            "userKey": "APOS-Types-AdditionalAssociation-TypeRelation-AdmUnit",
                                            "typeUserKey": "APOS-Types-AdmUnit",
                                            "direction": "OUT"
                                        }
                                    ]
                                },
                                {
                                    "alias": "admOrg",
                                    "title": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                    "typeUserKey": "APOS-Types-AdmUnit",
                                    "direction": "OUT"
                                },
                                {
                                    "alias": "position",
                                    "title": "APOS-Types-Engagement-TypeRelation-Position",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-Position",
                                    "typeUserKey": "APOS-Types-Position",
                                    "direction": "OUT"
                                },
                                {
                                    "alias": "jobfunction",
                                    "title": "APOS-Types-Engagement-TypeRelation-Jobfunctions",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-Jobfunctions",
                                    "typeUserKey": "APOS-Types-Jobfunction",
                                    "direction": "OUT"
                                },
                                {
                                    "alias": "user",
                                    "title": "APOS-Types-User-TypeRelation-Engagement",
                                    "userKey": "APOS-Types-User-TypeRelation-Engagement",
                                    "typeUserKey": "APOS-Types-User",
                                    "direction": "IN"
                                },
                                {
                                    "alias": "person",
                                    "title": "APOS-Types-Engagement-TypeRelation-Person",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-Person",
                                    "typeUserKey": "APOS-Types-Person",
                                    "direction": "OUT"
                                }
                            ]
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
                            "typeRelations": [
                                {"userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit", "projection": {}},
                                {"userKey": "APOS-Types-Engagement-TypeRelation-Position", "projection": {"identity": True}},
                                {"userKey": "APOS-Types-Engagement-TypeRelation-Jobfunctions", "projection": {"state": True, "identity": True}},
                                {"userKey": "APOS-Types-Engagement-TypeRelation-Person", "projection": {"state": True, "identity": True}},
                                {
                                    "userKey": "APOS-Types-Engagement-TypeRelation-AdditionalAssociation",
                                    "projection": {
                                        "state": True,
                                        "typeRelations": [
                                            {"userKey": "APOS-Types-AdditionalAssociation-TypeRelation-AdmUnit", "projection": {}}
                                        ]
                                    }
                                }
                            ],
                            "incomingTypeRelations": [
                                {
                                    "userKey": "APOS-Types-User-TypeRelation-Engagement",
                                    "projection": {
                                        "identity": True,
                                        "attributes": ["APOS-Types-User-Attribute-UserPrincipalName"]
                                    }
                                }
                            ]
                        }
                    },
                    "validDate": self._changes_date.strftime("%Y-%m-%d"),
                    "offset": 0,
                    "limit": len(engagement_uuids) + 1  # Engagement UUIDs plus one (for checking return length)
                }
            ]
        }
        res = self.session.post(self._graph_query_url, json=graph_query)
        res.raise_for_status()
        data = res.json()
        return self._unpack_employee_details(employee_details_response=data, requested_uuids=engagement_uuids)

    def get_employment_changes(self) -> list[dict]:
        """
        Get employee changes from Delta, filter for changes relevant for Nexus and return relevant details for those changes.

        return: List of dictionaries with details for employees with relevant changes. In the form:
        [
            {
                'upn': <user principal name>,
                'user': <user / DQ-number>,
                'cpr': <cpr number>,
                'name': <employee name>,
                'organizations': [<list of associated organizational units (arbejdsplads) as UUIDs>],
                'job_title': <job title, only set for employees with job function>,
            },
            ...
        ]
        """
        history_query = {
            "queryList": [
                {
                    "validFrom": self._changes_date.strftime("%Y-%m-%d"),
                    "includeAuthor": False,
                    "objType": "APOS-Types-Engagement",
                    "scopeList": [
                        "BASIC",
                        "STATE",
                        "TYPE_RELATIONS"
                    ]
                }
            ],
            "transaction": "ALL"
        }

        # Get changes in the past valid from changes date
        self.session._acquire_token()
        res_employee_changes_valid_date = self.session.post(self._history_url, json=history_query, timeout=300)
        res_employee_changes_valid_date.raise_for_status()
        data_employee_changes_valid_date = res_employee_changes_valid_date.json()

        # Get changes made on the changes date
        history_query['queryList'][0]['from'] = f"{history_query['queryList'][0].pop('validFrom')}T00:00:00.000Z"
        self.session._acquire_token()
        res_employee_changes_change_date = self.session.post(self._history_url, json=history_query, timeout=300)
        res_employee_changes_change_date.raise_for_status()
        data_employee_changes_change_date = res_employee_changes_change_date.json()

        # Filter changes to only keep the ones valid from exactly on the changes date
        all_employee_changes = [
            reg
            for reg in data_employee_changes_valid_date['queryResultList'][0]['registrationList']
            if reg.get('validityDate') == self._changes_date.strftime("%Y-%m-%d")
        ]

        # Filter changes made on the changes date to only keep the ones valid from before or on the changes date
        for reg in data_employee_changes_change_date['queryResultList'][0]['registrationList']:
            if date.fromisoformat(reg['validityDate']) <= self._changes_date:
                all_employee_changes.append(reg)

        # Filter for types of changes relevant for Nexus permissions
        employees_with_relevant_changes = [
            change['objectUuid']
            for change in all_employee_changes
            if self._filter_relevant_time_or_type_of_change(change, self._changes_date)
        ]

        # Remove duplicates
        employees_with_relevant_changes = list(set(employees_with_relevant_changes))

        employments_with_details = self._get_engagements_details(employees_with_relevant_changes)
        employee_orgs_dict = defaultdict(list)
        employee_detail_dict = defaultdict(dict)
        for employee_details in employments_with_details:
            if employee_details['state'] == 'STATE_ACTIVE':
                orgs = []
                if any([job in self._job_functions_to_import for job in employee_details['jobs_add']]):
                    # Internal substitutes (interne vikarer) has an additional associations and job functions
                    if employee_details['aa_orgs_add']:
                        for aa_org in employee_details['aa_orgs_add']:
                            if aa_org in self._adm_org_dict.keys():
                                orgs = orgs + [aa_org] + self._adm_org_dict[aa_org]
                        employee_details['set_job_title'] = True
                    # External substitutes (eksterne vikarer) has no additional associations but job functions and an organization (administrativ enhed)
                    elif employee_details['org'] in self._adm_org_dict.keys():
                        orgs = orgs + [employee_details['org']] + self._adm_org_dict[employee_details['org']]
                        employee_details['set_job_title'] = True

                if employee_details['position'] in self._position_types_to_import:
                    # Regular employees has a position and an organization (administrativ enhed), internal substitutes (interne vikarer) can be regular employees
                    if employee_details['org'] in self._adm_org_dict.keys():
                        orgs = orgs + [employee_details['org']] + self._adm_org_dict[employee_details['org']]

                # Filter if connected to Nexus (has relevant job function, position or organization (administrativ enhed))
                if any([any([job in self._job_functions_to_import for job in employee_details['jobs_add']]),
                        any([org in self._adm_org_dict.keys() for org in employee_details['aa_orgs_add']]),
                        any([job in self._job_functions_to_import for job in employee_details['jobs_remove']]),
                        any([org in self._adm_org_dict.keys() for org in employee_details['aa_orgs_remove']]),
                        (employee_details['position'] in self._position_types_to_import and employee_details['org'] in self._adm_org_dict.keys())]):
                    employee_orgs_dict[employee_details['uuid']].extend(orgs)
                    employee_detail_dict[employee_details['uuid']].update(employee_details)

        # Set relavant details/changes needed for setting Nexus information
        employees_to_change = []
        for e in employee_orgs_dict:
            employee = {
                'upn': employee_detail_dict[e]['upn'],
                'user': employee_detail_dict[e]['user'],
                'cpr': employee_detail_dict[e]['cpr'],
                'name': employee_detail_dict[e]['name'],
                'organizations': employee_orgs_dict[e]
            }
            if len(employee_detail_dict[e]['jobs_add']) == 1 and employee_detail_dict[e]['set_job_title']:
                employee['job_title'] = employee_detail_dict[e]['jobs_add'][0]
            employees_to_change.append(employee)

        # Ensure all employees have both 'user' and 'upn' set to a value
        for emp in employees_to_change[:]:  # shallow copy of list to allow modification while iterating
            if not emp.get('user') or not emp.get('upn'):
                #  TODO: How to handle employees missing user in Delta (comtains DQ-number and UPN)
                logger.warning(f"Employee missing 'user' or 'upn', skipping employee. Employee details: {emp['name']}. Not handling!")
                employees_to_change.remove(emp)
                # raise ValueError(f"Employee missing 'user' or 'upn': {emp['name']}")

        return employees_to_change
