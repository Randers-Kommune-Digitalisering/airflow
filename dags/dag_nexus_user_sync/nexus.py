import logging
from airflow.hooks.base import BaseHook
from rkdigi.token_session import ManagedOAuth2Session


logger = logging.getLogger(__name__)


class NexusClient:
    _created = False

    def __init__(self, hook: BaseHook, adm_org_dict: dict, supplier_list: list[dict]):
        if NexusClient._created:
            raise Exception("NexusClient can only be created once.")
        NexusClient._created = True
        self._base_url: str = hook.get_connection(hook.http_conn_id).host.strip('/')
        self._logout_url: str = hook.get_connection(hook.http_conn_id).extra_dejson.get("logout_url")
        self._session: ManagedOAuth2Session = self._set_session(hook)
        self._home: dict = self._set_home()
        self._active_org_list: list[dict] = self._fetch_all_active_organisations(delta_orgs=adm_org_dict, supplier_list=supplier_list)
        self._all_job_titles = self._fetch_all_job_titles()

    def _set_session(self, hook: BaseHook) -> ManagedOAuth2Session:
        """
        Sets up the Nexus API session using credentials from Airflow connection.

        :param hook: Airflow BaseHook to get connection from.
        :return: Tuple containing the Nexus API session and the base URL for API requests.
        """
        nexus_conn = hook.get_connection(hook.http_conn_id)
        return ManagedOAuth2Session(
            token_url=nexus_conn.extra_dejson.get("token_url"),
            client_id=nexus_conn.login,
            client_secret=nexus_conn.password,
        )

    def _set_home(self) -> dict:
        """
        Fetches the home resource from the Nexus API.
        """
        res = self._session.get(f"{self._base_url}/api/core/mobile/randers/v2/")
        res.raise_for_status()
        return res.json()

    def _collect_syncIds_and_ids_from_org(self, org: dict) -> list[dict]:
        """
        Recursively collects syncIds and ids from an organization and its children.
        """
        sync_ids_and_ids = []
        if 'syncId' in org and org['syncId'] is not None:
            sync_ids_and_ids.append({'id': org['id'], 'syncId': org['syncId'], 'name': org['name']})
        for child in org.get('children', []):
            if isinstance(child, dict):
                sync_ids_and_ids.extend(self._collect_syncIds_and_ids_from_org(child))
        return sync_ids_and_ids

    def _fetch_all_active_organisations(self, delta_orgs: dict, supplier_list: list[dict]) -> list[dict]:
        """
        Fetches all active organizations from the Nexus API, filters them based on syncIds present in delta_orgs, and adds supplier info from supplier_list.
        """
        # Get active organizations
        res = self._session.get(self._home['_links']['activeOrganizationsTree']['href'])
        res.raise_for_status()
        all_active_organisations = res.json()

        if isinstance(all_active_organisations, dict):
            all_active_organisations = [all_active_organisations]

        # Collect syncIds and ids from all active organizations
        organisation_ids = []
        for org in all_active_organisations:
            organisation_ids.extend(self._collect_syncIds_and_ids_from_org(org))

        # Filter the list of organizations to only include those with syncIds present in delta_orgs
        delta_orgs_list = [item for key, values in delta_orgs.items() for item in [key] + values]
        relevant_organisations = [org for org in organisation_ids if org.get('syncId') in delta_orgs_list]

        # Get active suppliers
        res = self._session.get(self._home['_links']['suppliers']['href'])
        res.raise_for_status()
        all_suppliers_res = res.json()

        active_suppliers = [supplier for supplier in all_suppliers_res if supplier.get('active')]

        # Build lookups
        org_to_supplier_lookup: dict = {entry['delta_id']: entry['nexus_id'] for entry in supplier_list}
        supplier_by_id = {str(supplier['id']): supplier for supplier in active_suppliers}

        # Add supplier info to the relevant orgs
        for org in relevant_organisations:
            supplier_id = org_to_supplier_lookup.get(org['syncId'])
            if supplier_id:
                org['supplier'] = supplier_by_id.get(str(supplier_id))

        return relevant_organisations

    def _fetch_all_job_titles(self) -> list[dict]:
        res = self._session.get(self._home['_links']['professionalJobs']['href'])
        res.raise_for_status()
        return res.json()

    def logout(self) -> None:
        """
        Logs out of the Nexus API session.
        """
        res = self._session.get(self._logout_url)
        res.raise_for_status()

    def _fetch_professional(self, primary_identifier: str) -> dict | None:
        """
        Fetches a professional from the Nexus API based on the primary identifier (user/DQ-number).
        Returns the professional dict if found, None if not found, and raises an exception if multiple professionals are found.
        """
        res = self._session.get(self._home['_links']['professionals']['href'], params={'query': primary_identifier})
        res.raise_for_status()
        professionals = res.json()
        if len(professionals) > 1:
            raise Exception(
                f"Expected to find exactly one professional with primary identifier: {primary_identifier}, but found {len(professionals)}"
            )
        elif len(professionals) == 0:
            logger.info(f"No professional found with primary identifier: {primary_identifier}")
            return None
        return professionals[0]

    def _update_existing_external_professional(self, broken_object: dict, primary_identifier: str, upn: str) -> dict | None:
        """
        If a professional with the same CPR but different primary identifier (user/DQ-number) already exists in Nexus.
        Attempt to update that professional with the new primary identifier and UPN instead of creating a new professional.
        """
        res = self._session.put(f"{self._base_url}/api/core/mobile/randers/v2/professionals/stsProfessional/stsLinkUpdates", json={"type": "cpr", "identifier": broken_object['details']['cpr']})
        if 'reason' in res.json():
            if res.json()['reason'] == 'ProfessionalWithStsSnNotFetched':
                logger.error(f"Professional {primary_identifier} not found in external system (CPR does not exist)")
                return
        res.raise_for_status()
        data = res.json()
        if data:
            # Find the old object for the professional
            old_professinal = self._fetch_professional(data['primaryIdentifier'])
            if old_professinal:
                # Update the professional with the new username/dq-number (primary identifier)
                res = self._session.get(old_professinal['_links']['configuration']['href'])
                if 'reason' in res.json():
                    if res.json()['reason'] == 'ProfessionalWithStsSnNotFetched':
                        logger.error(f"Professional {primary_identifier} not found in external system (CPR does not exist)")
                        return
                res.raise_for_status()
                professinal_conf = res.json()
                if professinal_conf:
                    professinal_conf['primaryIdentifier'] = primary_identifier
                    professinal_conf['identityId'] = upn
                    professinal_conf['activeDirectoryConfiguration']['upn'] = upn
                    res = self._session.put(old_professinal['_links']['configuration']['href'], json=professinal_conf)
                    res.raise_for_status()
                    update_res = res.json()
                    if update_res:
                        logger.info(f'Updated username (DQ-number/unique ID) and UPN for professional {primary_identifier}')
                        professinal = self._fetch_professional(primary_identifier)
                        if professinal:
                            return professinal
                        else:
                            logger.error(f'Unable to get professional {primary_identifier} after update')
                    else:
                        logger.error(f'Failed to update existing external professional {primary_identifier}')
            else:
                logger.error('Multiple results returned for old primary identifier')
        else:
            logger.error(f"Failed to import professional {primary_identifier}")

    def _create_external_professional(self, external_professional: dict, employee: dict) -> dict | None:
        """
        Creates a new professional in Nexus based on the external professional data and employee data from Delta.
        """
        external_professional['primaryIdentifier'] = employee['user']
        external_professional['identityId'] = employee['upn']
        external_professional['activeDirectoryConfiguration']['upn'] = employee['upn']
        external_professional['primaryAddress']['route'] = 'home:importProfessionalFromSts'
        external_professional['activeDirectoryConfiguration']['route'] = 'home:importProfessionalFromSts'
        res = self._session.post(external_professional['_links']['create']['href'], json=external_professional)
        if 'reason' in res.json():
            if res.json()['reason'] == 'ProfessionalServiceDataIntegrity':
                logger.error(f"Data integrity error when creating professional {employee['user']}")
                return
        res.raise_for_status()
        new_professional = res.json()
        if new_professional:
            logger.info(f"Professional {employee['user']} created")
            return new_professional
        else:
            logger.error(f"Failed to create professional {employee['user']} from external system")

    def _fetch_external_professional(self, employee: dict) -> dict | None:
        """
        Fetches professional data from the external system based on the employee's primary identifier (user/DQ-number).
        """
        res = self._session.get(self._home['_links']['importProfessionalFromSts']['href'], params={'query': employee['user']})
        data = res.json()
        if data:
            if 'reason' in data:
                if data['reason'] == 'ProfessionalWithStsSnNotFetched':
                    logger.error(f"Professional {employee['user']} not found in external system (user/DQ-number does not exist)")
                elif data['reason'] == 'ProfessionalWithSameCprAlreadyExists':
                    logger.warning(f"Professional {employee['user']} already exists in Nexus - attempt to update")
                    broken_object = data['brokenObject']
                    updated_professional = self._update_existing_external_professional(broken_object=broken_object, primary_identifier=employee['user'], upn=employee['upn'])
                    if updated_professional:
                        return updated_professional
                else:
                    logger.error(f"Unknown error in external system for professional {employee['user']}:\n {data}")
            else:
                new_professional = self._create_external_professional(external_professional=data, employee=employee)
                if new_professional:
                    return new_professional
                else:
                    logger.error(f"Failed to create professional {employee['user']} from external system")
        else:
            logger.error(f"Error fetching external professional: {data}")

    def _fetch_professional_org_syncIds(self, professional: dict) -> list[dict]:
        """
        Fetches the organizations assigned to a professional. Returns a list of dicts with 'id' (Nexus id), 'syncId' (Delta id) and 'name'
        """
        res = self._session.get(professional['_links']['self']['href'])
        res.raise_for_status()
        professional_self = res.json()

        res = self._session.get(professional_self['_links']['organizations']['href'])
        res.raise_for_status()
        professional_org_data = res.json()

        if isinstance(professional_org_data, dict):
            professional_org_data = [professional_org_data]

        professional_organizations = []
        for org in professional_org_data:
            professional_organizations.extend(self._collect_syncIds_and_ids_from_org(org))

        return professional_organizations

    def _update_professional_organisations(self, professional: dict, organisation_ids_to_add: list, organisation_ids_to_remove: list) -> None:
        """
        Updates the organizations assigned to a professional by adding and removing organization ids as specified.
        """
        res = self._session.get(professional['_links']['self']['href'])
        res.raise_for_status()
        professional = res.json()

        json_body = {
            "added": organisation_ids_to_add,
            "removed": organisation_ids_to_remove
        }

        res = self._session.post(professional['_links']['updateOrganizations']['href'], json=json_body)
        res.raise_for_status()

    def _update_professional_supplier(self, employee: dict, professional: dict, supplier: dict) -> dict:
        """
        Updates the supplier assigned to a professional.
        """
        res = self._session.get(professional['_links']['self']['href'])
        res.raise_for_status()
        professional = res.json()

        res = self._session.get(professional['_links']['configuration']['href'])
        res.raise_for_status()
        professional_config = res.json()

        current_supplier = professional_config.get('defaultOrganizationSupplier')
        if current_supplier and current_supplier.get('id') == supplier.get('id'):
            logger.info(f'Professional {employee["user"]} already has correct supplier assigned - not updating')
            return professional_config

        professional_config['defaultOrganizationSupplier'] = supplier

        res = self._session.put(professional_config['_links']['update']['href'], json=professional_config)
        res.raise_for_status()
        logger.info(f"Professional {employee['user']} updated with new supplier")
        return res.json()

    def _update_professional_job_title(self, employee: dict, professional: dict, job_title: dict) -> dict:
        """
        Updates the job title assigned to a professional if it's different from the current job title. Returns the updated professional.
        """
        # Professional self
        res = self._session.get(professional['_links']['self']['href'])
        res.raise_for_status()
        professional_self = res.json()

        # Professional configuration
        res = self._session.get(professional_self['_links']['configuration']['href'])
        res.raise_for_status()
        professional_config = res.json()

        if professional_config['professionalJob'] == job_title:
            logger.info(f'Professional {employee["user"]} already has correct job title assigned - not updating')
            return professional_self
        else:
            professional_config['professionalJob'] = job_title
            res = self._session.put(professional_config['_links']['update']['href'], json=professional_config)
            res.raise_for_status()
            logger.info(f"Professional {employee['user']} updated with job title")
            return res.json()

    def _execute_brugerauth(self, employee: dict, report_list: list | None = None) -> None:
        """
        Executes the main flow for an employee / professional:
        fetches the professional from Nexus, imports it if it doesn't exist, updates organizations and supplier as needed.
        """
        professional = self._fetch_professional(employee['user'])

        if not professional:
            logger.info(f"Professional {employee['user']} not found in Nexus - importing")
            external_professional = self._fetch_external_professional(employee=employee)
            if external_professional:
                professional = external_professional
            else:
                logger.error(f"Failed to import professional, skipping employee {employee['user']} - not handling!")
                if report_list is not None:
                    report_list.append(f"{employee['name']} ({employee['user']}) - kunne ikke importeres til Nexus")
                return

        professional_org_list = self._fetch_professional_org_syncIds(professional)

        assigned_sync_ids = {item['syncId'] for item in professional_org_list}
        organisation_ids_to_assign = [
            item['id'] for item in self._active_org_list
            if item['syncId'] in employee['organizations'] and item['syncId'] not in assigned_sync_ids
        ]
        # logger.info(f"Organisation ids to assign: {organisation_ids_to_assign}")

        to_remove_sync_ids = assigned_sync_ids - set(employee['organizations'])
        organisation_ids_to_remove = [
            item['id'] for item in self._active_org_list
            if item['syncId'] in to_remove_sync_ids
        ]
        # logger.info(f"Organisation ids to remove: {organisation_ids_to_remove}")

        if len(organisation_ids_to_assign) > 0 or len(organisation_ids_to_remove) > 0:
            self._update_professional_organisations(professional, organisation_ids_to_assign, organisation_ids_to_remove)
            logger.info(f"Professional {employee['user']} updated with organisations")
        else:
            logger.info(f"Professional {employee['user']} already has correct organisations assigned - not updating")

        if employee['organizations']:
            current = next((item for item in self._active_org_list if item['syncId'] == employee['organizations'][0]), {})
            supplier = current.get('supplier')

            # If it has a supplier update it (skipped if already assigned)
            if supplier:
                self._update_professional_supplier(employee=employee, professional=professional, supplier=supplier)
            else:
                logger.info(f"Top organisation for professional {employee['user']} has no supplier - not updating")
                if report_list is not None:
                    report_list.append(f"{employee['name']} ({employee['user']}) - ingen standardleverandør fundet")

        if employee['job_title']:
            job_title_obj = next((item for item in self._all_job_titles if item.get('name', '').lower() == employee['job_title'].lower() and item.get('active')), None)
            if job_title_obj:
                if self._update_professional_job_title(employee=employee, professional=professional, job_title=job_title_obj):
                    pass
                    # logger.info(f"Professional {primary_identifier} updated with job title")
                else:
                    logger.error(f"Failed to update professional {employee['user']} with job title")
            else:
                logger.warning(f"Job title '{employee['job_title']}' not found in Nexus - not updating")

        logger.info(f"Professional {employee['user']} updated sucessfully")

    def import_to_nexus_and_set_permissions(self, employees_changed_list: list[dict], report_list: list | None = None) -> None:
        """
        Main method to import/update professionals in Nexus and set their permissions based on the employee data from Delta.
        """
        for index, employee in enumerate(employees_changed_list):
            logger.info(f"Processing employee {employee['user']} - {index + 1}/{len(employees_changed_list)}")
            self._execute_brugerauth(employee=employee, report_list=report_list)
