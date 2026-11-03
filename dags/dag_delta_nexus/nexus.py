import logging
from dataclasses import dataclass

from requests.exceptions import HTTPError
from rkdigi.token_session import ManagedOAuth2Session


logger = logging.getLogger(__name__)
RANDERS_KOMMUNE_SYNC_ID = "ec7a13a3-3d44-46eb-a462-93d2443681b3"


class ProfessionalStsSkipped(Exception):
    """An existing professional could not be fetched from STS after a retry."""


@dataclass(frozen=True)
class DeltaUser:
    """User identifiers supplied by Delta for Nexus synchronization."""
    ad_guid: str
    username: str
    upn: str


@dataclass(frozen=True)
class DeltaAdministrativeUnit:
    """Administrative unit identified by its Delta UUID and display name."""
    uuid: str
    name: str


@dataclass(frozen=True)
class DeltaPerson:
    """Person details and organizational assignments received from Delta."""
    name: str
    user: DeltaUser
    adm_units: list[DeltaAdministrativeUnit]
    primary_adm_unit: DeltaAdministrativeUnit | None
    job_function: str | None

    @classmethod
    def from_dict(cls, data: dict) -> "DeltaPerson":
        """Build a person from a Delta user-details record."""

        user = data["user"]
        primary = data.get("primary_adm_unit")
        return cls(
            name=data["name"],
            user=DeltaUser(user["ADGUID"], user["UserName"], user["UserPrincipalName"]),
            adm_units=[DeltaAdministrativeUnit(**unit) for unit in data["adm_units"]],
            primary_adm_unit=DeltaAdministrativeUnit(**primary) if primary else None,
            job_function=data.get("job_function"),
        )


class NexusClient:
    """Access Nexus professionals and their organizational configuration."""

    def __init__(self, base_url: str, token_url: str, client_id: str, client_secret: str):
        """Connect to Nexus, synchronize STS organizations, and load active organizations."""

        self.base_url = base_url.rstrip("/")
        self.session = ManagedOAuth2Session(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
        )
        self._home = self._get_home()
        self._synchronize_sts_organizations()
        self._organizations = self._get_active_organizations_with_suppliers()

    # Universal private helper for navigating the Nexus API, which uses HAL (Hypertext Application Language) for hypermedia.
    def _follow(
        self, obj: dict, rel: str, method: str = "get",
        expected_reason: str | None = None, accept_error_reason: bool = False, **kwargs,
    ) -> dict | list[dict]:
        """Follow a HAL link relation from an object and return the resulting JSON response."""
        url = obj.get("_links", {}).get(rel, {}).get("href")
        if not url:
            available_rels = ", ".join(obj.get("_links", {}).keys()) or "<none>"
            raise ValueError(f"Missing HAL link rel '{rel}'. Available rels: {available_rels}")
        response = self.session.request(method.upper(), url, **kwargs)
        try:
            response.raise_for_status()
        except HTTPError as error:
            if expected_reason is None and not accept_error_reason:
                raise
            if error.response is None:
                raise
            try:
                result = error.response.json()
            except ValueError:
                raise error from None
            if not isinstance(result, dict) or not result.get("reason"):
                raise
            if expected_reason is not None and result["reason"] != expected_reason:
                raise
            return result
        return response.json() if response.content else {}

    def _set_identity_fields(self, config: dict, person: DeltaPerson) -> list[str]:
        """Update identity fields and describe the changes for the report."""

        changes = []
        old_identifier = config.get("primaryIdentifier")
        new_identifier = person.user.username.lower()
        if not isinstance(old_identifier, str) or old_identifier.casefold() != new_identifier.casefold():
            logger.info(f"Identity change pending primaryIdentifier: {old_identifier!r} -> {new_identifier!r}")
            config["primaryIdentifier"] = new_identifier
            changes.append(f"Brugernavn ændret: {old_identifier or 'ikke angivet'} → {new_identifier}")
        old_identity_id = config.get("identityId")
        new_identity_id = person.user.upn.lower()
        if not isinstance(old_identity_id, str) or old_identity_id.casefold() != new_identity_id.casefold():
            logger.info(f"Identity change pending identityId: {old_identity_id!r} -> {new_identity_id!r}")
            config["identityId"] = new_identity_id
            changes.append(f"Identitet ændret: {old_identity_id or 'ikke angivet'} → {new_identity_id}")
        old_upn = config["activeDirectoryConfiguration"].get("upn")
        new_upn = person.user.upn.lower()
        if not isinstance(old_upn, str) or old_upn.casefold() != new_upn.casefold():
            logger.info(f"Identity change pending activeDirectoryConfiguration.upn: {old_upn!r} -> {new_upn!r}")
            config["activeDirectoryConfiguration"]["upn"] = new_upn
            changes.append(f"UPN ændret: {old_upn or 'ikke angivet'} → {new_upn}")
        return changes

    # Private helpers for specific Nexus API endpoints.
    def _get_home(self) -> dict:
        """Return the home resource for the Nexus API."""
        response = self.session.get(f"{self.base_url}/api/core/mobile/randers/v2/")
        response.raise_for_status()
        return response.json()

    def _synchronize_sts_organizations(self) -> None:
        """Synchronize with external system (Opdatér fra Støttesystem)"""
        self._follow(self._home, "synchronizeStsOrganizations", method="post")

    def _get_active_organizations_with_suppliers(self) -> list[dict]:
        """Return active organizations with a syncId and their active supplier."""
        roots = self._follow(self._home, "activeOrganizationsTree")
        if isinstance(roots, dict):
            roots = [roots]

        organizations_by_id = {}
        pending = list(roots)
        while pending:
            org = pending.pop()
            pending.extend(child for child in org.get("children", []) if isinstance(child, dict))
            if org.get("active") is False or org.get("id") is None or not org.get("syncId"):
                continue
            organizations_by_id[org["id"]] = {
                "id": org["id"],
                "syncId": org["syncId"],
                "name": org.get("name"),
                "supplier": None,
            }

        for supplier in self._follow(self._home, "suppliers"):
            if not supplier.get("active"):
                continue
            organization = organizations_by_id.get(supplier.get("organizationId"))
            if organization is not None and organization["supplier"] is None:
                organization["supplier"] = supplier
        return list(organizations_by_id.values())

    def _update_existing_external_professional(self, broken_object: dict, person: DeltaPerson) -> dict | None:
        """Link an existing professional to STS and fetch it, or return None on failure."""

        linked = self._follow(
            self._home, "stsLinkUpdates", expected_reason="ProfessionalWithStsSnNotFetched", method="put",
            json={"type": "cpr", "identifier": broken_object["details"]["cpr"]},
        )
        if isinstance(linked, dict) and linked.get("reason") == "ProfessionalWithStsSnNotFetched":
            linked = self._follow(
                self._home, "stsLinkUpdates", expected_reason="ProfessionalWithStsSnNotFetched", method="put",
                json={"type": "cpr", "identifier": broken_object["details"]["cpr"]},
            )
            if isinstance(linked, dict) and linked.get("reason") == "ProfessionalWithStsSnNotFetched":
                raise ProfessionalStsSkipped()
        if not isinstance(linked, dict) or not linked.get("primaryIdentifier"):
            logger.error("Unable to link existing professional to STS")
            return None

        matches = self._follow(self._home, "professionals", params={"query": linked["primaryIdentifier"]})
        if not isinstance(matches, list) or len(matches) != 1:
            logger.error("Expected one existing professional for STS link")
            return None
        professional = matches[0]
        config = self._follow(professional, "configuration", expected_reason="ProfessionalWithStsSnNotFetched")
        if isinstance(config, dict) and config.get("reason") == "ProfessionalWithStsSnNotFetched":
            config = self._follow(professional, "configuration", expected_reason="ProfessionalWithStsSnNotFetched")
            if isinstance(config, dict) and config.get("reason") == "ProfessionalWithStsSnNotFetched":
                raise ProfessionalStsSkipped()
        if not isinstance(config, dict) or not isinstance(config.get("activeDirectoryConfiguration"), dict):
            logger.error("Existing professional has no configuration")
            return None
        updated = self._follow(professional, "self")
        if not isinstance(updated, dict):
            raise ValueError("Professional lookup did not return a professional")
        return updated

    def _import_professional(self, person: DeltaPerson) -> dict | None:
        """Import a professional from STS or link one that already exists."""

        external = self._follow(self._home, "importProfessionalFromSts", accept_error_reason=True, params={"query": person.user.username})
        if not isinstance(external, dict):
            logger.error("Unable to import professional from STS")
            return None
        if external.get("reason") == "ProfessionalWithSameCprAlreadyExists":
            return self._update_existing_external_professional(external["brokenObject"], person)
        if external.get("reason"):
            logger.error("Unable to import professional from STS")
            return None
        return external

    def _get_professional(self, person: DeltaPerson) -> dict | None:
        """Find a professional by AD GUID, importing or creating one if needed."""

        matches = self._follow(self._home, "professionals", params={"query": person.user.ad_guid})
        if not isinstance(matches, list):
            raise ValueError("Professional lookup did not return a list")
        if len(matches) > 1:
            raise ValueError("Professional lookup returned multiple matches")
        if not matches:
            professional = self._import_professional(person)
            if professional is None:
                raise RuntimeError("STS import or linking did not return a professional")
        else:
            professional = self._follow(matches[0], "self")
        if not isinstance(professional, dict):
            raise ValueError("Professional lookup did not return a professional")
        if "create" in professional.get("_links", {}):
            logger.warning("Professional should be created - SKIPPING")
            raise RuntimeError("Professional creation is disabled")
            if not isinstance(professional.get("activeDirectoryConfiguration"), dict):
                raise ValueError("Professional configuration cannot be updated")
            logger.info("Creating professional")
            self._set_identity_fields(professional, person)
            professional["primaryAddress"]["route"] = "home:importProfessionalFromSts"
            professional["activeDirectoryConfiguration"]["route"] = "home:importProfessionalFromSts"
            saved = self._follow(professional, "create", method="post", expected_reason="ProfessionalServiceDataIntegrity", json=professional)
            if isinstance(saved, dict) and saved.get("reason") == "ProfessionalServiceDataIntegrity":
                logger.error("Data integrity error creating professional")
                return None
            if not isinstance(saved, dict) or saved.get("reason"):
                logger.error("Unable to create professional from STS")
                return None
            logger.info("Professional created")
            professional = saved
        return professional

    # Public methods for updating professionals
    def update_existing_professional(self, person: DeltaPerson) -> list[str]:
        """Describe proposed changes to an existing professional in Danish."""
        logger.info(f"Handling: {person.name}")
        professional = self._get_professional(person)
        changes = []

        org_by_sync_id = {org["syncId"]: org for org in self._organizations}
        desired_sync_ids = {org.uuid for org in person.adm_units} | {RANDERS_KOMMUNE_SYNC_ID}
        missing_sync_ids = desired_sync_ids - org_by_sync_id.keys()
        if missing_sync_ids:
            raise ValueError("Delta organization is not present in active organizations")

        primary = person.primary_adm_unit
        if primary and primary.uuid not in org_by_sync_id:
            raise ValueError("Delta primary organization is not present in active organizations")

        current_orgs = self._follow(professional, "organizations")
        if isinstance(current_orgs, dict):
            current_orgs = [current_orgs]
        pending = list(current_orgs)
        current_by_sync_id = {}
        while pending:
            org = pending.pop()
            pending.extend(child for child in org.get("children", []) if isinstance(child, dict))
            if org.get("syncId") and org.get("id") is not None:
                current_by_sync_id[org["syncId"]] = org

        added_sync_ids = sorted(desired_sync_ids - current_by_sync_id.keys())
        removed_sync_ids = sorted((current_by_sync_id.keys() - desired_sync_ids) & org_by_sync_id.keys())
        added_ids = [org_by_sync_id[sync_id]["id"] for sync_id in added_sync_ids]
        removed_ids = [current_by_sync_id[sync_id]["id"] for sync_id in removed_sync_ids]
        if added_ids or removed_ids:
            added_names = [org_by_sync_id[sync_id].get("name") or sync_id for sync_id in added_sync_ids]
            removed_names = [current_by_sync_id[sync_id].get("name") or sync_id for sync_id in removed_sync_ids]
            logger.info(f"Organization update pending - adding: {added_names}, removing: {removed_names}")
            changes.extend(f"Organisation tilføjet: {name}" for name in added_names)
            changes.extend(f"Organisation fjernet: {name}" for name in removed_names)
            # self._follow(professional, "updateOrganizations", method="post", json={"added": added_ids, "removed": removed_ids})
            # logger.info("Organizations updated")

        config = self._follow(professional, "configuration")
        if not isinstance(config, dict) or not isinstance(config.get("activeDirectoryConfiguration"), dict):
            raise ValueError("Professional configuration cannot be updated")

        identity_changes = self._set_identity_fields(config, person)
        changes.extend(identity_changes)
        config_changed = bool(identity_changes)

        if person.job_function:
            jobs = self._follow(self._home, "professionalJobs")
            if not isinstance(jobs, list):
                raise ValueError("Professional jobs lookup did not return a list")
            job = next((item for item in jobs if isinstance(item, dict) and item.get("active") and isinstance(item.get("name"), str) and item["name"].casefold() == person.job_function.casefold()), None)
            if job is None:
                logger.warning(f"Job title not found: {person.job_function!r}")
                changes.append(f"Stillingsbetegnelse kunne ikke ændres: {person.job_function} findes ikke i Nexus.")
            elif config.get("professionalJob") != job:
                old_job = config.get("professionalJob")
                logger.info(f"Job title change pending: {(old_job.get('name') if isinstance(old_job, dict) else None)!r} -> {job['name']!r}")
                config["professionalJob"] = job
                config_changed = True
                changes.append(f"Stillingsbetegnelse ændret: {(old_job.get('name') if isinstance(old_job, dict) else None) or 'ikke angivet'} → {job['name']}")

        if primary:
            primary_organization = current_by_sync_id.get(primary.uuid)
            if primary_organization is None:
                logger.error(f"Delta primary organization not found among professional's organizations: {primary.uuid}")
                changes.append("Primær organisation kunne ikke ændres: organisationen findes ikke på Nexus-brugeren.")
            else:
                old_primary = config.get("primaryOrganization")
                old_primary_id = old_primary.get("id") if isinstance(old_primary, dict) else None
                if old_primary_id != primary_organization["id"]:
                    old_primary_name = old_primary.get("name") if isinstance(old_primary, dict) else None
                    logger.info(f"Primary organization change pending: {old_primary_name!r} -> {primary_organization.get('name')!r}")
                    config["primaryOrganization"] = primary_organization
                    config_changed = True
                    changes.append(f"Primær organisation ændret: {old_primary_name or 'ikke angivet'} → {primary_organization.get('name') or primary.uuid}")

        supplier = org_by_sync_id[primary.uuid].get("supplier") if primary else None
        old_supplier = config.get("defaultOrganizationSupplier") or {}
        if supplier is not None and old_supplier.get("id") != supplier["id"]:
            logger.info(f"Supplier change pending: {old_supplier.get('name')!r} -> {supplier.get('name')!r}")
            config["defaultOrganizationSupplier"] = supplier
            config_changed = True
            changes.append(f"Standardleverandør ændret: {old_supplier.get('name') or 'ikke angivet'} → {supplier.get('name') or supplier['id']}")

        if config_changed:
            # self._follow(config, "update", method="put", json=config)
            # logger.info("Configuration updated")
            pass
        return changes
