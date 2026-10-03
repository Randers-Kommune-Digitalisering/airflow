import re
import logging

from urllib.parse import urljoin
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo
from airflow.hooks.base import BaseHook
from rkdigi import ManagedOAuth2Session

from dag_xflow_nexus_hjaelpemidler.constants import STATE_ACTIVE_NAME, STATE_DEAD_TYPE_ID
from dag_xflow_nexus_hjaelpemidler.models import NexusDocument, NexusCase

logger = logging.getLogger(__name__)


class NexusClient:
    """A client for interacting with the Nexus API, which uses HAL (Hypertext Application Language) for hypermedia."""
    def __init__(self, nexus_hook: BaseHook):
        nexus_conn = nexus_hook.get_connection(nexus_hook.http_conn_id)
        self.base_url = f"{nexus_conn.host.rstrip('/')}/api/core/mobile/randers/v2/"
        self.session = ManagedOAuth2Session(
            token_url=nexus_conn.extra_dejson.get("token_url"),
            client_id=nexus_conn.login,
            client_secret=nexus_conn.password,
        )

    @staticmethod
    def _normalize(value):
        """Recursively normalize strings in the given value by stripping whitespace and collapsing multiple spaces."""
        if isinstance(value, str):
            return re.sub(r"\s+", " ", value).strip()

        if isinstance(value, dict):
            return {k: NexusClient._normalize(v) for k, v in value.items()}

        if isinstance(value, list):
            return [NexusClient._normalize(v) for v in value]

        return value

    def _json(self, response):
        """Process the JSON response from a requests.Response object, raising an error for HTTP issues."""
        response.raise_for_status()
        return self._normalize(response.json())

    # Universal private helpers for navigating the Nexus API, which uses HAL (Hypertext Application Language) for hypermedia.
    def _follow(self, obj: dict, rel: str, method: str = "get", **kwargs) -> dict:
        """Follow a HAL link relation from an object and return the resulting JSON response."""
        url = obj.get("_links", {}).get(rel, {}).get("href")
        if not url:
            available_rels = ", ".join(obj.get("_links", {}).keys()) or "<none>"
            raise ValueError(f"Missing HAL link rel '{rel}'. Available rels: {available_rels}")
        res = self.session.request(method.upper(), url, **kwargs)
        return self._json(res)

    def _get(self, endpoint: str = None, params: dict | None = None) -> dict:
        """Perform a GET request to the Nexus API, optionally to a specific endpoint with query parameters."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}" if endpoint else self.base_url
        response = self.session.get(url, params=params)
        return self._json(response)

    # Public methods
    def get_dashboard(self, patient: dict, name: str) -> dict:
        """Get the dashboard data for a specific patient and dashboard name."""
        patient_preferences = self._follow(patient, "patientPreferences")
        dashboard_partial = next((item for item in patient_preferences["CITIZEN_DASHBOARD"] if item.get("name") == name), None)
        if dashboard_partial is None:
            raise ValueError(f"Could not find '{name}' in patient preferences")
        return self._follow(dashboard_partial, "self")

    def get_widget(self, dashboard: dict, name: str) -> dict:
        """Get a specific widget from a dashboard by its header title."""
        widget = next((item for item in dashboard.get("view", {}).get("widgets", []) if item.get("headerTitle") == name), None)
        if widget is None:
            raise ValueError(f"Could not find widget with headerTitle '{name}' in dashboard")
        return widget

    def get_patient(self, search_text: str) -> dict | None:
        """Get the patient data for a specific CPR number."""
        home = self._get()
        patient_search = self._follow(home, "patients", params={"query": search_text})
        pages = patient_search.get("pages", [])
        if not pages:
            # raise ValueError("No patient data found for search_text")
            return None
        patient_data_search = self._follow(pages[0], "patientData")
        if len(patient_data_search) != 1:
            raise ValueError(f"Expected exactly one patient data entry, but found {len(patient_data_search)}")
        return self._follow(patient_data_search[0], "self")

    def is_active(self, patient: dict, set_if_not: bool = False) -> bool:
        """Check if a patient is currently active, optionally setting the state to active if not."""
        current_state = patient.get("patientState", {})

        # Always report 'DEAD' state as inactive - do not try to update
        if current_state.get("type", {}).get("id") == STATE_DEAD_TYPE_ID:
            # raise ValueError("State is 'Dead'")
            return False

        is_active = current_state.get("name") == STATE_ACTIVE_NAME  # Other states are considered active, but should be changed to this specific one.
        if not is_active and set_if_not:
            schedule = patient.get("patientStateValueSchedule", {})
            new_period = self._follow(obj=schedule, rel="prototypeValuePeriod", method="get")

            state = next((v for v in new_period.get("possibleValues", []) if v.get("name") == STATE_ACTIVE_NAME), None)
            if state is None:
                raise ValueError(f"Could not find patient state '{STATE_ACTIVE_NAME}' in possible values")

            local_midnight = datetime.combine(date.today(), time.min, tzinfo=ZoneInfo("Europe/Copenhagen"))
            iso_start = local_midnight.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            new_period["value"] = state
            new_period["startDate"] = iso_start
            new_period["endDate"] = None

            current_period = next((p for p in schedule.get("valuePeriods", []) if p.get("endDate") is None), None)
            if current_period is None:
                raise ValueError("Could not find the current (open) patientState value period")
            current_period["endDate"] = iso_start

            schedule.setdefault("valuePeriods", []).append(new_period)
            patient["patientState"] = state
            patient["patientStateStartDate"] = iso_start

            updated_patient = self._follow(obj=patient, rel="update", method="put", json=patient)
            return self.is_active(patient=updated_patient, set_if_not=False)
        return is_active

    def has_pathway(self, patient: dict, program_name: str, pathway_name: str | None = None, add_if_missing: bool = False) -> bool:
        """Check if a specific program or pathway is associated with a patient, optionally adding it if missing."""
        active_pathways = self._follow(obj=patient, rel="pathwayTree", method="get")  # active programs and their children (pathways)
        parent_program = next((p for p in active_pathways if p.get("name") == program_name), None)

        # Add the program if it is missing
        if parent_program is None and add_if_missing:
            available_programs = self._follow(obj=patient, rel="availableProgramPathways", method="get")
            program_to_add = next((p for p in available_programs if p.get("name") == program_name), None)
            if program_to_add is None:
                raise ValueError(f"Program '{program_name}' cannot be added because it is not available in the available program pathways")

            self._follow(obj=program_to_add, rel="enroll", method="put")
            active_pathways = self._follow(obj=patient, rel="pathwayTree", method="get")
            parent_program = next((p for p in active_pathways if p.get("name") == program_name), None)
            if parent_program is None:
                raise ValueError(f"Program '{program_name}' was not found after enrollment")

        if parent_program is None:
            return False

        if pathway_name is None:
            return True

        child_pathways = parent_program.get("children") or []
        if any(child.get("name") == pathway_name for child in child_pathways):
            return True

        # Add the pathway if it is missing
        if add_if_missing:
            available_pathways = self._follow(
                obj=parent_program,
                rel="availableNestedProgramPathways",
                method="get",
            )
            pathway_to_add = next(
                (pathway for pathway in available_pathways if pathway.get("name") == pathway_name),
                None,
            )
            if pathway_to_add is None:
                raise ValueError(
                    f"Pathway '{pathway_name}' cannot be added because it is not available "
                    f"under program '{program_name}'"
                )

            self._follow(obj=pathway_to_add, rel="enroll", method="put")
            return self.has_pathway(
                patient=patient,
                program_name=program_name,
                pathway_name=pathway_name,
                add_if_missing=False,
            )

        return False

    def get_auto_assignment(self, form: dict, assignment_title: str, organization_name: str) -> dict:
        """Get the auto-assignment for a specific form and assignment name."""
        activityIdentifier = form.get("activityIdentifier", {}).get("identifier")
        state = form.get("workflowState", {}).get("name")
        activity_type = form.get("activityIdentifier", {}).get("type")
        form_definition_id = form.get("formDefinition", {}).get("id")

        payload = {
            "activities": [
                {
                    "activity": activityIdentifier,
                    "groupKey": None,
                    "params": [
                        {"key": "state", "value": state},
                        {"key": "activityType", "value": activity_type},
                        {"key": "formDefinitionId", "value": form_definition_id},
                    ],
                }
            ]
        }

        auto_assignments_res = self._follow(obj=form, rel="autoAssignmentsPrototype", method="post", json=payload)
        assignments = auto_assignments_res.get("assignments", [])
        selected_assignment = next((a for a in assignments if a.get("type", {}).get("name") == assignment_title), None)
        if not selected_assignment:
            raise ValueError(f"Could not find assignment with type name '{assignment_title}' in response")

        if (selected_assignment.get("organizationAssignee") or {}).get("displayName") != organization_name:
            # If the current organization assignee does not match the desired organization,
            # fetch the list of available organization assignees and select the correct one.
            organization_href = selected_assignment.get("_links", {}).get("availableOrganizationAssignees", {}).get("href")
            if not organization_href:
                raise ValueError("Assignment has no availableOrganizationAssignees link")
            organizations = self._json(self.session.request("GET", urljoin(self.base_url, organization_href)))
            if not isinstance(organizations, list):
                raise ValueError("Expected a list of available organization assignees")
            organization = next((item for item in organizations if item.get("name") == organization_name), None)
            if organization is None:
                raise ValueError(f"Organization assignee '{organization_name}' is not available")

            selected_assignment["organizationAssignee"] = {
                "organizationId": organization["id"],
                "displayName": organization["name"],
            }

        return selected_assignment

    def create_assignment(self, assignment: dict) -> dict:
        """Create an assignment in Nexus."""
        CREATE_ACTION_NAME = "Opret"
        create_action = next((a for a in assignment.get("actions", []) if a.get("name") == CREATE_ACTION_NAME), None)
        if not create_action:
            raise ValueError(f"Could not find action '{CREATE_ACTION_NAME}' in assignment actions")
        return self._follow(obj=create_action, rel="createAssignment", method="post", json=assignment)

    def rollback_objects(self, created_docs: list[dict], created_form: dict | None, created_assignment: dict | None) -> None:
        """Delete already created objects in reverse order. Never raises, so the original error is kept."""
        DELETE_ACTION_NAME = "Slet"

        if created_assignment:
            try:
                delete_action = next((a for a in created_assignment.get("actions", []) if a.get("name") == DELETE_ACTION_NAME), None)
                if not delete_action:
                    raise ValueError(f"Could not find action '{DELETE_ACTION_NAME}' in assignment actions")
                self._follow(obj=delete_action, rel="updateAssignment", method="put", json=created_assignment)
                logger.warning(f"Rolled back assignment id={created_assignment.get('id')}")
            except Exception:
                logger.exception(f"Failed rolling back assignment id={created_assignment.get('id')}")

        if created_form:
            try:
                # Try to delete the form (Only 'Henvendelse Kropsbårne hjælpemidler' form supports direct deletion)
                self._follow(obj=created_form, rel="delete", method="delete")
                logger.warning(f"Rolled back form id={created_form.get('id')}")
            except Exception:
                try:
                    # If direct deletion fails, revert the form to draft mode and add a prefix to indicate it was cancelled due to an error
                    actions = self._follow(obj=created_form, rel="availableActions")
                    draft_action = next((action for action in actions if action.get("name") == "Kladde"), None)
                    if draft_action is None:
                        raise ValueError("Could not find action 'Kladde' in availableActions")
                    for item in created_form.get("items", []):
                        if item.get("label") == "Hvad er årsagen":
                            prefix = "ANNULLERET pga. fejl - "
                            value = item.get("value")
                            if value is None:
                                item["value"] = prefix
                            elif isinstance(value, str) and not value.startswith(prefix):
                                item["value"] = prefix + value
                    self._follow(obj=draft_action, rel="updateFormData", method="put", json=created_form)
                    logger.warning(f"Reverted form to draft id={created_form.get('id')}")
                except Exception:
                    logger.exception(f"Failed reverting form to draft id={created_form.get('id')}")

        for document in reversed(created_docs):
            try:
                self._follow(obj=document, rel="delete", method="delete")
                logger.warning(f"Rolled back document id={document.get('id')}")
            except Exception:
                logger.exception(f"Failed rolling back document id={document.get('id')}")

    def get_document(self, widget: dict, pathway_name: str) -> dict:
        """Retrieve a document (prototype, without creating it) from the specified widget in Nexus with the specified pathway name. Returns the document api object."""
        document = self._follow(widget["creatableObjects"], "documentPrototype")

        # Ensure the new document is associated with the correct pathway before proceeding
        if (document.get("pathwayAssociation", {}).get("placement") or {}).get("name") != pathway_name:
            pathway_associations = self._follow(document.get("pathwayAssociation"), "availablePathwayAssociation")
            pathway_association = next((c for p in pathway_associations for c in p.get("children", []) if c.get("name") == pathway_name), None)
            if pathway_association is None:
                raise ValueError(f"Could not find placement with name '{pathway_name}'.")
            pathway_association = self._follow(pathway_association, "self")
            document = self._follow(pathway_association, "documentPrototype")

        return document

    def create_document(self, document: dict, nexus_document: NexusDocument) -> dict:
        """Add a document to the specified widget in Nexus. Returns the created document api object."""
        document["name"] = nexus_document.name
        document["originalFileName"] = nexus_document.file_name

        if document.get("relevanceDate", "").split("T")[0] != nexus_document.date.strftime("%Y-%m-%d"):
            local_midnight = datetime.combine(nexus_document.date, time.min, tzinfo=ZoneInfo("Europe/Copenhagen"))
            document["relevanceDate"] = local_midnight.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

        created_document = self._follow(document, "create", method="post", json=document)
        return self._follow(created_document, "upload", method="post", files={"file": (nexus_document.file_name, nexus_document.file_bytes, nexus_document.mime_type)})

    def get_filled_form(self, widget: dict, nexus_case: NexusCase) -> dict:
        """Retrieve the filled form for the specified widget and nexus case."""
        fields_by_name = {field.label: field for field in nexus_case.fields}
        if len(fields_by_name) != len(nexus_case.fields):
            raise ValueError("Duplicate field names are not allowed")

        # Find the form with the title matching the nexus_case.form_name
        forms = widget.get("creatableObjects", {}).get("forms", [])
        if not forms:
            raise ValueError("No forms found in widget.creatableObjects.forms")

        form = next((form for form in forms if form.get("title") == nexus_case.form_name), None)
        if form is None:
            raise ValueError(f"Could not find form with title '{nexus_case.form_name}'.")
        form = self._follow(form, "formDataPrototype")

        # If the current pathway placement does not match the one specified in the nexus_case, find and set the correct placement
        if (form.get("pathwayAssociation", {}).get("placement") or {}).get("name") != nexus_case.pathway_name:
            placements = self._follow(form.get("pathwayAssociation"), "availablePathwayPlacements")
            placement = next((c.get("patientPathwayPlacement") for p in placements for c in p.get("children", []) if c.get("patientPathwayPlacement", {}).get("name") == nexus_case.pathway_name), None)
            if placement is None:
                raise ValueError(f"Could not find pathway placement '{nexus_case.pathway_name}'")

            form["pathwayAssociation"]["placement"] = placement

        # Apply the field values from the nexus_case to the form items
        unmatched_labels = set(fields_by_name)
        for item in form.get("items", []):
            field = fields_by_name.get(item.get("label"))
            if field is not None:
                field.apply(item)
                unmatched_labels.discard(item.get("label"))

        if unmatched_labels:
            available_labels = sorted({item.get("label") for item in form.get("items", [])})
            raise ValueError(
                f"Could not find form item(s) for field(s): {sorted(unmatched_labels)}.\n"
                f"Available form item labels: {available_labels}"
            )

        return form

    def create_form(self, form: dict, action_name: str) -> dict:
        """Create the specified form by performing the given action."""
        if not form.get("pathwayAssociation", {}).get("placement"):
            raise ValueError("Form does not have a pathway placement set")

        missing_required_fields = [i.get("label") for i in form.get("items", []) if i.get("required") and i.get("value") in (None, "", [])]
        if missing_required_fields:
            raise ValueError(f"Required form fields are missing values: {missing_required_fields}")

        actions = self._follow(form, "availableActions")
        action = next((a for a in actions if a.get("name") == action_name), None)
        if action is None:
            raise ValueError(f"Could not find action '{action_name}' in availableActions")

        created_form = self._follow(action, "createFormData", method="post", json=form)
        return created_form
