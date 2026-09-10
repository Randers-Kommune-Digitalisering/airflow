import logging

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo
from airflow.hooks.base import BaseHook

from rkdigi import ManagedOAuth2Session

logger = logging.getLogger(__name__)

# Element names
TOP_PROGRAM_NAME = "Sundhed, Kultur og Omsorg"
STATE_ACTIVE_NAME = "Aktiv"
STATE_DEAD_TYPE_ID = "DEAD"

# Elements in the Nexus API that are specific to the assistive devices dashboard and its widgets/forms/assignments.
ASSISTIVE_DEVICES_DASHBOARD_NAME = "Dokumentation - Personlige hjælpemidler"
ASSISTIVE_DEVICES_DASHBOARD_DOCS_WIDGET_NAME = "Breve og dokumenter Personlige hjælpemidler"
ASSISTIVE_DEVICES_DASHBOARD_COMMUNICATION_WIDGET_NAME = "Henvendelse Visitation Personlige hjælpemidler"
ASSISTIVE_DEVICES_COMMUNICATION_FORM_TITLE = "Henvendelse Kropsbårne hjælpemidler"
ASSISTIVE_DEVICES_ASSIGNMENT_NAME = "PHJÆ Nye ansøgninger fra X-Flow"


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

    # Universal private helpers for navigating the Nexus API, which uses HAL (Hypertext Application Language) for hypermedia.
    def _link(self, obj: dict, rel: str) -> str:
        """Get the URL for a given HAL link relation from an object."""
        return obj.get("_links", {}).get(rel, {}).get("href")

    def _follow(self, obj: dict, rel: str, method: str = "get", **kwargs) -> dict:
        """Follow a HAL link relation from an object and return the resulting JSON response."""
        url = obj.get("_links", {}).get(rel, {}).get("href")
        if not url:
            available_rels = ", ".join(obj.get("_links", {}).keys()) or "<none>"
            raise ValueError(f"Missing HAL link rel '{rel}'. Available rels: {available_rels}")
        res = self.session.request(method.upper(), url, **kwargs)
        res.raise_for_status()
        return res.json()

    def _get(self, endpoint: str = None, params: dict | None = None) -> dict:
        """Perform a GET request to the Nexus API, optionally to a specific endpoint with query parameters."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}" if endpoint else self.base_url
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _post(self, endpoint: str = None, data: dict | None = None) -> dict:
        """Perform a POST request to the Nexus API, optionally to a specific endpoint with JSON data."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}" if endpoint else self.base_url
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

    # Private helpers
    def _get_patient_dashboard(self, patient_data: dict, dashboard_name: str) -> dict:
        """Get the dashboard data for a specific patient and dashboard name."""
        patient_preferences = self._follow(patient_data, "patientPreferences")
        dashboard_partial = next((item for item in patient_preferences["CITIZEN_DASHBOARD"] if item.get("name") == dashboard_name), None)
        if dashboard_partial is None:
            raise ValueError(f"Could not find '{dashboard_name}' in patient preferences")
        return self._follow(dashboard_partial, "self")

    def _get_widget(self, dashboard: dict, widget_header_title: str) -> dict:
        """Get a specific widget from a dashboard by its header title."""
        widget = next((item for item in dashboard["view"]["widgets"] if item.get("headerTitle") == widget_header_title), None)
        if widget is None:
            raise ValueError(f"Could not find widget with headerTitle '{widget_header_title}' in dashboard")
        return widget

    # Public methods
    def get_patient_data(self, cpr: str) -> dict | None:
        """Get the patient data for a specific CPR number."""
        home = self._get()
        patient_search = self._follow(home, "patients", params={"query": cpr})
        pages = patient_search.get("pages", [])
        if not pages:
            # raise ValueError("No patient data found for CPR")
            return None
        patient_data_search = self._follow(pages[0], "patientData")
        if len(patient_data_search) != 1:
            raise ValueError(f"Expected exactly one patient data entry, but found {len(patient_data_search)}")
        return self._follow(patient_data_search[0], "self")

    def is_active(self, patient_data: dict, set_if_not: bool = False) -> bool:
        """Check if a patient is currently active, optionally setting the state to active if not."""
        current_state = patient_data.get("patientState", {})

        # Always report 'DEAD' state as inactive - do not try to update
        if current_state.get("type", {}).get("id") == STATE_DEAD_TYPE_ID:
            # raise ValueError("State is 'Dead'")
            return False

        is_active = current_state.get("name") == STATE_ACTIVE_NAME  # Other states are considered active, but should be changed to this specific one.
        if not is_active and set_if_not:
            schedule = patient_data.get("patientStateValueSchedule", {})
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
            patient_data["patientState"] = state
            patient_data["patientStateStartDate"] = iso_start

            updated_patient = self._follow(obj=patient_data, rel="update", method="put", json=patient_data)
            return self.is_active(patient_data=updated_patient, set_if_not=False)
        return is_active

    def has_program(self, patient_data: dict, program_name: str, add_if_missing: bool = False) -> bool:
        """Check if a specific program is associated with a patient, optionally adding it if missing."""
        active_programs = self._follow(obj=patient_data, rel="activePrograms", method="get")
        exists = next((p for p in active_programs if p.get("name") == program_name), None) is not None
        if not exists and add_if_missing:
            # Add the program if it is missing
            available_programs = self._follow(obj=patient_data, rel="availableProgramPathways", method="get")
            program_to_add = next((p for p in available_programs if p.get("name") == program_name), None)
            if program_to_add is None:
                raise ValueError(f"Program '{program_name}' cannot be added because it is not available in the available program pathways")
            self._follow(obj=program_to_add, rel="enroll", method="put")
            return self.has_program(patient_data=patient_data, program_name=program_name, add_if_missing=False)
        return exists

    def add_assistive_device_document(self, patient_data: dict, date: date, name: str, file_name: str, file_bytes: bytes, mime_type: str) -> dict:
        """Add a document to the assistive devices dashboard for a specific patient."""        
        # Ensure the file name is valid
        file_name = "_".join(file_name.strip().rstrip(".").split())
        if not file_name:
            raise ValueError("File name cannot be empty")

        dashboard = self._get_patient_dashboard(patient_data, ASSISTIVE_DEVICES_DASHBOARD_NAME)
        widget = self._get_widget(dashboard, ASSISTIVE_DEVICES_DASHBOARD_DOCS_WIDGET_NAME)
        new_document = self._follow(widget["creatableObjects"], "documentPrototype")
        new_document["name"] = name
        new_document["originalFileName"] = file_name

        if new_document.get("relevanceDate", "").split("T")[0] != date.strftime("%Y-%m-%d"):
            local_midnight = datetime.combine(date, time.min, tzinfo=ZoneInfo("Europe/Copenhagen"))
            new_document["relevanceDate"] = local_midnight.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

        created_document = self._follow(new_document, "create", method="post", json=new_document)

        return self._follow(created_document, "upload", method="post", files={"file": (file_name, file_bytes, mime_type)})

    def create_assistive_device_communication_form(
            self,
            patient_data: dict,
            application_date: date,
            application_reason: str,
            communication_source: str,
            device: str,
            optional_contact_info: str | None = None,
            patient_understands: bool | None = None,
            can_information_be_obtained: bool | None = None
    ) -> dict:
        """Create a communication form for assistive devices for a specific patient."""
        # Field names
        APPLICATION_DATE_FIELD = "Ansøgningsdato"
        APPLICATION_REASON_FIELD = "Henvendelses årsag"
        COMMUNICATION_SOURCE_FIELD = "Henvendelseskilde"
        PATIENT_UNDERSTANDS_FIELD = "Er borgeren indforstået med henvendelsen?"
        PATIENT_INFORMED_FIELD = "Borger oplyst om ovenstående"
        INFORMATION_OBTAINED_FIELD = "Der er givet tilladelse til indhentning af oplysninger"
        DEVICE = "Hvad søges der om?"
        CONTACT_INFO_FIELD = "Uddyb med navn, telefonnummer m.m."
        # Action name
        COMPLETED_ACTION_NAME = "Udfyldt"

        dashboard = self._get_patient_dashboard(patient_data, ASSISTIVE_DEVICES_DASHBOARD_NAME)
        widget = self._get_widget(dashboard, ASSISTIVE_DEVICES_DASHBOARD_COMMUNICATION_WIDGET_NAME)

        forms = widget.get("creatableObjects", {}).get("forms", [])
        if not forms:
            raise ValueError("No forms found in widget.creatableObjects.forms")

        selected_form = next((form for form in forms if form.get("title") == ASSISTIVE_DEVICES_COMMUNICATION_FORM_TITLE), None)
        if selected_form is None:
            raise ValueError(
                f"Could not find form with title '{ASSISTIVE_DEVICES_COMMUNICATION_FORM_TITLE}'. "
            )

        form = self._follow(selected_form, "formDataPrototype")

        # If "forløb" 'Personlige hjælpemidler" is not already set, find it in the available pathway associations and set it.
        if form.get("pathwayAssociation", {}).get("placement") is None:
            available_pathway_associations = self._follow(
                obj=form.get("pathwayAssociation", {}),
                rel="availablePathwayAssociation",
                params={"subjectId": form.get("formDefinition", {}).get("uid")}
            )
            # NOTE: Hardcoded name for pathway association 'Sundhed, Kultur og Omsorg'
            sundhed_kultur_og_omsorg_association = next(
                (assoc for assoc in available_pathway_associations if assoc.get("patientPathwayPlacement", {}).get("name") == "Sundhed, Kultur og Omsorg"),
                None
            )
            if sundhed_kultur_og_omsorg_association is None:
                raise ValueError("Could not find 'Sundhed, Kultur og Omsorg' association in available pathway associations")

            # NOTE: Hardcoded name for pathway association 'Personlige hjælpemidler'
            personlige_hjaelpemidler_association = next(
                (assoc for assoc in sundhed_kultur_og_omsorg_association.get("children", []) if assoc.get("patientPathwayPlacement", {}).get("name") == "Personlige hjælpemidler"),
                None
            )
            placement = personlige_hjaelpemidler_association.get("patientPathwayPlacement")
            if placement is None:
                raise ValueError("Could not find 'Personlige hjælpemidler' placement in available pathway associations")
            form["pathwayAssociation"]["placement"] = placement

        def _get_dropdown_value_or_raise(field_item: dict, option_name: str) -> dict:
            option = next((v for v in field_item.get("possibleValues", []) if v.get("name") == option_name), None)
            if option is None:
                available_options = [v.get("name") for v in field_item.get("possibleValues", [])]
                raise ValueError(
                    f"Could not find option '{option_name}' for field '{field_item.get('label')}'. "
                    f"Available options: {available_options}"
                )
            return option

        # Populate the form with provided data
        for item in form.get("items", []):
            label = item.get("label")
            if label == APPLICATION_DATE_FIELD:
                # Nexus expects an ISO UTC timestamp string for date fields.
                local_midnight = datetime.combine(application_date, time.min, tzinfo=ZoneInfo("Europe/Copenhagen"))
                item["value"] = local_midnight.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            elif label == APPLICATION_REASON_FIELD:
                item["value"] = application_reason
            elif label == COMMUNICATION_SOURCE_FIELD:
                item["value"] = _get_dropdown_value_or_raise(item, communication_source)
            elif label == PATIENT_UNDERSTANDS_FIELD:
                # NOTE: Hardcoded options
                selected_name = "Uafklaret" if patient_understands is None else ("Ja" if patient_understands else "Nej")
                item["value"] = _get_dropdown_value_or_raise(item, selected_name)
            elif label == PATIENT_INFORMED_FIELD:
                # NOTE: Hardcoded options
                item["value"] = _get_dropdown_value_or_raise(item, "Skriftligt")
            elif label == INFORMATION_OBTAINED_FIELD:
                # NOTE: Hardcoded options
                selected_name = "Der er ikke taget stilling" if can_information_be_obtained is None else ("Ja" if can_information_be_obtained else "Nej")
                item["value"] = _get_dropdown_value_or_raise(item, selected_name)
            elif label == DEVICE:
                option = next((v for v in item.get("possibleValues", []) if v.get("name", "").lower() == device.lower()), None)
                if option is not None:
                    item["value"] = [option]
            elif label == CONTACT_INFO_FIELD and optional_contact_info:
                item["value"] = optional_contact_info

        actions = self._follow(form, "availableActions")
        action = next((a for a in actions if a.get("name") == COMPLETED_ACTION_NAME), None)
        if action is None:
            raise ValueError(f"Could not find action '{COMPLETED_ACTION_NAME}' in availableActions")

        created_form = self._follow(action, "createFormData", method="post", json=form)
        return created_form

    def get_auto_assignment(self, form: dict, assignment_name: str) -> dict:
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
        selected_assignment = next((a for a in assignments if a.get("type", {}).get("name") == assignment_name), None)
        if not selected_assignment:
            raise ValueError(f"Could not find assignment with type name '{assignment_name}' in response")

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
                self._follow(obj=created_form, rel="delete", method="delete")
                logger.warning(f"Rolled back form id={created_form.get('id')}")
            except Exception:
                logger.exception(f"Failed rolling back form id={created_form.get('id')}")

        for document in reversed(created_docs):
            try:
                self._follow(obj=document, rel="delete", method="delete")
                logger.warning(f"Rolled back document id={document.get('id')}")
            except Exception:
                logger.exception(f"Failed rolling back document id={document.get('id')}")
