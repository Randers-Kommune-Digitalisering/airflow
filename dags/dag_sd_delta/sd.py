import logging
import xml.etree.ElementTree as ET
from io import BytesIO
from itertools import product
from datetime import datetime

import pandas as pd
from requests import Response
from requests.exceptions import HTTPError

from airflow.providers.http.hooks.http import HttpHook


logger = logging.getLogger(__name__)
# TODO: HttpHook er altid by default sat til POST i forvejen
SD_HTTP_HOOK = HttpHook(method="POST", http_conn_id="sd_silkeborgdata")

# TODO: add doc string
def _xml_to_df_with_exploded_elements(
    xml_content: bytes,
    primary_tag: str = "Person",
    secondary_tag: str = "Employment",
    tertiary_tags: tuple[str, ...] = ("EmploymentStatus", "Department", "Profession"),
) -> pd.DataFrame:
    # TODO: add doc string
    def xml_records(xml_fragment: bytes, xpath: str) -> list[dict]:
        try:
            df = pd.read_xml(BytesIO(xml_fragment), xpath=xpath, dtype=str)
        except ValueError:
            return []
        if df is None or df.empty:
            return []
        return df.where(pd.notna(df), None).to_dict(orient='records')

    root = ET.fromstring(xml_content)
    rows = []
    tertiary_tags_set = set(tertiary_tags)

    for primary_node in root.findall(f'.//{primary_tag}'):
        primary_values = {
            child.tag.rsplit('}', 1)[-1]: ((child.text or '').strip() or None)
            for child in primary_node
            if child.tag.rsplit('}', 1)[-1] != secondary_tag and len(list(child)) == 0
        }

        for secondary_node in primary_node.findall(f'./{secondary_tag}'):
            secondary_values = {
                child.tag.rsplit('}', 1)[-1]: ((child.text or '').strip() or None)
                for child in secondary_node
                if child.tag.rsplit('}', 1)[-1] not in tertiary_tags_set
                and len(list(child)) == 0
            }

            secondary_xml = ET.tostring(secondary_node, encoding='utf-8')
            extracted_records = [
                xml_records(secondary_xml, f'.//{tertiary_tag}') or [None]
                for tertiary_tag in tertiary_tags
            ]

            for record_tuple in product(*extracted_records):
                row = {**primary_values, **secondary_values}
                for tertiary_tag, tertiary_record in zip(tertiary_tags, record_tuple):
                    if isinstance(tertiary_record, dict):
                        row.update({f'{tertiary_tag}_{k}': v for k, v in tertiary_record.items()})
                rows.append(row)

    df = pd.DataFrame(rows)

    def parse_date_value(value):
        if pd.isna(value):
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            return None

    for column_name in df.columns:
        if 'Date' in column_name:
            df[column_name] = df[column_name].apply(parse_date_value)
        else:
            df[column_name] = df[column_name].apply(
                lambda value: None if pd.isna(value) else str(value)
            )

    return df


def _check_for_fault(response: Response) -> None:
    # TODO: Update doc string with input and output types
    """Raise response status, checks if the response contains a SOAP Fault and raises an HTTPError if it does."""
    response.raise_for_status()
    if "<Fault>" in response.text:
        response.status_code = 400
        root = ET.fromstring(response.text)
        fault = root.find(".//{*}Fault")
        if fault is not None:
            fault_str = ET.tostring(fault, encoding="unicode")
        else:
            fault_str = response.text
        err = HTTPError(fault_str)
        err.response = response
        raise err


def get_institutions_df() -> pd.DataFrame:
    # TODO: Update doc string with input and output types
    """Fetches all institutions from Silkeborg Data and returns them as a DataFrame."""
    res = SD_HTTP_HOOK.run(
        endpoint="/GetInstitution20080201",
        params={'RegionIdentifier': '9R'}
    )
    _check_for_fault(response=res)
    return pd.read_xml(BytesIO(res.content), xpath='.//Institution')


def get_professions_xml(inst_id: str) -> ET.Element:
    # TODO: Update doc string with input and output types
    """Fetch all professions XML for a given institution and return the XML root element."""
    res = SD_HTTP_HOOK.run(
        endpoint="/GetProfession20080201",
        params={'InstitutionIdentifier': inst_id}
    )
    _check_for_fault(response=res)
    root = ET.fromstring(res.content)
    professions_root = ET.Element("Professions")
    for profession_node in root.findall("./{*}Profession"):
        professions_root.append(profession_node)
    return professions_root


def get_departments_df(inst_id: str, activation_date: datetime, deactivation_date: datetime) -> pd.DataFrame:
    # TODO: Update doc string with input and output types
    """Fetches all departments for a given institution from SD and returns them as a DataFrame."""
    res = SD_HTTP_HOOK.run(
        endpoint="/GetDepartment20111201",
        params={
            'InstitutionIdentifier': inst_id,
            'ActivationDate': activation_date.strftime("%Y-%m-%dT%H:%M:%S"),
            'DeactivationDate': deactivation_date.strftime("%Y-%m-%dT%H:%M:%S"),
            'DepartmentNameIndicator': True
        }
    )
    _check_for_fault(response=res)
    return pd.read_xml(BytesIO(res.content), xpath='.//Department')


def get_persons_df(inst_id: str, effective_date: datetime) -> pd.DataFrame:
    # TODO: Update doc string with input and output types
    """Fetches all persons for a given institution from SD and returns them as a DataFrame."""
    res = SD_HTTP_HOOK.run(
        endpoint="/GetPerson",
        params={
            "InstitutionIdentifier": inst_id,
            "EffectiveDate": effective_date.strftime("%Y-%m-%d"),
            "StatusActiveIndicator": True,
            "StatusPassiveIndicator": False
        }
    )
    _check_for_fault(response=res)
    return _xml_to_df_with_exploded_elements(xml_content=res.content)


def get_employments_with_changes_df(inst_id: str, activation_datetime: datetime, deactivation_datetime: datetime) -> pd.DataFrame:
    # TODO: Update doc string with input and output types
    """Return one row per Employment with Person fields copied onto each employment row."""
    res = SD_HTTP_HOOK.run(
        endpoint="/GetEmploymentChangedAtDate20070401",
        params={
            'InstitutionIdentifier': inst_id,
            'ActivationDate': activation_datetime.strftime("%Y-%m-%d"),
            'DeactivationDate': deactivation_datetime.strftime("%Y-%m-%d"),
            'ActivationTime': activation_datetime.strftime("%H:%M:%S"),
            'DeactivationTime': deactivation_datetime.strftime("%H:%M:%S"),
            'EmploymentStatusIndicator': True,
            'ProfessionIndicator': True,
            'DepartmentIndicator': True,
            'SalaryCodeGroupIndicator': False,
            'WorkingTimeIndicator': False,
            'SalaryAgreementIndicator': False,
            'FutureInformationIndicator': True
        }
    )
    _check_for_fault(response=res)
    df = _xml_to_df_with_exploded_elements(xml_content=res.content)

    # Keep lightweight flags indicating which sections were explicitly marked as changed.
    for section_name in ("EmploymentStatus", "Department", "Profession"):
        changed_at_column = f"{section_name}_changedAtDate"
        has_change_column = f"{section_name}_HasChangedAtDate"
        if changed_at_column in df.columns:
            df[has_change_column] = df[changed_at_column].notna()
        else:
            df[has_change_column] = False

    changed_at_columns = [column_name for column_name in df.columns if column_name.endswith("changedAtDate")]
    df = df.drop(columns=changed_at_columns)

    expected_columns = [
        "PersonCivilRegistrationIdentifier",
        "EmploymentIdentifier",
        "EmploymentDate",
        "Department_DepartmentIdentifier",
        "Department_ActivationDate",
        "Department_DeactivationDate",
        "Profession_JobPositionIdentifier",
        "Profession_EmploymentName",
        "Profession_ActivationDate",
        "Profession_DeactivationDate",
        "Profession_AppointmentCode",
        "EmploymentStatus_EmploymentStatusCode",
        "EmploymentStatus_ActivationDate",
        "EmploymentStatus_DeactivationDate",
        "EmploymentStatus_HasChangedAtDate",
        "Department_HasChangedAtDate",
        "Profession_HasChangedAtDate",
    ]

    for column_name in expected_columns:
        if column_name not in df.columns:
            df[column_name] = None

    return df


def get_employment_on_date_df(inst_id: str, cpr: str, employment_id: str, effective_date: datetime) -> pd.DataFrame:
    # TODO: Update doc string with input and output types
    """Return dataframe with one ow for the Employment for the given cpr and employment_id."""
    res = SD_HTTP_HOOK.run(
        endpoint="/GetEmployment20070401",
        params={
            'InstitutionIdentifier': inst_id,
            'EmploymentIdentifier': employment_id,
            'PersonCivilRegistrationIdentifier': cpr,
            'EffectiveDate': effective_date.strftime("%Y-%m-%d"),
            'StatusActiveIndicator': True,
            # 'StatusPassiveIndicator': False, # TODO Clean up hvis det ikke skal bruges
            'StatusPassiveIndicator': True,
            'EmploymentStatusIndicator': True,
            'ProfessionIndicator': True,
            'DepartmentIndicator': True,
            'SalaryCodeGroupIndicator': False,
            'WorkingTimeIndicator': False,
            'SalaryAgreementIndicator': False
        }
    )
    _check_for_fault(response=res)
    df = _xml_to_df_with_exploded_elements(xml_content=res.content)
    if df.empty:
        # raise ValueError(f"No employment found in Institution {inst_id} for EmploymentIdentifier {employment_id} on {effective_date.strftime('%Y-%m-%d')}")
        logger.warning(f"No employment found in Institution {inst_id} for EmploymentIdentifier {employment_id} on {effective_date.strftime('%Y-%m-%d')}")
    return df


def get_person_on_date_df(inst_id: str, cpr: str, employment_id: str, effective_date: datetime) -> pd.DataFrame:
    # TODO: Update doc string with input and output types
    """Return dataframe with one row for the Person for the given cpr and employment_id."""
    res = SD_HTTP_HOOK.run(
        endpoint="/GetPerson",
        params={
            'InstitutionIdentifier': inst_id,
            'EmploymentIdentifier': employment_id,
            'PersonCivilRegistrationIdentifier': cpr,
            'EffectiveDate': effective_date.strftime("%Y-%m-%d"),
            'StatusActiveIndicator': True,
            'StatusPassiveIndicator': True,
            'ContactInformationIndicator': False,
            'PostalAddressIndicator': False
        }
    )
    _check_for_fault(reponse=res)
    root = ET.fromstring(res.content)
    person_nodes = root.findall('.//{*}Person')

    if not person_nodes:
        logger.warning(
            f"No person found in Institution {inst_id} for EmploymentIdentifier {employment_id} on {effective_date.strftime('%Y-%m-%d')}"
        )
        return pd.DataFrame()

    if len(person_nodes) > 1:
        logger.warning(
            f"Multiple persons found in Institution {inst_id} for EmploymentIdentifier {employment_id} on {effective_date.strftime('%Y-%m-%d')}"
        )
        return pd.DataFrame()

    person_node = person_nodes[0]
    person_row = {
        child.tag.rsplit('}', 1)[-1]: ((child.text or '').strip() or None)
        for child in person_node
        if len(list(child)) == 0
    }

    employment_identifiers = []
    for employment_node in person_node.findall('./{*}Employment'):
        employment_identifier_node = employment_node.find('./{*}EmploymentIdentifier')
        if employment_identifier_node is None:
            continue
        value = (employment_identifier_node.text or '').strip()
        if value:
            employment_identifiers.append(value)
    person_row['EmploymentIdentifiers'] = employment_identifiers

    return pd.DataFrame([person_row])


def employment_exists_on_date(inst_id: str, cpr: str, employment_id: str, effective_date: datetime) -> bool:
    # TODO: Update doc string with input and output types
    """Check if an employment exists for the given cpr and employment_id on the effective_date."""
    res = SD_HTTP_HOOK.run(
        endpoint="/GetEmployment20070401",
        params={
            'InstitutionIdentifier': inst_id,
            'EmploymentIdentifier': employment_id,
            'PersonCivilRegistrationIdentifier': cpr,
            'EffectiveDate': effective_date.strftime("%Y-%m-%d"),
            'StatusActiveIndicator': True,
            'StatusPassiveIndicator': True,
            'EmploymentStatusIndicator': True,
            'ProfessionIndicator': True,
            'DepartmentIndicator': True,
            'SalaryCodeGroupIndicator': False,
            'WorkingTimeIndicator': False,
            'SalaryAgreementIndicator': False
        }
    )

    # In this endpoint, a non-existing employment may be returned as a SOAP Fault.
    if "<Fault>" in res.text:
        root = ET.fromstring(res.text)
        fault_string = root.findtext(".//{*}faultstring") or ""
        if "does not exist" in fault_string and "EmploymentIdentifier" in fault_string:
            return False

    _check_for_fault(response=res)
    df = _xml_to_df_with_exploded_elements(xml_content=res.content)
    if df.empty:
        return False
    return True
