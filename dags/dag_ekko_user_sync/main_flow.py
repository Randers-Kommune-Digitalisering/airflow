import io
import json
import pycurl
import logging
import pendulum
import pandas as pd

from datetime import datetime

from lxml import etree
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.ftp.hooks.ftp import FTPHook


logger = logging.getLogger(__name__)


def get_ekko_sd_departments(ekko_sd_departments_str: str, sd_http_hook: HttpHook) -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    """
    Fetches department identifiers and names from the Silkeborg Data API
    and filters them based on a predefined list of department identifiers.
    Returns:
    - A pandas DataFrame containing all department identifiers and names.
    - A list of tuples containing filtered department identifiers (strings) and names (strings) for Ekko SD sync. e.g. [("<id>", "<name>"), ...]
    """
    if not ekko_sd_departments_str:
        raise ValueError("Variable 'ekko_sd_departments' is not set or is empty")
    ekko_sd_department_ids = json.loads(ekko_sd_departments_str)

    res = sd_http_hook.run(
        endpoint="/GetDepartment20111201",
        params={
            'InstitutionIdentifier': 'RG',
            'ActivationDate': pendulum.today().format("YYYY-MM-DD"),
            'DeactivationDate': pendulum.today().format("YYYY-MM-DD"),
            'DepartmentNameIndicator': True
        }
    )

    res.raise_for_status()

    all_sd_departments_df = pd.read_xml(res.text, xpath='.//Department')
    ekko_sd_departments_list = all_sd_departments_df[all_sd_departments_df['DepartmentIdentifier'].isin(ekko_sd_department_ids)][['DepartmentIdentifier', 'DepartmentName']].apply(tuple, axis=1).tolist()
    return all_sd_departments_df, ekko_sd_departments_list


def get_ekko_sd_user_data(sd_departments_task_id: str, sd_http_hook: HttpHook, **context) -> pd.DataFrame:
    """
    Get user data DataFrame from SD client.
    The function retrieves department data from a previous task, then for each department,
    it fetches employment and person data from the SD API and returns a DataFrame with relevant information for Ekko user synchronization.
    """
    all_sd_departments_df, ekko_sd_departments_list = context['ti'].xcom_pull(task_ids=sd_departments_task_id)

    ekko_employees_df = pd.DataFrame(columns=['Navn', 'Personalenr.', 'Email', 'MasterGroup', 'UserGroup', 'Titel', 'Fødselsdag', 'Ansættelsesdato', 'Mobiltelefonnr.', 'occupation_rate'])

    res = sd_http_hook.run(
        endpoint="/GetOrganization",
        params={
            'RegionCode': '9r',
            'InstitutionCode': 'RG'
        }
    )

    res.raise_for_status()
    root = etree.fromstring(res.content)
    org = []
    region = root.find('.//Region')
    if region is not None:
        institution = region.find('Institution')
        if institution is not None:
            for dept_elem in institution.findall('Department'):
                org.append(_parse_department(dept_elem))

    for sd_department in ekko_sd_departments_list:
        sd_id = sd_department[0]
        sd_name = sd_department[1]

        res = sd_http_hook.run(
            endpoint="/GetEmployment20070401",
            params={
                'InstitutionIdentifier': "RG",
                'DepartmentIdentifier': sd_id,
                'EffectiveDate': pendulum.today().format("YYYY-MM-DD"),
                'DepartmentLevelCode': 0,
                'StatusActiveIndicator': True,
                'EmploymentStatusIndicator': True,
                'ProfessionIndicator': True,
                'WorkingTimeIndicator': True
            }
        )

        res.raise_for_status()
        root = etree.fromstring(res.content)
        persons_objs = root.xpath("//Person")

        employees = []
        for p in persons_objs:
            cpr = p.find('PersonCivilRegistrationIdentifier').text if p.find('PersonCivilRegistrationIdentifier') is not None else None
            if not cpr:
                continue
            for e in p.xpath('.//Employment'):
                emp_id = e.find('EmploymentIdentifier')
                emp_date = e.find('EmploymentDate')
                employment_date = emp_date.text if emp_date is not None else None
                employment_id = emp_id.text if emp_id is not None else None
                profession = e.find('Profession/EmploymentName').text if e.find('Profession/EmploymentName') is not None else None
                occupation_rate = float(e.find('WorkingTime/OccupationRate').text) if e.find('WorkingTime/OccupationRate') is not None else 0
                employees.append({'cpr': cpr, 'employment_id': employment_id, 'employment_date': employment_date, 'profession': profession, 'occupation_rate': occupation_rate})

        res = sd_http_hook.run(
            endpoint="/GetPerson",
            params={
                'InstitutionIdentifier': "RG",
                'DepartmentIdentifier': sd_id,
                'EffectiveDate': pendulum.today().format("YYYY-MM-DD"),
                'DepartmentLevelCode': 0,
                'StatusActiveIndicator': True,
                'ContactInformationIndicator': True
            }
        )

        res.raise_for_status()
        root = etree.fromstring(res.content)
        persons_objs = root.xpath("//Person")

        persons = []
        for p in persons_objs:
            cpr = p.find('PersonCivilRegistrationIdentifier').text
            person_phones = p.xpath('./ContactInformation/TelephoneNumberIdentifier/text()')
            person_emails = p.xpath('./ContactInformation/EmailAddressIdentifier/text()')
            first_name = p.find('PersonGivenName').text
            last_name = p.find('PersonSurnameName').text
            name = f"{first_name} {last_name}"
            employment_ids = []
            employment_phones = []
            employment_emails = []
            for e in p.xpath('./Employment'):
                emp_id = e.find('EmploymentIdentifier')
                emp_phones = e.xpath('./ContactInformation/TelephoneNumberIdentifier/text()')
                emp_emails = e.xpath('./ContactInformation/EmailAddressIdentifier/text()')
                employment_ids.append(emp_id.text)
                employment_phones.extend(emp_phones)
                employment_emails.extend(emp_emails)

            all_emails = person_emails + employment_emails
            randers_email = next((email.lower() for email in all_emails if email.lower().endswith('@randers.dk')), None)

            person_dict = {
                'cpr': cpr,
                'name': name,
                'employment_ids': employment_ids if employment_ids else [],
                'person_phones': person_phones if person_phones else [],
                'employment_phones': employment_phones if employment_phones else [],
                'email': randers_email
            }
            persons.append(person_dict)

        for employment_dict in employees:
            person_dict = next((p for p in persons if employment_dict['cpr'] in p['cpr']), None)
            if not person_dict:
                raise ValueError(f"No person found for employment based on CPR - employment id: {employment_dict['employment_id']}")

            name = person_dict['name']
            employment_id = employment_dict['employment_id']
            email = person_dict['email']

            master_group_id = _find_level3_parent_code(org=org, child_code=sd_id)
            master_group = all_sd_departments_df.loc[all_sd_departments_df['DepartmentIdentifier'] == master_group_id, 'DepartmentName'].squeeze() if master_group_id else None

            user_group = sd_name
            profession = employment_dict['profession']
            birth_day = _get_birth_date_from_cpr(employment_dict['cpr'])
            employment_date = employment_dict['employment_date']
            mobile_phone = _get_mobile_number_from_person(person_dict)
            occupation_rate = employment_dict['occupation_rate']

            ekko_employees_df.loc[len(ekko_employees_df)] = [name, employment_id, email, master_group, user_group, profession, birth_day, employment_date, mobile_phone, occupation_rate]

    # Sort by occupation rate descending, then drop duplicates based on 'Personalenr.' keeping the first (which has the highest occupation rate)
    # Finally, drop the 'occupation_rate' column as it's no longer needed
    ekko_employees_df = (
        ekko_employees_df
        .sort_values('occupation_rate', ascending=False)
        .drop_duplicates(subset='Personalenr.', keep='first')
        .drop(columns='occupation_rate')
        .reset_index(drop=True)
    )
    return ekko_employees_df


def upload_ekko_users(sd_user_data_task_id: str, ekko_ftps_hook: FTPHook, **context) -> bool:
    """"
    Upload the Ekko user data CSV file to the Ekko FTPS server using pycurl for secure upload.
    The function retrieves the user data DataFrame from a previous task, converts it to CSV format, and uploads it to the FTPS server (from FTPHook).

    Using pycurl because the standard FTPHook does not support implicit FTPS which is required by the Ekko FTPS server.
    """
    ekko_users_df = context['ti'].xcom_pull(task_ids=sd_user_data_task_id)

    csv_file = io.BytesIO()
    ekko_users_df.to_csv(csv_file, index=False, sep=';', encoding='cp1252')
    csv_file.seek(0)
    filename = f"ejendomme-og-drift-brugere-{pendulum.today().format('DD-MM-YYYY')}.csv"

    conn = BaseHook.get_connection(ekko_ftps_hook.ftp_conn_id)
    host = conn.host
    if not host.startswith("ftps://"):
        host = "ftps://" + host
    username = conn.login
    password = conn.password

    file_url = f"{host.rstrip('/')}/{filename}"

    c = pycurl.Curl()
    c.setopt(c.URL, file_url)
    c.setopt(c.USERPWD, f"{username}:{password}")
    c.setopt(c.SSL_VERIFYPEER, 0)
    c.setopt(c.SSL_VERIFYHOST, 0)
    c.setopt(c.UPLOAD, 1)
    c.setopt(c.READDATA, csv_file)
    c.setopt(c.FTP_SSL, pycurl.FTPSSL_ALL)
    c.setopt(c.FTPSSLAUTH, pycurl.FTPAUTH_TLS)
    c.setopt(c.FTP_USE_EPSV, 1)
    c.perform()
    c.close()

    logger.info(f"File {filename} uploaded successfully to {file_url}")

    return True


def _parse_department(dept_elem: etree._Element) -> dict:
    """Recursively parse department XML element into a dictionary with sub-departments."""
    dept = {
        'DepartmentCode': dept_elem.findtext('DepartmentCode'),
        'DepartmentLevel': dept_elem.findtext('DepartmentLevel'),
        'Departments': []
    }
    for child in dept_elem.findall('Department'):
        dept['Departments'].append(_parse_department(child))
    return dept


def _check_if_mobile_number_and_clean(number: str) -> str | bool:
    """
    Check if the given number is a valid mobile number and clean it.
    Based on Danish mobile number rules: https://guldnummer.com/tjek-nummer

    :param number: Phone number to check
    :type number: str
    :return: Cleaned mobile number if valid, otherwise False
    :rtype: str | bool
    """
    FIRST_NUMBER_FOR_MOBILE = [2, 30, 31, 40, 41, 42, 50, 51, 52, 53, 60, 61, 71, 81, 91, 92, 93]

    if len(number) > 8:
        number = number[-8:]

    if len(number) == 8 and (int(number[0]) in FIRST_NUMBER_FOR_MOBILE or int(number[:2]) in FIRST_NUMBER_FOR_MOBILE):
        return number
    else:
        return False


def _find_level3_parent_code(org: list[dict], child_code: str) -> str | None:
    """Find the level 3 parent department code for a given child department code."""
    for dept in org:
        if _contains_department(dept, child_code):
            if dept['DepartmentLevel'] == '3':
                return dept['DepartmentCode']
            result = _find_level3_parent_code(dept.get('Departments', []), child_code)
            if result:
                return result
    return None


def _contains_department(dept: dict, target_code: str) -> bool:
    """Check if a department or its sub-departments contain the target department code."""
    if dept['DepartmentCode'] == target_code:
        return True
    return any(_contains_department(sub, target_code) for sub in dept.get('Departments', []))


def _get_mobile_number_from_person(person: dict) -> str | None:
    """
    Attempt to retrieve a mobile number from the provided person dict.
    Prioritizes employment phone numbers over personal phone numbers.

    :param person: Dictionary containing employment and personal phone numbers. Keys 'employment_phones' and 'person_phones' should contain lists.
    :type person: dict
    :return: Mobile phone number if found, otherwise None
    :rtype: str | None
    """
    mobile_number = None
    for num in person['employment_phones']:
        mobile_number = _check_if_mobile_number_and_clean(num)
        if mobile_number:
            break
    if not mobile_number:
        for num in person['person_phones']:
            mobile_number = _check_if_mobile_number_and_clean(num)
            if mobile_number:
                break
    return mobile_number


def _get_century_from_cpr(cpr_number: str) -> int:
    """
    Determine the century of birth from a CPR number, using the control digit rules.
    Rules based on Danish CPR number system: https://www.cpr.dk/cpr-systemet/opbygning-af-cpr-nummeret

    :param cpr_number: CPR number string
    :type cpr_number: str
    :return: Century of birth (e.g., 1900, 2000, 1800)
    :rtype: int
    """
    first_control_digit = int(cpr_number[6])
    if first_control_digit in [0, 1, 2, 3]:
        return 1900
    short_year = int(cpr_number[4:6])
    if first_control_digit in [4, 9]:
        if short_year >= 37:
            return 1900
        else:
            return 2000
    elif first_control_digit in [5, 6, 7, 8]:
        if short_year <= 57:
            return 2000
        else:
            return 1800


def _get_birth_date_from_cpr(cpr_number: str) -> str:
    """
    Extract and format the birth date from a CPR number.

    :param cpr_number: CPR number string
    :type cpr_number: str
    :return: Birth date in DD-MM-YYYY format
    :rtype: str
    """
    century = _get_century_from_cpr(cpr_number)
    dt = datetime(year=century + int(cpr_number[4:6]), month=int(cpr_number[2:4]), day=int(cpr_number[0:2]))
    return dt.strftime("%d-%m-%Y")
