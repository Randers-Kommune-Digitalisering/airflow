import io
import logging
import re
import pandas as pd
from typing import Any
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import get_current_context
from rkdigi.email_handling import EmailReader, EmailSender

from dag_social_frikort.social_frikort_data import (
    extract_social_frikort_address_rows,
    extract_social_frikort_income_rows,
    get_social_frikort_report_fields,
    social_frikort_income_columns,
    write_social_frikort_report_excel_bytes,
)
from utils.mail_attachments import find_latest_attachment
from utils.cpr_excel import extract_cprs

logger = logging.getLogger(__name__)


def _get_missing_or_empty_keys(config: dict, required_keys: tuple[str, ...]) -> list[str]:
    """
    Return required keys that are missing or have an empty value.

    :param config: Configuration dictionary to validate.
    :param required_keys: Keys that must be present and non-empty.
    :return: Missing/empty key names.
    """
    return [key for key in required_keys if not config.get(key)]


def _resolve_month_interval() -> tuple[str, str]:
    """
    Resolve and validate month interval from DAG runtime params.

    :return: Tuple containing (start_month, end_month).
    :raises AirflowFailException: If required params are missing or the interval is invalid.
    """
    ctx = get_current_context()
    start_month = (ctx.get("params") or {}).get("start_month")
    end_month = (ctx.get("params") or {}).get("end_month")

    if not start_month or not end_month:
        raise AirflowFailException("Need to specify both start_month and end_month (YYYYMM).")

    start_month = str(start_month).strip()
    end_month = str(end_month).strip()

    regex_pattern = r"^\d{4}(?:0[1-9]|1[0-2])$"
    if not (re.fullmatch(regex_pattern, start_month) and re.fullmatch(regex_pattern, end_month)):
        raise AirflowFailException("start_month and end_month must be in YYYYMM format.")

    if start_month > end_month:
        raise AirflowFailException("start_month must not be after end_month.")

    return start_month, end_month


def process_social_frikort() -> None:
    """Read CPRs from the mailbox and email income and address information."""

    logger.info("Starting Social Frikort processing.")

    # Get and validate runtime configurations from Airflow Variables
    skat_client_config = Variable.get("skat_client_config", deserialize_json=True)
    social_frikort_runtime_config = Variable.get("social_frikort_runtime_config", deserialize_json=True)
    if not isinstance(skat_client_config, dict):
        raise AirflowFailException("SKAT client configuration must be a JSON object.")
    if not isinstance(social_frikort_runtime_config, dict):
        raise AirflowFailException("Social Frikort runtime configuration must be a JSON object.")

    skat_required_keys = (
         "virksomhed_se_nummer_identifikator",
         "abonnement_type_kode",
         "abonnent_type_kode",
         "adgang_formaal_type_kode",
    )
    missing_skat_keys = _get_missing_or_empty_keys(skat_client_config, skat_required_keys)
    if missing_skat_keys:
        raise AirflowFailException(
            f"SKAT client configuration is missing required keys: {', '.join(missing_skat_keys)}"
        )

    virksomhed_se_nummer_identifikator = skat_client_config.get("virksomhed_se_nummer_identifikator")
    abonnement_type_kode = skat_client_config.get("abonnement_type_kode")
    abonnent_type_kode = skat_client_config.get("abonnent_type_kode")
    adgang_formaal_type_kode = skat_client_config.get("adgang_formaal_type_kode")

    runtime_required_keys = ("sender_email", "recipient_emails", "smtp_server", "imap_server")
    missing_runtime_keys = _get_missing_or_empty_keys(social_frikort_runtime_config, runtime_required_keys)
    if missing_runtime_keys:
        raise AirflowFailException(
            "Social Frikort runtime configuration is missing required keys: "
            f"{', '.join(missing_runtime_keys)}"
        )

    report_fields = get_social_frikort_report_fields()
    sender = social_frikort_runtime_config.get("sender_email")
    recipients = social_frikort_runtime_config.get("recipient_emails")
    smtp_server = social_frikort_runtime_config.get("smtp_server")
    imap_server = social_frikort_runtime_config.get("imap_server")

    social_frikort_imap_conn = BaseHook.get_connection("social_frikort_imap")
    if not all((social_frikort_imap_conn.login, social_frikort_imap_conn.password, imap_server)):
        raise AirflowFailException("Social Frikort IMAP credentials are not configured.")

    email_reader = EmailReader(
        email=social_frikort_imap_conn.login,
        password=social_frikort_imap_conn.password,
        imap_server=imap_server,
    )
    found = find_latest_attachment(
            email_reader=email_reader,
            filename_prefixes="Social",
        )
    if not found:
        raise AirflowFailException("No matching CPR-list attachment found in the mailbox.")

    uid, filename, content_bytes = found
    logger.info(f"Found attachment: {filename} (UID: {uid.decode()}) with size {len(content_bytes)} bytes")

    try:
        # Expected input file format: an Excel sheet where CPR numbers are in the "Cprnr." column
        cpr_column = "Cprnr."
        input_df = pd.read_excel(
            io.BytesIO(content_bytes),
            engine="openpyxl",
            dtype={cpr_column: str},
            sheet_name=0,
        )
        cpr_list = extract_cprs(df=input_df, column=cpr_column)
        if not cpr_list:
            raise AirflowFailException("No CPR values found in the Social Frikort Excel file.")

        start_month, end_month = _resolve_month_interval()

        logger.info(f"Social Frikort date range: {start_month} -> {end_month}")

        from utils.kombit import TempClientCert
        from kombit_client.integrations.sf0770a import SKATForwardEIndkomstClient
        from kombit_client.integrations.sf1520 import PersonBaseDataExtendedClient

        report_rows: list[dict[str, Any]] = []
        address_rows: list[dict[str, Any]] = []
        with TempClientCert() as client_cert_path:
            skat_client = SKATForwardEIndkomstClient(
                virksomhed_se_nummer_identifikator=virksomhed_se_nummer_identifikator,
                abonnement_type_kode=abonnement_type_kode,
                abonnent_type_kode=abonnent_type_kode,
                adgang_formaal_type_kode=adgang_formaal_type_kode,
                client_certificate_file_path=client_cert_path,
            )
            person_client = PersonBaseDataExtendedClient(
                client_certificate_file_path=client_cert_path,
            )

            total_cprs = len(cpr_list)
            for idx, cpr in enumerate(cpr_list, start=1):
                logger.info(f"Processing CPR {idx}/{total_cprs}")
                try:
                    payload = skat_client.indkomstoplysninger_laes(
                        person_civil_registration_identifier=cpr,
                        soege_aar_maaned_fra_kode=start_month,
                        soege_aar_maaned_til_kode=end_month,
                    )
                    rows = extract_social_frikort_income_rows(payload=payload, report_fields=report_fields)
                except Exception as error:
                    logger.error(f"SKAT lookup failed for CPR row {idx}/{total_cprs} ({type(error).__name__}); CPR will be retained with empty income fields.")
                    rows = []

                # Keep a row without income when no records match, and separate adjacent CPR blocks.
                report_rows.extend(rows or [{
                    column: cpr if column == "cpr" else None
                    for column in social_frikort_income_columns(report_fields)
                }])
                if idx != total_cprs:
                    report_rows.append({
                        column: None for column in social_frikort_income_columns(report_fields)
                    })

                try:
                    person_payload = person_client.person_lookup(pnr=cpr)
                    address_rows.extend(extract_social_frikort_address_rows(payload=person_payload, cpr=cpr))
                except Exception as error:
                    logger.error(f"Address lookup failed for CPR row {idx}/{total_cprs} ({type(error).__name__}); CPR will be retained without address.")
                    address_rows.append({"cpr": cpr})

        excel_bytes = write_social_frikort_report_excel_bytes(rows=report_rows, report_fields=report_fields, address_rows=address_rows)
        context = get_current_context()
        report_date = context["logical_date"].in_timezone(context["dag"].timezone).date().isoformat()
        report_filename = f"SocialFrikort_{start_month}_til_{end_month}_{report_date}.xlsx"

        email_sender = EmailSender(smtp_server=smtp_server)
        email_sender.send_email(
            sender=sender,
            recipients=recipients,
            subject=f"Social Frikort indkomstoplysninger ({start_month} -> {end_month})",
            body="Indkomstoplysninger fra SKAT er vedhæftet som Excel.",
            attachments=[(report_filename, excel_bytes)],
        )
        logger.info("Social Frikort report sent successfully.")
    except Exception as e:
        raise AirflowFailException("Failed to process Social Frikort report") from e
    finally:
        # Ensure to delete the processed input email
        email_reader.delete_email_by_uid(uid=uid, mailbox="INBOX", expunge=True)
        logger.info("Deleted processed Social Frikort input email from INBOX.")
