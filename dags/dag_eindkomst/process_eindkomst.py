import logging
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import get_current_context
from rkdigi.email_handling import EmailReader, EmailSender

from dag_eindkomst.eindkomst_data import (
    extract_cpr_se_pairs_from_txt,
    extract_eindkomst_rows,
    get_eindkomst_report_config,
    write_eindkomst_report_excel_bytes,
)
from utils.mail_attachments import find_latest_attachment
from utils.config_validation import get_missing_or_empty_keys, resolve_month_interval

logger = logging.getLogger(__name__)


def process_eindkomst() -> None:
    """Read CPRs from a text attachment and email the SKAT income report."""

    logger.info("Starting eIndkomst processing")

    skat_client_config = Variable.get("skat_client_config", deserialize_json=True)
    eindkomst_runtime_config = Variable.get("eindkomst_runtime_config", deserialize_json=True)
    if not isinstance(skat_client_config, dict):
        raise AirflowFailException("SKAT client configuration must be a JSON object.")
    if not isinstance(eindkomst_runtime_config, dict):
        raise AirflowFailException("eIndkomst runtime configuration must be a JSON object.")

    skat_required_keys = (
        "virksomhed_se_nummer_identifikator", "abonnement_type_kode",
        "abonnent_type_kode", "adgang_formaal_type_kode",
    )

    missing_skat_keys = get_missing_or_empty_keys(skat_client_config, skat_required_keys)
    if missing_skat_keys:
        raise AirflowFailException(
            f"SKAT client configuration is missing required keys: {', '.join(missing_skat_keys)}"
        )

    eindkomst_required_keys = ("sender_email", "recipient_emails", "smtp_server", "imap_server")
    missing_eindkomst_keys = get_missing_or_empty_keys(eindkomst_runtime_config, eindkomst_required_keys)
    if missing_eindkomst_keys:
        raise AirflowFailException(
            f"eIndkomst runtime configuration is missing required keys: {', '.join(missing_eindkomst_keys)}"
        )

    columns, fields, column_descriptions = get_eindkomst_report_config()
    start_month, end_month = resolve_month_interval()

    logger.info(f"eIndkomst date range: {start_month} -> {end_month}")

    sender = eindkomst_runtime_config["sender_email"]
    recipients = eindkomst_runtime_config["recipient_emails"]
    smtp_server = eindkomst_runtime_config["smtp_server"]
    imap_server = eindkomst_runtime_config["imap_server"]

    virksomhed_se_nummer_identifikator = skat_client_config["virksomhed_se_nummer_identifikator"]
    abonnement_type_kode = skat_client_config.get("abonnement_type_kode")
    abonnent_type_kode = skat_client_config.get("abonnent_type_kode")
    adgang_formaal_type_kode = skat_client_config.get("adgang_formaal_type_kode")

    eindkomst_imap_conn = BaseHook.get_connection("eindkomst_imap")
    if not eindkomst_imap_conn.login or not eindkomst_imap_conn.password:
        raise AirflowFailException("eIndkomst IMAP credentials are not configured.")

    email_reader = EmailReader(
        email=eindkomst_imap_conn.login,
        password=eindkomst_imap_conn.password,
        imap_server=imap_server,
    )
    found = find_latest_attachment(
        email_reader=email_reader,
        criteria="ALL",
        extensions=(".txt",),
        filename_prefixes=("eindkomst",))

    if not found:
        raise AirflowFailException("No eIndkomst text attachment found in the mailbox.")

    uid, filename, content_bytes = found
    logger.info(f"Found attachment: {filename} (UID: {uid.decode()}) with size {len(content_bytes)} bytes")

    try:
        cpr_se_pairs = extract_cpr_se_pairs_from_txt(content_bytes)
        logger.info(f"Extracted {len(cpr_se_pairs)} CPR/SE pairs ({len({cpr for cpr, _ in cpr_se_pairs})} unique CPRs) from eIndkomst input.")

        from utils.kombit import TempClientCert
        from kombit_client.integrations.sf0770a import SKATForwardEIndkomstClient

        rows: list[dict[str, Any]] = []
        with TempClientCert() as certificate:
            client = SKATForwardEIndkomstClient(
                virksomhed_se_nummer_identifikator=virksomhed_se_nummer_identifikator,
                abonnement_type_kode=abonnement_type_kode,
                abonnent_type_kode=abonnent_type_kode,
                adgang_formaal_type_kode=adgang_formaal_type_kode,
                client_certificate_file_path=certificate,
            )
            for index, (cpr, se_number) in enumerate(cpr_se_pairs, start=1):
                logger.info(f"Processing eIndkomst input row {index}/{len(cpr_se_pairs)}")
                if not se_number:
                    rows.append({"CPR": cpr})
                    continue
                # SKAT is queried by CPR, the input SE number is used to filter the response below.
                payload = client.indkomstoplysninger_laes(
                    person_civil_registration_identifier=cpr,
                    soege_aar_maaned_fra_kode=start_month,
                    soege_aar_maaned_til_kode=end_month,
                )
                rows.extend(extract_eindkomst_rows(payload, cpr, se_number, fields))

        report_date = get_current_context()["logical_date"].in_timezone(
            get_current_context()["dag"].timezone
        ).date().isoformat()

        filename = f"eIndkomst_{start_month}_til_{end_month}_{report_date}.xlsx"

        EmailSender(smtp_server=smtp_server).send_email(
            sender=sender,
            recipients=recipients,
            subject=f"eIndkomst: Indkomstoplysninger ({start_month} - {end_month})",
            body="Indkomstoplysninger er vedhæftet som Excel.",
            attachments=[(filename, write_eindkomst_report_excel_bytes(rows, columns, column_descriptions))],
        )
        logger.info("eIndkomst report sent successfully.")
    except Exception as error:
        raise AirflowFailException("Failed to process eIndkomst report.") from error
    finally:
        # Make sure to delete the processed input email
        email_reader.delete_email_by_uid(uid=uid, mailbox="INBOX", expunge=True)
        logger.info("Deleted processed eIndkomst input email from INBOX.")
