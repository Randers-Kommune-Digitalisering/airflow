import logging
import pandas as pd
import io
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from rkdigi.email_handling import EmailSender, EmailReader
from dag_arbejdsfortjeneste.arbejdsfortjeneste_data import (
    get_report_field_to_blanket_field_id,
    get_change_report_key_columns,
    build_diff_table,
    extract_cprs,
    find_latest_attachment,
    extract_rows_from_serviceplatform_response,
    iter_months,
    write_arbejdsfortjeneste_report_excel_bytes,
)

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

    if start_month > end_month:
        raise AirflowFailException("start_month must not be after end_month.")

    return start_month, end_month


def process_arbejdsfortjeneste() -> None:
    """
     Process Arbejdsfortjeneste CPR input and deliver an income change report.

    :return: None.
    :raises AirflowFailException: If required config or CPR input is missing, or if report processing fails.
    """
    logger.info("Starting to process arbejdsfortjeneste data...")

    skat_client_config = Variable.get("skat_client_config", deserialize_json=True)
    arbejdsfortjeneste_runtime_config = Variable.get("arbejdsfortjeneste_runtime_config", deserialize_json=True)

    if not isinstance(skat_client_config, dict):
        raise AirflowFailException("SKAT client configuration must be a JSON object.")

    skat_required_keys = (
        "virksomhed_se_nummer_identifikator",
        "abonnement_type_kode",
        "abonnent_type_kode",
        "adgang_formaal_type_kode",
    )
    missing_skat_keys = _get_missing_or_empty_keys(config=skat_client_config, required_keys=skat_required_keys)
    if missing_skat_keys:
        raise AirflowFailException(
            f"SKAT client configuration is missing required keys: {', '.join(missing_skat_keys)}"
        )

    virksomhed_se_nummer_identifikator = skat_client_config.get("virksomhed_se_nummer_identifikator")
    abonnement_type_kode = skat_client_config.get("abonnement_type_kode")
    abonnent_type_kode = skat_client_config.get("abonnent_type_kode")
    adgang_formaal_type_kode = skat_client_config.get("adgang_formaal_type_kode")

    if not isinstance(arbejdsfortjeneste_runtime_config, dict):
        raise AirflowFailException("Arbejdsfortjeneste runtime configuration must be a JSON object.")

    runtime_required_keys = (
        "sender_email",
        "recipient_emails",
        "smtp_server",
        "imap_server",
    )
    missing_runtime_keys = _get_missing_or_empty_keys(
        config=arbejdsfortjeneste_runtime_config,
        required_keys=runtime_required_keys,
    )
    if missing_runtime_keys:
        raise AirflowFailException(
            f"Arbejdsfortjeneste runtime configuration is missing required keys: {', '.join(missing_runtime_keys)}"
        )

    sender = arbejdsfortjeneste_runtime_config.get("sender_email")
    recipients = arbejdsfortjeneste_runtime_config.get("recipient_emails")
    smtp_server = arbejdsfortjeneste_runtime_config.get("smtp_server")
    imap_server = arbejdsfortjeneste_runtime_config.get("imap_server")

    arbejdsfortjeneste_imap_conn = BaseHook.get_connection("arbejdsfortjeneste_imap")

    if not all((arbejdsfortjeneste_imap_conn.login, arbejdsfortjeneste_imap_conn.password, imap_server)):
        raise AirflowFailException("IMAP configuration is not set properly.")

    email_reader = EmailReader(
        email=arbejdsfortjeneste_imap_conn.login,
        password=arbejdsfortjeneste_imap_conn.password,
        imap_server=imap_server,
    )

    found = find_latest_attachment(
        email_reader=email_reader
    )

    if not found:
        raise AirflowFailException("No matching attachment found in the mailbox.")

    uid, filename, content_bytes = found
    logger.info(f"Found attachment: {filename} (UID: {uid.decode()}) with size {len(content_bytes)} bytes")

    try:
        df = pd.read_excel(
            io.BytesIO(content_bytes),
            engine="openpyxl",
            dtype={"Cprnr.": str},
            sheet_name=0,
        )

        cpr_list = extract_cprs(df=df)
        if not cpr_list:
            raise AirflowFailException("No CPR values found in the Excel file")

        start_month, end_month = _resolve_month_interval()
        months = list(iter_months(start_yyyymm=start_month, end_yyyymm=end_month))

        logger.info(f"Arbejdsfortjeneste date range: {start_month} -> {end_month} (months: {', '.join(months)})")

        field_map = get_report_field_to_blanket_field_id()
        report_fields = tuple(field_map.keys())

        base_cols = [
            "cpr",
            "VirksomhedSENummerIdentifikator",
            "IndkomstPersonGruppeDispositionDato",
            "AngivelsePeriode",
            *report_fields,
        ]

        def _placeholder_row(for_cpr: str) -> dict:
            return {
                "cpr": for_cpr,
                "VirksomhedSENummerIdentifikator": None,
                "IndkomstPersonGruppeDispositionDato": None,
                "AngivelsePeriode": None,
                **{k: None for k in report_fields},
            }

        all_diff: list[pd.DataFrame] = []
        all_indkomst_rows: list[dict] = []

        from utils.kombit import TempClientCert
        from kombit_client.integrations.sf0770a import SKATForwardEIndkomstClient  # Import lazily to avoid Airflow freezing issue

        with TempClientCert() as client_cert_path:
            skat_client = SKATForwardEIndkomstClient(
                virksomhed_se_nummer_identifikator=virksomhed_se_nummer_identifikator,
                abonnement_type_kode=abonnement_type_kode,
                abonnent_type_kode=abonnent_type_kode,
                adgang_formaal_type_kode=adgang_formaal_type_kode,
                client_certificate_file_path=client_cert_path,
            )

            total_cprs = len(cpr_list)
            for idx, cpr in enumerate(cpr_list):
                logger.info(f"Processing CPR {idx + 1}/{total_cprs}")

                try:
                    range_payload = skat_client.indkomstoplysninger_laes(
                        person_civil_registration_identifier=cpr,
                        soege_aar_maaned_fra_kode=start_month,
                        soege_aar_maaned_til_kode=end_month,
                    )
                    rows_range, _ = extract_rows_from_serviceplatform_response(payload=range_payload)
                    rows_range = rows_range or [_placeholder_row(for_cpr=cpr)]
                except Exception as e:
                    logger.error(f"Failed range fetch for CPR {idx + 1}/{total_cprs}: {e}")
                    rows_range = [_placeholder_row(for_cpr=cpr)]

                all_indkomst_rows.extend(rows_range)
                if idx != total_cprs - 1:
                    all_indkomst_rows.append({col: None for col in base_cols})

                rows_by_month: dict[str, list[dict]] = {}
                for month in months:
                    try:
                        month_payload = skat_client.indkomstoplysninger_laes(
                            person_civil_registration_identifier=cpr,
                            soege_aar_maaned_fra_kode=month,
                            soege_aar_maaned_til_kode=month,
                        )
                        month_rows, _ = extract_rows_from_serviceplatform_response(payload=month_payload)
                        rows_by_month[month] = month_rows or [_placeholder_row(for_cpr=cpr)]
                    except Exception as e:
                        logger.error(f"Failed month fetch for CPR {idx + 1}/{total_cprs} ({month}): {e}")
                        rows_by_month[month] = [_placeholder_row(for_cpr=cpr)]

                for month_idx in range(1, len(months)):
                    prev_month = months[month_idx - 1]
                    curr_month = months[month_idx]

                    df_prev = pd.DataFrame(rows_by_month[prev_month], columns=base_cols)
                    df_curr = pd.DataFrame(rows_by_month[curr_month], columns=base_cols)
                    all_diff.append(build_diff_table(df_prev=df_prev, df_curr=df_curr, prev_month=prev_month, curr_month=curr_month))

        indkomst_df = pd.DataFrame(all_indkomst_rows, columns=base_cols)

        diff_cols = [
            "FraMåned",
            "TilMåned",
            *get_change_report_key_columns(),
            "Felt",
            "Sidste måned",
            "Denne måned",
            "Indikator",
            "Ændring",
        ]
        final_diff = pd.concat(all_diff, ignore_index=True) if all_diff else pd.DataFrame(columns=diff_cols)

        excel_bytes = write_arbejdsfortjeneste_report_excel_bytes(indkomst_df=indkomst_df, diff_df=final_diff)

        ctx = get_current_context()
        logical_date = ctx["logical_date"]
        dag_tz = ctx["dag"].timezone
        report_date = logical_date.in_timezone(dag_tz).date().isoformat()

        report_filename = f"Arbejdsfortjeneste_{start_month}_til_{end_month}_{report_date}.xlsx"

        email_sender = EmailSender(smtp_server=smtp_server)
        email_sender.send_email(
            sender=sender,
            recipients=recipients,
            subject=f"Arbejdsfortjeneste report ({start_month} -> {end_month})",
            body="Indkomstoplysninger (interval) og ændringer (måned-til-måned) er vedhæftet som Excel.",
            attachments=[(report_filename, excel_bytes)],
        )

        logger.info("Arbejdsfortjeneste processing completed successfully (email sent).")

        email_reader.delete_email_by_uid(uid=uid, mailbox="INBOX", expunge=True)
        logger.info(f"Deleted Arbejdsfortjeneste input email {filename} with UID {uid!r} from INBOX")

    except Exception as e:
        raise AirflowFailException("Failed to process arbejdsfortjeneste report") from e
