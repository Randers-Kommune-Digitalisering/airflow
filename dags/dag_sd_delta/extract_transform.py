import logging
import tempfile
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path

from zoneinfo import ZoneInfo

from dag_sd_delta.sd import (
    get_departments_df,
    get_employment_on_date_df,
    get_employments_with_changes_df,
    get_institutions_df,
    get_person_on_date_df,
    get_persons_df,
    get_professions_xml
)

logger = logging.getLogger(__name__)

# Import and transform changes section
STATUS_META = {
    "0": {"label": "Ansat ikke i løn", "group": "ACTIVE"},
    "1": {"label": "Aktiv", "group": "ACTIVE"},
    "3": {"label": "Midlertidig ude af løn", "group": "ACTIVE"},
    "4": {"label": "Ansat i konflikt", "group": "ACTIVE"},
    "7": {"label": "Emigreret eller død", "group": "INACTIVE"},
    "8": {"label": "Fratrådt", "group": "INACTIVE"},
    "9": {"label": "Pensioneret", "group": "INACTIVE"},
    "S": {"label": "Slettet", "group": None},
    None: {"label": None, "group": None},
}

OUT_COLUMNS = [
    "Institutions-niveau",
    "Stamafdeling",
    "CPR-nummer",
    "Navn (for-/efternavn)",
    "Stillingskode nuværende",
    "Stillingskode niveau 2",
    "Startdato",
    "Slutdato",
    "Ansættelsesstatus",
    "Tjenestenummer",
    "Afdeling",
    "Handling",
]


def _get_profession_with_level_2(professions_xml: ET.Element, position_id: str) -> tuple[str | None, str | None, str | None, str | None]:
    """Resolve profession and level-2 profession metadata for a job position id.

    Falls back to level 3 when level 2 does not exist in the hierarchy.
    """

    def _node_text(node: ET.Element | None) -> str | None:
        return node.text.strip() if node is not None and node.text else None

    matched_profession_node = professions_xml.find(
        f".//{{*}}Profession[{{*}}JobPositionIdentifier='{position_id}']"
    )
    if matched_profession_node is None:
        raise ValueError(f"No profession found with JobPositionIdentifier={position_id}")

    parent_map: dict[ET.Element, ET.Element] = {
        child: parent for parent in professions_xml.iter() for child in parent
    }
    level_2_profession_node: ET.Element | None = None
    level_3_profession_node: ET.Element | None = None
    current_node: ET.Element | None = matched_profession_node

    while current_node is not None:
        level_code = _node_text(current_node.find("./{*}JobPositionLevelCode"))
        if level_code == "2":
            level_2_profession_node = current_node
            break
        if level_code == "3" and level_3_profession_node is None:
            level_3_profession_node = current_node
        current_node = parent_map.get(current_node)

    if level_2_profession_node is None:
        if level_3_profession_node is not None:
            logger.warning(
                "No level 2 profession found for position_id=%s. Falling back to level 3.",
                position_id,
            )
            level_2_profession_node = level_3_profession_node
        else:
            raise ValueError(
                f"No level 2 or level 3 profession found for position_id={position_id}"
            )

    matched_id = _node_text(matched_profession_node.find("./{*}JobPositionIdentifier"))
    matched_name = _node_text(matched_profession_node.find("./{*}JobPositionName"))
    level_2_id = _node_text(level_2_profession_node.find("./{*}JobPositionIdentifier")) if level_2_profession_node is not None else None
    level_2_name = _node_text(level_2_profession_node.find("./{*}JobPositionName")) if level_2_profession_node is not None else None

    return matched_name, matched_id, level_2_name, level_2_id


def _parse_iso_date_or_none(value: object) -> date | None:
    """Parse supported date-like values to python date, returning None for missing values."""
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    raise ValueError(f"Unsupported date value type: {type(value)}")


def _max_non_null_date(row: pd.Series, columns: list[str]) -> date | None:
    """Return max non-null date among the provided columns for a row."""
    values = [row[column_name] for column_name in columns if pd.notna(row[column_name])]
    return max(values) if values else None


def _min_non_null_date(row: pd.Series, columns: list[str]) -> date | None:
    """Return min non-null date among the provided columns for a row."""
    values = [row[column_name] for column_name in columns if pd.notna(row[column_name])]
    return min(values) if values else None


def _format_name_with_code(name_value: object, code_value: object) -> str:
    """Format a human-readable label with code; fail on missing required values."""
    if pd.notna(name_value) and pd.notna(code_value):
        return f"{name_value} ({code_value})"
    raise ValueError(
        "Missing required value(s) for name and code: "
        f"name_value={name_value}, code_value={code_value}"
    )


def _format_date_series(date_series: pd.Series) -> pd.Series:
    """Format date-like values to dd.mm.yyyy for final output."""

    def _format_date_value(value: object) -> str:
        date_text = str(value).strip()
        if date_text == "9999-12-31":
            return "31.12.9999"
        parsed_date = datetime.strptime(date_text, "%Y-%m-%d")
        return parsed_date.strftime("%d.%m.%Y")

    return date_series.map(_format_date_value)


def _collect_source_date_columns(employment_changes_df: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Return available activation and deactivation source date columns."""
    activation_source_columns = [
        column_name
        for column_name in [
            "Department_ActivationDate",
            "Profession_ActivationDate",
            "EmploymentStatus_ActivationDate",
        ]
        if column_name in employment_changes_df.columns
    ]
    deactivation_source_columns = [
        column_name
        for column_name in [
            "Department_DeactivationDate",
            "Profession_DeactivationDate",
            "EmploymentStatus_DeactivationDate",
        ]
        if column_name in employment_changes_df.columns
    ]
    return activation_source_columns, deactivation_source_columns


def _normalize_and_filter_status_periods(employment_changes_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize status groups and keep rows relevant for period shaping."""
    employment_changes_df = employment_changes_df.copy()

    employment_changes_df["EmploymentStatusGroup"] = employment_changes_df[
        "EmploymentStatus_EmploymentStatusCode"
    ].map(lambda status_code: STATUS_META.get(status_code, {}).get("group") or "UNKNOWN")

    employment_group_columns = [
        column_name
        for column_name in ["PersonCivilRegistrationIdentifier", "EmploymentIdentifier"]
        if column_name in employment_changes_df.columns
    ]
    has_active_for_employment = employment_changes_df.groupby(
        employment_group_columns,
        dropna=False,
    )["EmploymentStatusGroup"].transform(lambda statuses: (statuses == "ACTIVE").any())

    return employment_changes_df[
        (employment_changes_df["EmploymentStatusGroup"] == "ACTIVE")
        | (
            (employment_changes_df["EmploymentStatusGroup"] == "INACTIVE")
            & (~has_active_for_employment)
        )
        | (employment_changes_df["EmploymentStatusGroup"] == "UNKNOWN")
    ].copy()


def _compute_period_columns(employment_changes_df: pd.DataFrame) -> pd.DataFrame:
    """Build ActivationDate/DeactivationDate columns from source components and filter invalid periods."""
    employment_changes_df = employment_changes_df.copy()

    activation_source_columns, deactivation_source_columns = _collect_source_date_columns(
        employment_changes_df
    )

    for column_name in [*activation_source_columns, *deactivation_source_columns]:
        employment_changes_df[column_name] = employment_changes_df[column_name].map(
            _parse_iso_date_or_none
        )

    employment_changes_df["ActivationDate"] = employment_changes_df.apply(
        lambda row: _max_non_null_date(row=row, columns=activation_source_columns),
        axis=1,
    )
    employment_changes_df["DeactivationDate"] = employment_changes_df.apply(
        lambda row: _min_non_null_date(row=row, columns=deactivation_source_columns),
        axis=1,
    )

    return employment_changes_df[
        employment_changes_df["ActivationDate"].notna()
        & employment_changes_df["DeactivationDate"].notna()
        & (employment_changes_df["ActivationDate"] <= employment_changes_df["DeactivationDate"])
    ].copy()


def _merge_contiguous_periods(employment_changes_df: pd.DataFrame) -> pd.DataFrame:
    """Merge adjacent periods that belong to the same employment identity and metadata."""
    employment_changes_df = employment_changes_df.copy()

    employment_changes_df = employment_changes_df.drop(
        columns=[
            "Profession_AppointmentCode",
            *[
                column_name
                for column_name in employment_changes_df.columns
                if column_name.endswith("_ActivationDate")
                or column_name.endswith("_DeactivationDate")
            ],
        ]
    )

    merge_key_columns = [
        column_name
        for column_name in [
            "PersonCivilRegistrationIdentifier",
            "EmploymentIdentifier",
            "EmploymentDate",
            "Department_DepartmentIdentifier",
            "Profession_JobPositionIdentifier",
            "Profession_EmploymentName",
        ]
        if column_name in employment_changes_df.columns
    ]

    sort_columns = [*merge_key_columns, "ActivationDate", "DeactivationDate"]
    employment_changes_df = employment_changes_df.sort_values(sort_columns).reset_index(drop=True)

    previous_end = employment_changes_df.groupby(merge_key_columns, dropna=False)[
        "DeactivationDate"
    ].shift(1)
    previous_end_plus_one_day = previous_end.apply(
        lambda value: value + timedelta(days=1) if pd.notna(value) else None
    )
    starts_new_segment = previous_end.isna() | (
        employment_changes_df["ActivationDate"] > previous_end_plus_one_day
    )

    employment_changes_df["_segment_id"] = starts_new_segment.groupby(
        [employment_changes_df[column_name] for column_name in merge_key_columns],
        dropna=False,
    ).cumsum()

    segment_group_columns = [*merge_key_columns, "_segment_id"]
    first_value_columns = [
        column_name
        for column_name in employment_changes_df.columns
        if column_name not in {*segment_group_columns, "ActivationDate", "DeactivationDate"}
    ]
    aggregation_map = {
        "ActivationDate": ("ActivationDate", "min"),
        "DeactivationDate": ("DeactivationDate", "max"),
        **{column_name: (column_name, "first") for column_name in first_value_columns},
    }

    employment_changes_df = employment_changes_df.groupby(
        segment_group_columns,
        dropna=False,
        as_index=False,
    ).agg(**aggregation_map)
    employment_changes_df = employment_changes_df.drop(columns=["_segment_id"])

    dedupe_columns = [
        column_name
        for column_name in [
            "EmploymentIdentifier",
            "Department_DepartmentIdentifier",
            "EmploymentStatus_EmploymentStatusCode",
            "ActivationDate",
            "DeactivationDate",
        ]
        if column_name in employment_changes_df.columns
    ]

    return employment_changes_df.drop_duplicates(
        subset=dedupe_columns,
        keep="first",
    ).reset_index(drop=True)


def _prepare_for_enrichment(employment_changes_df: pd.DataFrame, persons_df: pd.DataFrame) -> pd.DataFrame:
    """Rename flattened columns and enrich with person name columns from persons snapshot."""
    employment_changes_df = employment_changes_df.rename(
        columns=lambda column_name: column_name.split("_", 1)[1]
        if "_" in column_name
        else column_name
    )

    return employment_changes_df.merge(
        persons_df[
            [
                "PersonCivilRegistrationIdentifier",
                "EmploymentIdentifier",
                "PersonGivenName",
                "PersonSurnameName",
            ]
        ].drop_duplicates(
            subset=["PersonCivilRegistrationIdentifier", "EmploymentIdentifier"],
            keep="first",
        ),
        on=["PersonCivilRegistrationIdentifier", "EmploymentIdentifier"],
        how="left",
    )


def _enrich_row_with_snapshot(
    employment_changes_df: pd.DataFrame,
    row_index: int,
    inst_id: str,
) -> pd.DataFrame:
    """Enrich one row with point-in-time employment/person snapshots for missing fields."""
    row = employment_changes_df.loc[row_index]
    cpr = row.get("PersonCivilRegistrationIdentifier")
    employment_id = row.get("EmploymentIdentifier")
    activation_date = row.get("ActivationDate")

    if pd.isna(cpr) or pd.isna(employment_id) or pd.isna(activation_date):
        raise ValueError(
            f"Missing required value(s) for row at index {row_index}: "
            f"EmploymentIdentifier={employment_id}, ActivationDate={activation_date}"
        )

    effective_date = activation_date

    employment_snapshot_df = get_employment_on_date_df(
        inst_id=inst_id,
        cpr=str(cpr),
        employment_id=str(employment_id),
        effective_date=effective_date,
    )

    if employment_snapshot_df.empty:
        return employment_changes_df.drop(index=row_index)

    snapshot_row = employment_snapshot_df.iloc[0]

    person_given_name = row.get("PersonGivenName")
    person_surname_name = row.get("PersonSurnameName")

    if pd.isna(person_given_name) or pd.isna(person_surname_name):
        person_snapshot_df = get_person_on_date_df(
            inst_id=inst_id,
            cpr=str(cpr),
            employment_id=str(employment_id),
            effective_date=effective_date,
        )

        if person_snapshot_df.empty:
            return employment_changes_df.drop(index=row_index)

        person_snapshot_row = person_snapshot_df.iloc[0]
        person_given_name = person_snapshot_row.get("PersonGivenName")
        person_surname_name = person_snapshot_row.get("PersonSurnameName")

    mapped_values = {
        "PersonCivilRegistrationIdentifier": snapshot_row.get("PersonCivilRegistrationIdentifier"),
        "EmploymentIdentifier": snapshot_row.get("EmploymentIdentifier"),
        "EmploymentDate": snapshot_row.get("EmploymentDate"),
        "ActivationDate": snapshot_row.get("EmploymentStatus_ActivationDate"),
        "DeactivationDate": snapshot_row.get("EmploymentStatus_DeactivationDate"),
        "EmploymentStatusCode": snapshot_row.get("EmploymentStatus_EmploymentStatusCode"),
        "DepartmentIdentifier": snapshot_row.get("Department_DepartmentIdentifier"),
        "JobPositionIdentifier": snapshot_row.get("Profession_JobPositionIdentifier"),
        "EmploymentName": snapshot_row.get("Profession_EmploymentName"),
        "PersonGivenName": person_given_name,
        "PersonSurnameName": person_surname_name,
    }

    for column_name, column_value in mapped_values.items():
        if (
            column_name in employment_changes_df.columns
            and pd.notna(column_value)
            and pd.isna(employment_changes_df.at[row_index, column_name])
        ):
            employment_changes_df.at[row_index, column_name] = column_value

    return employment_changes_df


def _enrich_with_snapshots(
    employment_changes_df: pd.DataFrame,
    inst_id: str,
) -> pd.DataFrame:
    """Enrich rows with missing values using point-in-time snapshots."""
    rows_with_missing_values = set(
        employment_changes_df.index[employment_changes_df.isna().any(axis=1)].tolist()
    )

    rows_to_enrich = sorted(rows_with_missing_values)
    for row_index in rows_to_enrich:
        employment_changes_df = _enrich_row_with_snapshot(
            employment_changes_df=employment_changes_df,
            row_index=row_index,
            inst_id=inst_id,
        )

    return employment_changes_df


def build_output_df(
    employment_changes_df: pd.DataFrame,
    inst_id: str,
    inst_name_mapping_df: pd.DataFrame,
    start_time: datetime,
    prof_name_mapping_xml: ET.Element,
) -> pd.DataFrame:
    """Build final output dataframe for one institution."""
    status_code_series = employment_changes_df["EmploymentStatusCode"]

    institution_name_match = inst_name_mapping_df.loc[
        inst_name_mapping_df["InstitutionIdentifier"] == inst_id,
        "InstitutionName",
    ].dropna()
    institution_name = institution_name_match.iloc[0] if not institution_name_match.empty else inst_id

    dept_name_mapping_df = get_departments_df(
        inst_id=inst_id,
        activation_date=start_time,
        deactivation_date=datetime(9999, 12, 31).date(),
    )

    dept_name_lookup = dept_name_mapping_df.drop_duplicates(
        subset=["DepartmentIdentifier"],
        keep="first",
    ).set_index("DepartmentIdentifier")["DepartmentName"]

    dept_ids = employment_changes_df["DepartmentIdentifier"]
    dept_names = dept_ids.map(dept_name_lookup)
    dept_codes = dept_ids.apply(lambda value: f"{inst_id}_{value}" if pd.notna(value) else None)

    job_position_ids = employment_changes_df["JobPositionIdentifier"]
    profession_name_cache: dict[str, tuple[str | None, str | None, str | None, str | None]] = {}

    def _resolve_profession_names(position_id_value: object) -> tuple[str | None, str | None, str | None, str | None]:
        if pd.isna(position_id_value):
            raise ValueError(
                "Missing required value(s) for profession: "
                f"position_id_value={position_id_value}"
            )

        position_id = str(position_id_value).strip()
        if position_id not in profession_name_cache:
            profession_name_cache[position_id] = _get_profession_with_level_2(
                professions_xml=prof_name_mapping_xml,
                position_id=position_id,
            )
        return profession_name_cache[position_id]

    profession_name_pairs = job_position_ids.apply(_resolve_profession_names)
    profession_names = profession_name_pairs.str[0]
    profession_ids = profession_name_pairs.str[1]
    level_2_profession_names = profession_name_pairs.str[2]
    level_2_profession_ids = profession_name_pairs.str[3]
    profession_codes = profession_ids.apply(lambda value: f"RG_{value}" if pd.notna(value) else None)
    level_2_profession_codes = level_2_profession_ids.apply(
        lambda value: f"{inst_id}_{value}" if pd.notna(value) else None
    )

    out_df = pd.DataFrame(index=employment_changes_df.index)
    out_df["Institutions-niveau"] = _format_name_with_code(name_value=institution_name, code_value=inst_id)
    out_df["Stamafdeling"] = [
        _format_name_with_code(name_value=name_value, code_value=code_value)
        for name_value, code_value in zip(dept_names, dept_codes)
    ]
    out_df["CPR-nummer"] = employment_changes_df["PersonCivilRegistrationIdentifier"]

    out_df["Navn (for-/efternavn)"] = (
        employment_changes_df["PersonGivenName"].fillna("").str.strip()
        + " "
        + employment_changes_df["PersonSurnameName"].fillna("").str.strip()
    ).str.strip().replace("", pd.NA)
    if out_df["Navn (for-/efternavn)"].isna().any():
        raise ValueError("Missing person name")

    out_df["Stillingskode nuværende"] = [
        _format_name_with_code(name_value=name_value, code_value=code_value)
        for name_value, code_value in zip(profession_names, profession_codes)
    ]
    out_df["Stillingskode niveau 2"] = [
        _format_name_with_code(name_value=name_value, code_value=code_value)
        for name_value, code_value in zip(level_2_profession_names, level_2_profession_codes)
    ]
    out_df["Startdato"] = _format_date_series(employment_changes_df["ActivationDate"])
    out_df["Slutdato"] = _format_date_series(employment_changes_df["DeactivationDate"])
    out_df["Ansættelsesstatus"] = status_code_series.map(
        lambda status_code: STATUS_META.get(status_code, {}).get("label", status_code)
    )
    out_df["Tjenestenummer"] = employment_changes_df["EmploymentIdentifier"]
    out_df["Afdeling"] = employment_changes_df["DepartmentIdentifier"]
    out_df["Handling"] = None

    out_df = out_df[OUT_COLUMNS]

    required_output_df = out_df.drop(columns=["Handling"], errors="ignore")
    if required_output_df.isna().any().any():
        raise ValueError("Output contains missing required value(s).")

    return out_df


def _prepare_institution_changes(
    inst_id: str,
    excluded_dept_ids: list[str],
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch and normalize changed employment rows for one institution.
    """
    # Fetch all changes in the period for institution from SD
    employment_changes_df = get_employments_with_changes_df(
        inst_id=inst_id,
        activation_datetime=start_time,
        deactivation_datetime=end_time,
    )
    if employment_changes_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Remove excluded departments while keeping rows without department id.
    employment_changes_df = employment_changes_df[
        ~employment_changes_df["Department_DepartmentIdentifier"].isin(excluded_dept_ids)
        | employment_changes_df["Department_DepartmentIdentifier"].isna()
    ]

    # Keep deleted employments - must be handled with Delta api, not with excel file.
    employment_changes_deleted_df = employment_changes_df[
        employment_changes_df["EmploymentStatus_EmploymentStatusCode"] == "S"
    ]

    # Keep non-deleted employments for excel file.
    employment_changes_df = employment_changes_df[
        employment_changes_df["EmploymentStatus_EmploymentStatusCode"] != "S"
    ]

    return employment_changes_df, employment_changes_deleted_df


def _process_one_institution(
    inst_name_mapping_df: pd.DataFrame,
    prof_name_mapping_xml: ET.Element,
    inst: dict,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, list[dict]]:
    """Run the full transformation pipeline for one institution and return output rows."""
    inst_id = inst["inst_id"]
    excluded_dept_ids = inst["excluded_dept_ids"]

    logger.info(f"Processing institution {inst_id} with excluded departments:\n {excluded_dept_ids}")

    employment_changes_df, employment_changes_deleted_df = _prepare_institution_changes(
        inst_id=inst_id,
        excluded_dept_ids=excluded_dept_ids,
        start_time=start_time,
        end_time=end_time,
    )

    if employment_changes_df.empty and employment_changes_deleted_df.empty:
        logger.info(f"No employment changes found for institution {inst_id}")
        return pd.DataFrame(), []

    deleted_list = []
    if not employment_changes_deleted_df.empty:
        logger.info(f"{len(employment_changes_deleted_df)} deleted employment(s) found for institution {inst_id}")
        for row in employment_changes_deleted_df.itertuples():
            deleted_list.append({
                "institution_id": inst_id,
                "employment_id": str(row.EmploymentIdentifier),
                "cpr": str(row.PersonCivilRegistrationIdentifier),
                "date": row.EmploymentStatus_ActivationDate.isoformat(),
            })

    out_df = pd.DataFrame()
    if not employment_changes_df.empty:
        # Fetch person snapshot used both for names and join enrichment.
        persons_df = get_persons_df(
            inst_id=inst_id,
            effective_date=end_time,
        )

        # Normalize statuses and build consolidated date periods.
        employment_changes_df = _normalize_and_filter_status_periods(employment_changes_df=employment_changes_df)
        employment_changes_df = _compute_period_columns(employment_changes_df=employment_changes_df)
        employment_changes_df = _merge_contiguous_periods(employment_changes_df=employment_changes_df)

        # Rename flattened columns and enrich rows with snapshot-based fallbacks.
        employment_changes_df = _prepare_for_enrichment(employment_changes_df=employment_changes_df, persons_df=persons_df)
        employment_changes_df = _enrich_with_snapshots(
            employment_changes_df=employment_changes_df,
            inst_id=inst_id,
        )

        # Build final output dataframe for this institution.
        out_df = build_output_df(
            employment_changes_df=employment_changes_df,
            inst_id=inst_id,
            inst_name_mapping_df=inst_name_mapping_df,
            start_time=start_time,
            prof_name_mapping_xml=prof_name_mapping_xml,
        )

        logger.info(f"{len(out_df)} employment changes found for institution {inst_id}")
    return out_df, deleted_list


def get_and_transform_changes(
    insts_to_import: list[dict],
    start_time: datetime,
    end_time: datetime,
) -> dict[str, str | bool]:
    """
    Main DAG business flow.

    Fetch institutions, process each institution and write a combined excel file when there are changes.
    """
    # Read institution and profession mapping once and reuse for all institutions.
    inst_name_mapping_df = get_institutions_df()
    prof_name_mapping_xml = get_professions_xml(inst_id='RG')
    out_dfs: list[pd.DataFrame] = []
    deleted_list: list[dict] = []
    # Process institutions one by one and collect output chunks.
    for inst in insts_to_import:
        changes, deleted = _process_one_institution(
            inst_name_mapping_df=inst_name_mapping_df,
            prof_name_mapping_xml=prof_name_mapping_xml,
            inst=inst,
            start_time=start_time,
            end_time=end_time,
        )
        out_dfs.append(changes)
        deleted_list.extend(deleted)

    # Generate an output file if any changes were found.
    output_file = None
    if not out_dfs and not deleted_list:
        logger.warning("No employment changes found for any institution. Not generating an output file.")
    elif out_dfs:
        combined_out_df = pd.concat(out_dfs, ignore_index=True)

        # Write result as excel file
        output_dir = Path(tempfile.gettempdir()) / "sd_delta_sync"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "sd-delta-sync.xlsx"
        with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
            combined_out_df.to_excel(
                writer,
                sheet_name="Ark1",
                index=False,
                header=True,
            )
            logger.info(f"Saved combined output with {len(combined_out_df)} rows to {output_file}")
    elif not deleted_list:
        deleted_list = None

    return {
        "start_time": start_time.astimezone(ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%dT%H:%M:%S"),
        "end_time": end_time.astimezone(ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%dT%H:%M:%S"),
        "report_path": str(output_file),
        "deleted_employments": deleted_list
    }
