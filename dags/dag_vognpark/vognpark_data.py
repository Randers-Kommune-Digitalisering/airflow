import io
import pandas as pd
import logging
from openpyxl.utils import get_column_letter
from typing import Any, Iterable
from airflow.providers.http.hooks.http import HttpHook
from rkdigi.email_handling import EmailReader

logger = logging.getLogger(__name__)

INSUBIZ_EXCEL_FIELDS = [
    "registration",
    "registrationDate",
    "make",
    "model",
    "modelYear",
    "chassisNumber",
    "cart",
    "endDate",
    "customer_name",
    "usageCategory_text",
    "usage_text",
    "Level1",
    "Level2",
    "Level3",
    "Level4",
    "Level5",
    "Level6",
]

INSUBIZ_COLUMN_RENAME_MAP = {
    "registration": "Reg.nr.",
    "registrationDate": "Reg.dato",
    "make": "Mærke",
    "model": "Model",
    "modelYear": "Årgang",
    "chassisNumber": "Stelnr.",
    "cart": "Træk",
    "endDate": "Afg.dato",
    "customer_name": "Kundenavn",
    "usageCategory_text": "Anvendelse",
    "usage_text": "Art",
}

INSUBIZ_ROOT_CUSTOMER_ID = 216146  # RK
INSUBIZ_MAX_LEVELS = 6


def _format_datetime_column(df: pd.DataFrame, col: str) -> None:
    """
    If the column does not exist, the function returns without changes.

    :param df: DataFrame containing the column to format.
    :param col: Name of the column to parse and format as datetime.
    :return: None. The DataFrame is modified in place.
    """
    if col not in df.columns:
        return
    dt = pd.to_datetime(df[col], errors="coerce")
    df[col] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")


def normalize_insubiz_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames source columns to the internal export naming and formats known
    datetime columns to a consistent timestamp string format.

    :param df: Raw Insubiz DataFrame.
    :return: Normalized DataFrame with renamed and formatted columns.
    """
    out = df.rename(columns=INSUBIZ_COLUMN_RENAME_MAP)

    for col in ("Reg.dato", "Afg.dato", "start_date"):
        _format_datetime_column(out, col)

    return out


def _get_insubiz_credentials(http_hook: HttpHook) -> tuple[str, str]:
    """
    Read Insubiz API credentials from the Airflow HTTP connection extras.

    Expects the Airflow connection (http_conn_id = http_hook.http_conn_id) to contain
    extras keys: ``api_key`` and ``secret_key``.

    :param http_hook: Airflow HttpHook configured for the Insubiz Cloud API connection.
    :return: (api_key, secret_key)
    """
    conn = http_hook.get_connection(http_hook.http_conn_id)

    extras = conn.extra_dejson

    api_key = extras.get("api_key")
    secret_key = extras.get("secret_key")

    if not api_key or not secret_key:
        raise ValueError(
            "Missing api_key or secret_key in Airflow connection extras."
        )

    return api_key, secret_key


def _insubiz_sign_in(http_hook: HttpHook) -> str:
    """
    Authenticate against the Insubiz Cloud API and return a bearer token (JWT).

    :param http_hook: Airflow HttpHook configured for the Insubiz Cloud API connection.
    :return: JWT token string to be used as ``Authorization: Bearer <token>``.
    """
    api_key, secret_key = _get_insubiz_credentials(http_hook=http_hook)

    session = http_hook.get_conn()

    session.auth = None

    payload = {
        "apiKey": api_key,
        "secretKey": secret_key,
    }

    response = session.post(
        url=f"{http_hook.base_url}/api/v1.3/Authentication/SignInAsync",
        json=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    response.raise_for_status()

    body: Any = response.json()

    if not isinstance(body, dict):
        raise ValueError(f"Unexpected SignIn response type: {type(body)}")

    if not body.get("isAuthenticated"):
        msg = body.get("message") or "Authentication failed"
        raise ValueError(f"Insubiz SignIn failed: {msg}")

    token = body.get("token")

    if not token or not isinstance(token, str):
        raise ValueError("Insubiz SignIn response missing 'token'")

    return token


def fetch_insubiz_vehicles(
    http_hook: HttpHook,
    page_size: int = 500,
) -> list[dict[str, Any]]:
    """
    Fetch all vehicles from the Insubiz Cloud API (paged)

    :param http_hook: Airflow HttpHook configured for the Insubiz Cloud API connection.
    :param page_size: Page size used for pagination requests.
    :return: List of vehicle data (concatenated across pages).
    """
    logger.info("Fetching vehicles from Insubiz Cloud API ...")

    jwt = _insubiz_sign_in(http_hook=http_hook)

    page_no = 1
    all_rows: list[dict] = []

    headers = {
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    while True:
        payload = {
            "pageNo": page_no,
            "pageSize": page_size,
        }

        response = http_hook.run(
            endpoint="/api/v1.3/Vehicle/GetVehiclesPagedAsync",
            json=payload,
            headers=headers,
        )

        response.raise_for_status()

        body: Any = response.json()

        if isinstance(body, dict):
            batch = body.get("data", []) or []
        elif isinstance(body, list):
            batch = body
        else:
            raise ValueError(f"Unexpected response type from vehicles endpoint: {type(body)}")

        if not isinstance(batch, list):
            raise ValueError("Expected a list of vehicles in response under key 'data'.")

        all_rows.extend(batch)

        logger.info(f"Fetched pageNo={page_no} rowws={len(batch)} total={len(all_rows)}")

        if len(batch) < page_size:
            break

        page_no += 1

    logger.info(f"Successfully retrieved Insubiz vehicles. Total records: {len(all_rows)}")

    return all_rows


def fetch_insubiz_customers(
    http_hook: HttpHook,
    page_size: int = 400,
) -> list[dict[str, Any]]:
    """
    Fetch all customers from the Insubiz Cloud API (paged)

    This is used to build a lookup (customer_id -> customer dict) so that vehicles can
    be enriched with Level1..Level6 based on the customer parent hierarchy.

    :param http_hook: Airflow HttpHook configured for the Insubiz Cloud API connection.
    :param page_size: Page size used for pagination requests.
    :return: List of customer data
    """
    logger.info("Fetching customers from Insubiz Cloud API ...")

    jwt = _insubiz_sign_in(http_hook=http_hook)

    page_no = 1
    all_rows: list[dict] = []

    headers = {
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    while True:
        payload = {"pageNo": page_no, "pageSize": page_size}

        response = http_hook.run(
            endpoint="/api/v1.3/Customer/GetCustomersPagedAsync",
            json=payload,
            headers=headers,
        )

        response.raise_for_status()
        body: Any = response.json()

        if isinstance(body, dict):
            batch = body.get("data", []) or []
        elif isinstance(body, list):
            batch = body
        else:
            raise ValueError(f"Unexpected response type from customers endpoint: {type(body)}")

        if not isinstance(batch, list):
            raise ValueError("Expected a list of customers in response under key 'data'.")

        all_rows.extend(batch)

        logger.info(f"Fetched customers pageNo={page_no} rows={len(batch)} total={len(all_rows)}")

        if len(batch) < page_size:
            break

        page_no += 1

    logger.info(f"Successfully retrieved Insubiz customers. Total records: {len(all_rows)}")
    return all_rows


def dfs_to_excel_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    """
    Write one or more DataFrames to an Excel workbook and return the bytes.

    :param sheets: Mapping of sheet_name -> DataFrame.
    :return: Excel file content as bytes (.xlsx).
    """
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            sheet_name = name[:31]  # Excel sheet name limit
            frame.to_excel(writer, index=False, sheet_name=sheet_name)

            ws = writer.sheets[sheet_name]

            # Dynamisk column width
            for idx, col in enumerate(frame.columns, start=1):
                max_length = max(
                    frame[col].astype(str).map(len).max(),
                    len(str(col))
                )

                ws.column_dimensions[get_column_letter(idx)].width = min(
                    max_length + 2,
                    50,  # max width
                )

    return output.getvalue()


def read_motorstyrelsen_excel_bytes(excel_bytes: bytes) -> pd.DataFrame:
    """
    Read a Motorstyrelsen Excel file from bytes and return it as a DataFrame.

    :param excel_bytes: Raw .xlsx file content.
    :return: Parsed Motorstyrelsen DataFrame.
    """
    return (
        pd.read_excel(io.BytesIO(excel_bytes), dtype={"Nummer (DPRN)": str})
        .rename(columns={"Nummer (DPRN)": "registreringsnummer"})
    )


def _norm_reg(series: pd.Series) -> pd.Series:
    """
    Normalize registration number values for stable comparisons.

    :param series: Input series containing registration numbers.
    :return: Normalized series.
    """
    return series.astype(str).str.strip().str.upper()


def compare_motorstyrelsen_vs_insubiz(
    motor_df: pd.DataFrame,
    insubiz_df: pd.DataFrame,
    motor_reg_col: str = "registreringsnummer",
    insubiz_reg_col: str = "Reg.nr.",
    exclude_prefix: str = "UINDREG",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compare Motorstyrelsen vs Insubiz vehicles by registration number.

    Output:
    - need_to_delete: vehicles present in Insubiz but not in Motorstyrelsen
    - need_to_add: vehicles present in Motorstyrelsen but not in Insubiz

    :param motor_df: Motorstyrelsen DataFrame.
    :param insubiz_df: Insubiz DataFrame.
    :param motor_reg_col: Column name in motor_df containing registration numbers.
    :param insubiz_reg_col: Column name in insubiz_df containing registration numbers.
    :param exclude_prefix: Exclude registration numbers starting with this prefix.
    :return: (need_to_delete_df, need_to_add_df)
    """
    motor_key = _norm_reg(series=motor_df[motor_reg_col])
    insubiz_key = _norm_reg(series=insubiz_df[insubiz_reg_col])

    motor_mask = ~motor_key.str.startswith(pat=exclude_prefix, na=False)
    insubiz_mask = ~insubiz_key.str.startswith(pat=exclude_prefix, na=False)

    motor_df2 = motor_df.loc[motor_mask].copy()
    insubiz_df2 = insubiz_df.loc[insubiz_mask].copy()

    motor_key2 = _norm_reg(series=motor_df2[motor_reg_col])
    insubiz_key2 = _norm_reg(series=insubiz_df2[insubiz_reg_col])

    need_to_add = motor_df2.loc[~motor_key2.isin(set(insubiz_key2))].copy()
    logger.info(f"Found {len(need_to_add)} vehicles that needs to be added (in Insubiz)")

    need_to_delete = insubiz_df2.loc[~insubiz_key2.isin(set(motor_key2))].copy()
    logger.info(f"Found {len(need_to_delete)} vehicles that needs to be deleted (in Insubiz)")

    return need_to_delete, need_to_add


def _to_valid_id_or_none(value: Any) -> int | None:
    """
    Convert a value to int if possible, otherwise return None.

    :param value: Any value representing an id (int/str/None).
    :return: Integer value, or None if conversion fails or value is empty/zero.
    """
    try:
        if value is None:
            return None
        v = int(value)
        return v if v != 0 else None
    except (TypeError, ValueError):
        return None


def build_customer_levels(
    customer_id: Any,
    customer_by_id: dict[int, dict],
    root_id: int = INSUBIZ_ROOT_CUSTOMER_ID,
    max_levels: int = INSUBIZ_MAX_LEVELS,
) -> dict[str, str]:
    """
    Build Level1..LevelN (names) for a customer by walking the parent hierarchy.

    :param customer_id: Customer id from vehicle payload (e.g. customer.id).
    :param customer_by_id: Lookup mapping customer id -> customer dict from
                           ``Customer/GetCustomersPagedAsync``.
    :param root_id: The id that must exist in the chain for levels to be populated.
    :param max_levels: Maximum number of Level columns to populate (default 6).
    :return: Dict with keys "Level1".."LevelN" (strings). Missing/unknown => "".
    """
    levels = {f"Level{i}": "" for i in range(1, max_levels + 1)}

    current_id = _to_valid_id_or_none(value=customer_id)
    if not current_id:
        return levels

    seen: set[int] = set()
    chain_leaf_to_root: list[dict] = []

    while current_id and current_id not in seen:
        seen.add(current_id)

        customer = customer_by_id.get(current_id)
        if not isinstance(customer, dict):
            break

        chain_leaf_to_root.append(customer)

        parent = customer.get("parent") or {}
        parent_id = _to_valid_id_or_none(value=parent.get("id"))
        if not parent_id:
            break

        current_id = parent_id

    chain_root_to_leaf = list(reversed(chain_leaf_to_root))

    root_index = next(
        (i for i, c in enumerate(chain_root_to_leaf) if _to_valid_id_or_none(value=c.get("id")) == root_id),
        None,
    )
    if root_index is None:
        return levels  # root ikke i kæden => tomme levels

    path = chain_root_to_leaf[root_index:]  # root -> leaf
    for i, node in enumerate(path[:max_levels], start=1):
        levels[f"Level{i}"] = (node.get("name") or "").strip()

    return levels


def enrich_vehicles_with_customer_levels(
    vehicles_df: pd.DataFrame,
    customers: list[dict[str, Any]],
) -> pd.DataFrame:
    """
    Enrich a vehicles DataFrame with Level1..Level6 based on customer hierarchy.

    :param vehicles_df: Vehicles DataFrame (output from json_normalize).
    :param customers: Raw customer rows from ``fetch_insubiz_customers``.
    :return: Copy of vehicles_df with Level1..Level6 columns added.
    """
    df = vehicles_df.copy()

    customer_by_id = {
        int(c["id"]): c
        for c in customers
        if isinstance(c, dict) and c.get("id") is not None
    }

    if "customer_id" in df.columns:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")

        unique_customer_ids = df["customer_id"].dropna().astype(int).unique().tolist()

        levels_map = {
            cid: build_customer_levels(customer_id=cid, customer_by_id=customer_by_id)
            for cid in unique_customer_ids
        }

        levels_df = (
            pd.DataFrame.from_dict(levels_map, orient="index")
            .reset_index(names="customer_id")
        )
        levels_df["customer_id"] = levels_df["customer_id"].astype("Int64")

        df = df.merge(levels_df, on="customer_id", how="left")
    else:
        for i in range(1, 7):
            df[f"Level{i}"] = ""

    return df


def find_latest_motorstyrelsen_excel_attachment(
    email_reader: EmailReader,
    mailbox: str = "INBOX",
    criteria: str = "UNSEEN",
    filename_prefixes: Iterable[str] = ("Aktindsigt",),
    max_emails: int = 50,
) -> tuple[bytes, str, bytes] | None:
    """
    Find the newest Motorstyrelsen Excel attachment in a mailbox.

    :param email_reader: EmailReader used to fetch emails.
    :param mailbox: Mailbox/folder to search in (e.g. "INBOX").
    :param criteria: IMAP search criteria (e.g. "ALL", "UNSEEN").
    :param filename_prefixes: Allowed attachment filename prefixes.
    :param max_emails: Maximum number of emails to fetch
    :return: (uid, filename, content_bytes) for the first matching attachment, or None.
    """
    emails, failed = email_reader.get_emails(
        mailbox=mailbox,
        criteria=criteria,
        set_flags=None,
        max=max_emails,
        low_to_high=False,
    )

    logger.info(f"Fetched {len(emails)} email(s), {len(failed)} failed to fetch.")

    for msg in emails:
        uid: bytes = getattr(msg, "uid", None)

        for part in msg.iter_attachments():
            filename = part.get_filename() or ""
            if not filename.lower().endswith(".xlsx"):
                continue
            if filename_prefixes and not any(filename.startswith(p) for p in filename_prefixes):
                continue

            content = part.get_payload(decode=True)
            if content:
                return uid, filename, content

    return None
