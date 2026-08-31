import datetime
import logging
from typing import Any

import pandas as pd
from playwright.sync_api import (
    Error as PlaywrightError,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

logger = logging.getLogger(__name__)

NoegletalPayload = dict[str, Any]

# Oldest month that should be included in the full rebuild of the BOM tables.
HISTORY_START = datetime.date(2023, 1, 1)


def _shift_months(month_start: datetime.date, months: int) -> datetime.date:
    """
    Shift a first-of-month date by a number of months.

    :param month_start: First day of a month.
    :param months: Number of months to shift (may be negative).
    :return: First day of the shifted month.
    """
    total_months = month_start.year * 12 + (month_start.month - 1) + months
    return datetime.date(total_months // 12, total_months % 12 + 1, 1)


def _month_boundaries(
    run_date: datetime.date,
    history_start: datetime.date = HISTORY_START,
) -> list[datetime.date]:
    """
    Build the list of 'Til' boundaries (first day of month) for every completed month.

    :param run_date: Date of the Airflow DAG run (Europe/Copenhagen).
    :param history_start: First month that should be included.
    :return: List of first-of-month dates from history_start+1 month up to run_date's month.
    """
    boundary = _shift_months(month_start=history_start.replace(day=1), months=1)
    last_boundary = run_date.replace(day=1)

    boundaries: list[datetime.date] = []
    while boundary <= last_boundary:
        boundaries.append(boundary)
        boundary = _shift_months(month_start=boundary, months=1)
    return boundaries


def _format_range(fra: datetime.date, til: datetime.date) -> tuple[str, str]:
    """
    Format a date range the way the BOM datepicker expects it.

    :param fra: Start date.
    :param til: End date.
    :return: Tuple (fra, til) as strings in 'DD-MM-YYYY' format.
    """
    return fra.strftime("%d-%m-%Y"), til.strftime("%d-%m-%Y")


def _close_open_multiselect_dropdowns(page: Page, timeout_ms: int = 10_000) -> None:
    """
    Close any open Bootstrap multiselect dropdowns.

    :param page: Active Playwright page.
    :param timeout_ms: Max wait time for dropdowns to close.
    """
    try:
        if page.locator("div.btn-group.open").count() == 0:
            return

        page.keyboard.press("Escape")
        page.evaluate("document.body.click();")
        page.wait_for_function(
            "selector => document.querySelectorAll(selector).length === 0",
            arg="div.btn-group.open",
            timeout=timeout_ms,
        )
    except (PlaywrightTimeoutError, PlaywrightError):
        logger.debug("Could not confirm that multiselect dropdowns were closed")


def _set_date_range(page: Page, fra: str, til: str) -> None:
    """
    Set the BOM datepicker range (Fra/Til) and trigger validation.

    :param page: Active Playwright page.
    :param fra: Start date as 'DD-MM-YYYY'.
    :param til: End date as 'DD-MM-YYYY'.
    """
    _close_open_multiselect_dropdowns(page=page)

    from_input = page.locator("#datepicker > input:nth-child(1)")
    to_input = page.locator("#datepicker > input:nth-child(2)")

    from_input.wait_for(state="visible", timeout=30000)
    from_input.scroll_into_view_if_needed(timeout=10000)
    from_input.fill(fra, timeout=20000)

    to_input.wait_for(state="visible", timeout=30000)
    to_input.fill(til, timeout=20000)
    to_input.press("Tab")


def _click_noegletal_and_wait_refresh(page: Page) -> None:
    """
    Click 'Søg' (if present) and then 'Nøgletal', and wait for the table to refresh.

    :param page: Active Playwright page.
    """
    try:
        before_html = page.locator("#servicemaal-noegletal-table").inner_html(timeout=5000)
    except (PlaywrightTimeoutError, PlaywrightError):
        before_html = ""

    search_button = page.locator(
        "body > div > div.container > form > div:nth-child(3) > div > span > "
        "button.btn.btn-primary"
    )
    if search_button.count() > 0:
        search_button.first.scroll_into_view_if_needed(timeout=10000)
        search_button.first.click(timeout=30000)

    page.locator("#servicemaal-result-toggler > button:nth-child(2)").click(timeout=30000)

    try:
        if before_html:
            page.wait_for_function(
                """([selector, previousHtml]) => {
                    const table = document.querySelector(selector);
                    if (!table) {
                        return false;
                    }
                    return table.innerHTML.length > 0 && table.innerHTML !== previousHtml;
                }""",
                arg=["#servicemaal-noegletal-table", before_html],
                timeout=30000,
            )
        else:
            page.locator("#servicemaal-noegletal-table tbody tr").first.wait_for(
                state="attached",
                timeout=30000,
            )
    except PlaywrightTimeoutError:
        page.locator("#servicemaal-noegletal-table tbody tr").first.wait_for(
            state="attached",
            timeout=30000,
        )


def _extract_noegletal_payload(page: Page, max_rows: int = 6) -> NoegletalPayload:
    """
    Extract values from the 'Nøgletal' table.

    :param page: Active Playwright page.
    :param max_rows: Maximum number of table rows to extract.
    :return: Payload with dates and lists for 'Kategori', 'Sagsbehandling', 'Servicemal Procent'.
    """
    rows = page.locator("#servicemaal-noegletal-table tbody tr")
    rows.first.wait_for(state="attached", timeout=30000)

    fra_val = (
        page.locator("#datepicker > input:nth-child(1)").input_value(timeout=10000) or ""
    ).strip()
    til_val = (
        page.locator("#datepicker > input:nth-child(2)").input_value(timeout=10000) or ""
    ).strip()

    row_count = min(rows.count(), max_rows)

    kategori: list[str] = []
    sagsbehandling: list[str] = []
    servicemaal_procent: list[str] = []

    for row_index in range(row_count):
        cell_texts = [
            text.strip()
            for text in rows.nth(row_index).locator("td").all_inner_texts()
        ]

        kategori.append(cell_texts[0] if len(cell_texts) > 0 else "")
        sagsbehandling.append(cell_texts[3] if len(cell_texts) > 3 else "")
        servicemaal_procent.append(cell_texts[13] if len(cell_texts) > 13 else "")

    return {
        "Til Dato": til_val,
        "Fra Dato": fra_val,
        "Kategori": kategori,
        "Sagsbehandling": sagsbehandling,
        "Servicemal Procent": servicemaal_procent,
    }


def login_to_bom(page: Page, bom_url: str, username: str, password: str) -> bool:
    """
    Log into BOM and wait for the main navigation to be available.

    :param page: Active Playwright page.
    :param bom_url: BOM login URL.
    :param username: ADFS username.
    :param password: ADFS password.
    :return: True when login succeeded, otherwise False.
    """
    try:
        logger.info("Navigating to BOM login page...")
        page.goto(bom_url, wait_until="domcontentloaded", timeout=60000)

        logger.info("Selecting kommune...")
        kommune_select = page.locator("form div div div select").first
        kommune_select.wait_for(state="visible", timeout=30000)
        kommune_select.select_option(label="Randers Kommune (RPA)", timeout=30000)

        logger.info("Clicking Fortsæt...")
        page.locator("form div div a").first.click(timeout=30000)

        logger.info("Entering username/password...")
        page.locator("#userNameInput").fill(username, timeout=30000)
        page.locator("#passwordInput").fill(password, timeout=30000)

        logger.info("Clicking Submit...")
        page.locator("#submitButton").click(timeout=30000)

        logger.info("Waiting for BOM to load after login submit...")
        page.locator("xpath=/html/body/div/header/div/div/div[2]/nav").wait_for(
            state="visible",
            timeout=60000,
        )
        logger.info("Login submit done; BOM loaded.")
        return True
    except PlaywrightTimeoutError:
        logger.exception("Timeout during BOM login")
        return False
    except PlaywrightError:
        logger.exception("BOM login failed")
        return False


def open_servicemaal_context(page: Page) -> bool:
    """
    Navigate to 'Statistik og Servicemål' and apply the Byg/Servicemål filters.

    :param page: Active Playwright page.
    :return: True when the filters are applied, otherwise False.
    """
    try:
        logger.info("Navigating to Statistik og Servicemål...")
        page.locator("xpath=/html/body/div/header/div/div/div[2]/nav").click(timeout=30000)
        page.locator(
            "body > div > header > div > div > div.span4.offset2 > nav > ul > li > ul > "
            "li:nth-child(4) > a"
        ).click(timeout=30000)

        logger.info("Setting Sagsområde = Byg...")
        page.locator(
            "form > div:nth-child(2) > div > div:nth-child(2) > button"
        ).click(timeout=30000)

        byg_checkbox = page.locator(
            "form > div:nth-child(2) > div > div:nth-child(2) > ul > li:nth-child(2) > a > "
            "label > input"
        )
        byg_checkbox.wait_for(state="visible", timeout=30000)
        if not byg_checkbox.is_checked():
            byg_checkbox.click(timeout=30000)

        logger.info("Selecting Servicemål checkboxes...")
        page.locator(
            "form > div:nth-child(2) > div > div:nth-child(4) > button"
        ).click(timeout=30000)

        servicemaal_label_selectors = [
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(2) > a > label",
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(3) > a > label",
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(4) > a > label",
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(5) > a > label",
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(6) > a > label",
        ]

        for selector in servicemaal_label_selectors:
            label = page.locator(selector)
            label.wait_for(state="visible", timeout=30000)
            label.scroll_into_view_if_needed(timeout=10000)
            logger.info(f"Clicking servicemål: {label.inner_text().strip() or selector}")
            label.click(timeout=30000)

        _close_open_multiselect_dropdowns(page=page)
        logger.info("Servicemål context is ready")
        return True
    except PlaywrightTimeoutError:
        logger.exception("Timeout while opening Servicemål context")
        return False
    except PlaywrightError:
        logger.exception("Failed to open Servicemål context")
        return False


def fetch_bom_data(
    page: Page,
    run_date: datetime.date,
) -> tuple[list[NoegletalPayload], list[NoegletalPayload]] | None:
    """
    Extract monthly and glidende gennemsnit 'Nøgletal' for every completed month since HISTORY_START.

    :param page: Active Playwright page with Servicemål filters applied.
    :param run_date: Date of the Airflow DAG run (Europe/Copenhagen).
    :return: Tuple (monthly_payloads, glidende_payloads) on success, otherwise None.
    """
    boundaries = _month_boundaries(run_date=run_date)
    if not boundaries:
        logger.warning(f"No completed months to extract for run date {run_date}")
        return None

    monthly_payloads: list[NoegletalPayload] = []
    glidende_payloads: list[NoegletalPayload] = []

    try:
        for boundary in boundaries:
            from_monthly, to_monthly = _format_range(
                fra=_shift_months(month_start=boundary, months=-1),
                til=boundary,
            )
            logger.info(f"Setting date range (monthly) Fra={from_monthly}, Til={to_monthly}")
            _set_date_range(page=page, fra=from_monthly, til=to_monthly)

            logger.info("Clicking Nøgletal (monthly)...")
            _click_noegletal_and_wait_refresh(page=page)
            monthly_payloads.append(_extract_noegletal_payload(page=page))

            fra_g, til_g = _format_range(
                fra=_shift_months(month_start=boundary, months=-12),
                til=boundary,
            )
            logger.info(f"Setting date range (glidende_gennemsnit) Fra={fra_g}, Til={til_g}")
            _set_date_range(page=page, fra=fra_g, til=til_g)

            logger.info("Clicking Nøgletal (glidende_gennemsnit)...")
            _click_noegletal_and_wait_refresh(page=page)
            glidende_payloads.append(_extract_noegletal_payload(page=page))

        logger.info(
            f"BOM data extracted successfully for {len(boundaries)} month(s) "
            f"(monthly + glidende_gennemsnit)."
        )
        return monthly_payloads, glidende_payloads
    except PlaywrightTimeoutError:
        logger.exception("Timeout while extracting BOM data")
        return None
    except PlaywrightError:
        logger.exception("Failed to extract BOM data")
        return None


def _payload_to_df(payload: NoegletalPayload) -> pd.DataFrame:
    """
    Convert a single BOM payload into a DataFrame.

    :param payload: Extracted payload from the Nøgletal table.
    :return: DataFrame with normalized columns.
    """
    return pd.DataFrame(
        {
            "Fra Dato": payload.get("Fra Dato", ""),
            "Til Dato": payload.get("Til Dato", ""),
            "Kategori": payload.get("Kategori", []),
            "Sagsbehandlingstid": payload.get("Sagsbehandling", []),
            "Servicemål i procent": payload.get("Servicemal Procent", []),
        }
    )


def process_bom_payloads(
    payloads: tuple[list[NoegletalPayload], list[NoegletalPayload]] | None,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    Transform extracted BOM payloads into pandas DataFrames.

    :param payloads: Tuple (monthly_payloads, glidende_payloads) returned by fetch_bom_data().
    :return: Tuple (df_monthly, df_glidende), or (None, None) on empty input.
    """
    if not payloads:
        return None, None

    monthly_payloads, glidende_payloads = payloads
    if not monthly_payloads or not glidende_payloads:
        return None, None

    df_monthly = pd.concat(
        [_payload_to_df(payload=payload) for payload in monthly_payloads],
        ignore_index=True,
    )
    df_glidende = pd.concat(
        [_payload_to_df(payload=payload) for payload in glidende_payloads],
        ignore_index=True,
    )
    return df_monthly, df_glidende


def run_bom_job(
    username: str,
    password: str,
    bom_url: str,
    run_date: datetime.date,
    headless: bool = True,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    Run the full BOM Playwright flow and return the extracted DataFrames.

    :param username: ADFS username.
    :param password: ADFS password.
    :param bom_url: BOM login URL.
    :param run_date: Date of the Airflow DAG run (Europe/Copenhagen).
    :param headless: Whether to run browser headless.
    :return: Tuple (df_monthly, df_glidende), or (None, None) when the flow fails.
    """
    logger.info(f"Starting BOM browser flow for run date {run_date}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=headless,
            args=[
                "--incognito",
            ],
        )
        context = browser.new_context()
        page = context.new_page()

        try:
            if not login_to_bom(
                page=page,
                bom_url=bom_url,
                username=username,
                password=password,
            ):
                return None, None

            if not open_servicemaal_context(page=page):
                return None, None

            payloads = fetch_bom_data(page=page, run_date=run_date)
            df_monthly, df_glidende = process_bom_payloads(payloads=payloads)

            logger.info("BOM browser flow completed")
            return df_monthly, df_glidende
        finally:
            context.close()
            browser.close()
