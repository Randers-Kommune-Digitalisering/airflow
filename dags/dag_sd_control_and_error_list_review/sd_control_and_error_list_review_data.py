import logging
import time
from collections import Counter
from playwright.sync_api import (
    Error as PlaywrightError,
    Frame,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)
from dag_sd_fleksjobrefusion.sd_fleksjobrefusion_data import (
    login_to_sd,
)

logger = logging.getLogger(__name__)

MESSAGES_TABLE_SELECTOR = "#messages_data"


def open_error_and_control_messages_context(page: Page) -> bool:
    """
    Open the Kg information -> Fejl- & Kontrolmeddelelser context.

    :param page: Active Personaleweb page.
    :return: True when the context was opened successfully.
    """
    try:
        logger.info("Opening Kg information and Fejl- & Kontrolmeddelelser context")
        nav_frame = page.frame_locator("#insideiframe").frame_locator(
            "xpath=//*[@id='jbFrameset']/frame"
        )

        nav_frame.locator("#fe171").click(timeout=20000)
        nav_frame.locator("#tab_2885").click(
            timeout=20000
        )

        logger.info("Fejl- & Kontrolmeddelelser context is ready")
        return True
    except PlaywrightTimeoutError:
        logger.exception("Timeout while opening Fejl- & Kontrolmeddelelser context")
        return False


def process_department(page: Page, department_code: str) -> bool:
    """
    Search for a department in SD Personaleweb and open the first result.

    :param page: Active Personaleweb page.
    :param department_code: SD department code (afdelingskode).
    :return: True when the department was opened, or False after a timeout.
    """
    try:
        logger.info(f"Searching for department {department_code}")
        logger.info("Clicking on the search field...")
        search_field = page.locator(
            "xpath=/html/body/div[3]/div/div/div/div/div[2]/div[2]/input"
        )
        search_field.wait_for(state="visible", timeout=20000)
        search_field.fill("")
        search_field.fill(department_code)
        page.wait_for_timeout(1500)  # Wait for search suggestions to appear
        search_field.click()

        logger.info("Clicking on the first search result")
        page.locator("xpath=/html/body/ul/li[1]").click(timeout=20000)
        page.wait_for_timeout(2000)

        logger.info(f"Department {department_code} opened")
        return True
    except PlaywrightTimeoutError:
        logger.exception(f"Timeout while processing department {department_code}")
        return False


def _find_frame_with_messages_table(
    page: Page,
    timeout_ms: int = 20_000,
) -> Frame | None:
    """
    Find the frame containing the Fejl- & Kontrolmeddelelser table.

    :param page: Active Personaleweb page.
    :param timeout_ms: Max wait time in milliseconds.
    :return: Frame containing the table, or None when not found.
    """
    deadline = time.time() + (timeout_ms / 1000)
    while time.time() < deadline:
        for frame in page.frames:
            try:
                if frame.locator(MESSAGES_TABLE_SELECTOR).count() > 0:
                    return frame
            except PlaywrightError:
                continue

        time.sleep(0.5)

    return None


def _set_rows_per_page(frame: Frame, rows_per_page: str = "100") -> None:
    """
    Set the table page-size dropdown so all rows are rendered.

    :param frame: Frame containing the messages table.
    :param rows_per_page: Page size to select.
    :return: None; a missing dropdown is logged and ignored.
    """
    try:
        dropdown = frame.locator("#messages\\:j_id17")
        dropdown.wait_for(state="visible", timeout=10000)
        try:
            dropdown.select_option(label=rows_per_page, timeout=10000)
        except (PlaywrightTimeoutError, PlaywrightError):
            dropdown.select_option(value=rows_per_page, timeout=10000)
        frame.wait_for_timeout(2000)
        logger.info(f"Table page size set to {rows_per_page} rows")
    except (PlaywrightTimeoutError, PlaywrightError):
        logger.warning(f"Could not set table page size to {rows_per_page}; using current page size")


def read_message_codes(frame: Frame) -> list[str]:
    """
    Read all "Kode" values from the Fejl- & Kontrolmeddelelser table.

    :param frame: Frame containing the messages table.
    :return: List of codes in table row order.
    """
    _set_rows_per_page(frame=frame)

    rows = frame.locator(f"{MESSAGES_TABLE_SELECTOR} > tr")

    # Rows are rendered progressively, so wait until the count stops growing.
    previous_count = -1
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            current_count = rows.count()
        except PlaywrightError:
            current_count = 0

        if current_count > 0 and current_count == previous_count:
            break

        previous_count = current_count
        time.sleep(0.5)

    code_cells = frame.locator(
        f"{MESSAGES_TABLE_SELECTOR} > tr > td:nth-child(5)"
    )

    try:
        codes = [text.strip() for text in code_cells.all_inner_texts()]
    except PlaywrightError:
        logger.exception("Could not read codes from the messages table")
        return []

    codes = [code for code in codes if code]
    logger.info(f"Read {len(codes)} code(s) from {rows.count()} row(s)")
    logger.info(f"Codes in table: {dict(Counter(codes))}")
    return codes


def _set_row(frame: Frame, row_index: int) -> bool:
    """
    Click the "Set" checkbox for a single table row.

    :param frame: Frame containing the messages table.
    :param row_index: Zero-based row index as used in the row element ids.
    :return: True when the checkbox was clicked.
    """
    try:
        checkbox = frame.locator(
            f"#messages\\:{row_index}\\:j_idt85 .ui-chkbox-box"
        )
        checkbox.wait_for(state="visible", timeout=10000)
        checkbox.click(timeout=10000)
        frame.wait_for_timeout(500)
        return True
    except (PlaywrightTimeoutError, PlaywrightError):
        logger.exception(f"Could not click Set for row {row_index}")
        return False


def review_department_messages(
    page: Page,
    allowed_codes: list[str],
) -> list[str]:
    """
    Mark all rows whose code is in the allow-list as "Set".

    :param page: Active Personaleweb page.
    :param allowed_codes: Codes that should be marked.
    :return: Codes that were marked, in table row order.
    """
    frame = _find_frame_with_messages_table(page=page)
    if frame is None:
        logger.warning("Fejl- & Kontrolmeddelelser table was not found")
        return []

    codes = read_message_codes(frame=frame)
    allowed = {code.strip().casefold() for code in allowed_codes}
    marked: list[str] = []

    for row_index, code in enumerate(codes):
        if code.casefold() not in allowed:
            continue

        if _set_row(frame=frame, row_index=row_index):
            marked.append(code)
            logger.info(f"Marked row {row_index} with code {code} as Set")

    return marked


def run_sd_control_and_error_list_review_job(
    username: str,
    password: str,
    department_codes: list[str],
    allowed_codes: list[str],
    sd_url: str,
    headless: bool = True,
) -> tuple[bool, dict[str, list[str]]]:
    """
    Run the SD Control and Error List Review flow for all departments.

    :param username: ADFS username.
    :param password: ADFS password.
    :param department_codes: SD department codes (afdelingskoder) to review.
    :param allowed_codes: Codes that should be marked as "Set" when found.
    :param sd_url: SD landing page URL.
    :param headless: Whether to run browser headless.
    :return: Tuple of success flag and matched codes per department.
    """
    logger.info("Starting SD Control and Error List Review browser flow")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=headless,
            args=[
                "--incognito",
                (
                    "--disable-features=msImplicitSignIn,"
                    "EnableWindowsGSAutoSignIn"
                ),
            ],
        )
        context = browser.new_context()
        page = context.new_page()

        try:
            active_page = login_to_sd(
                page=page,
                sd_url=sd_url,
                username=username,
                password=password,
            )
            if active_page is None:
                return False, {}

            if not open_error_and_control_messages_context(page=active_page):
                return False, {}

            matches: dict[str, list[str]] = {}
            failed_departments: list[str] = []

            for department_code in department_codes:
                if not process_department(
                    page=active_page,
                    department_code=department_code,
                ):
                    failed_departments.append(department_code)
                    continue

                relevant_codes = review_department_messages(
                    page=active_page,
                    allowed_codes=allowed_codes,
                )
                matches[department_code] = relevant_codes

                if relevant_codes:
                    for position, code in enumerate(relevant_codes, start=1):
                        logger.info(f"Department {department_code} match {position}/{len(relevant_codes)}: code {code}")
                else:
                    logger.info(f"Department {department_code} has no relevant codes")

            if failed_departments:
                logger.error(f"Failed to review department(s): {', '.join(failed_departments)}")
                return False, matches

            logger.info("SD Control and Error List Review flow completed")
            return True, matches
        finally:
            context.close()
            browser.close()
