import logging
import time
from typing import Iterable

import pandas as pd
from playwright.sync_api import (
    BrowserContext,
    Error as PlaywrightError,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)
from rkdigi.email_handling import EmailReader

logger = logging.getLogger(__name__)

PERSONALEWEB_TILE_SELECTOR = (
    "#product-cf662da2-9d3c-0108-e043-0a10f6400108 "
    "div[role='button'][aria-label='Personaleweb']"
)


def excel_to_sd_fleksjobrefusion_config(
    df: pd.DataFrame,
) -> list[dict[str, str]]:
    """
    Parse SD Fleksjobrefusion rows from Excel into browser-process config.

    :param df: DataFrame loaded from attached Excel.
    :return: List of person rows with keys:
        tjenestenummer, institution, beloeb, loenart.
    """
    df.columns = df.columns.str.strip()
    config: list[dict[str, str]] = []

    for _, row in df.iterrows():
        tjnr = str(row["TJNR."]).zfill(5)

        if pd.notna(row["Lønart 684"]):
            beloeb = round(float(row["Lønart 684"]), 2)
            config.append(
                {
                    "tjenestenummer": tjnr,
                    "institution": str(row["Int."]),
                    "beloeb": f"-{beloeb:.2f}".replace(".", ","),
                    "loenart": "684",
                }
            )
        elif pd.notna(row["Lønart 685"]):
            beloeb = round(float(row["Lønart 685"]), 2)
            config.append(
                {
                    "tjenestenummer": tjnr,
                    "institution": str(row["Int."]),
                    "beloeb": f"-{beloeb:.2f}".replace(".", ","),
                    "loenart": "685",
                }
            )

    logger.info("Parsed %s SD Fleksjobrefusion row(s) from Excel", len(config))
    return config


def find_latest_fleksjobrefusion_excel_attachment(
    email_reader: EmailReader,
    mailbox: str = "INBOX",
    criteria: str = "UNSEEN",
    filename_prefixes: Iterable[str] = ("Fleksjobrefusion"),
    max_emails: int = 50,
) -> tuple[bytes, str, bytes] | None:
    """
    Find the newest matching Excel attachment in an IMAP mailbox.

    :param email_reader: EmailReader used to fetch emails.
    :param mailbox: Mailbox/folder to search in (e.g. "INBOX").
    :param criteria: IMAP search criteria (e.g. "ALL", "UNSEEN").
    :param filename_prefixes: Allowed attachment filename prefixes.
    :param max_emails: Maximum number of emails to fetch and scan.
    :return: (uid, filename, content_bytes) for the first matching attachment, or None.
    """
    emails, failed = email_reader.get_emails(
        mailbox=mailbox,
        criteria=criteria,
        set_flags=None,
        max=max_emails,
        low_to_high=False,  # start with newest emails first
    )

    logger.info(f"Fetched {len(emails)} email(s), {len(failed)} failed to fetch.")

    for msg in emails:
        uid: bytes = getattr(msg, "uid", None)
        subject = msg.get("Subject", "")
        logger.info(f"Email UID: {uid}, Subject: {subject}")

        for part in msg.iter_attachments():
            filename = part.get_filename() or ""
            filename_lc = filename.lower()
            if not filename_lc.endswith(".xlsx"):
                continue

            if not any(filename_lc.startswith(p.lower()) for p in filename_prefixes):
                continue

            content = part.get_payload(decode=True)  # bytes
            if not content:
                continue

            return uid, filename, content

    return None


def _find_page_with_selector(
    context: BrowserContext,
    selector: str,
    timeout_ms: int = 60_000,
) -> Page | None:
    """
    Find the newest open page that contains a visible selector.

    :param context: Playwright browser context.
    :param selector: Selector that must be visible.
    :param timeout_ms: Max wait time in milliseconds.
    :return: Matching page or None when not found within timeout.
    """
    deadline = time.time() + (timeout_ms / 1000)
    while time.time() < deadline:
        for candidate_page in reversed(context.pages):
            try:
                candidate_page.wait_for_load_state(
                    "domcontentloaded",
                    timeout=1000,
                )
            except PlaywrightTimeoutError:
                pass
            except PlaywrightError:
                continue

            try:
                candidate_page.locator(selector).first.wait_for(
                    state="visible",
                    timeout=1000,
                )
                return candidate_page
            except PlaywrightTimeoutError:
                continue
            except PlaywrightError:
                continue

        time.sleep(0.5)

    return None


def _log_open_pages(context: BrowserContext, prefix: str) -> None:
    """
    Log currently open pages to debug tab/window switching.

    :param context: Playwright browser context.
    :param prefix: Prefix added to each log line.
    """
    for index, candidate_page in enumerate(context.pages):
        try:
            title = candidate_page.title()
        except PlaywrightError:
            title = "<unavailable-during-navigation>"

        try:
            url = candidate_page.url
        except PlaywrightError:
            url = "<unavailable-during-navigation>"

        logger.debug("%s page[%s]: title=%r url=%r", prefix, index, title, url)


def _resolve_post_login_page(
    context: BrowserContext,
    previous_pages: list[Page],
    fallback_page: Page,
    timeout_ms: int = 20_000,
) -> Page:
    """
    Resolve active page after login submit, allowing popup/new-window transitions.

    :param context: Playwright browser context.
    :param previous_pages: Snapshot of pages before login submit.
    :param fallback_page: Existing page used when no new page appears.
    :param timeout_ms: Max wait time for new page transitions.
    :return: Page to continue the flow on.
    """
    deadline = time.time() + (timeout_ms / 1000)
    previous_ids = {id(page) for page in previous_pages}

    while time.time() < deadline:
        current_pages = list(context.pages)
        new_pages = [page for page in current_pages if id(page) not in previous_ids]
        if new_pages:
            resolved_page = new_pages[-1]
            try:
                resolved_page.wait_for_load_state(
                    "domcontentloaded",
                    timeout=3000,
                )
            except (PlaywrightTimeoutError, PlaywrightError):
                pass
            return resolved_page

        for candidate_page in reversed(current_pages):
            try:
                if candidate_page.locator(PERSONALEWEB_TILE_SELECTOR).count() > 0:
                    return candidate_page
            except PlaywrightError:
                continue

        time.sleep(0.4)

    return fallback_page


def login_to_sd(
    page: Page,
    sd_url: str,
    username: str,
    password: str,
) -> Page | None:
    """
    Perform login flow for SD Fleksjobrefusion and switch to Personaleweb.

    :param page: Playwright page used to start navigation.
    :param sd_url: SD landing page URL.
    :param username: ADFS username.
    :param password: ADFS password.
    :return: The active Personaleweb page, or None when login fails.
    """
    try:
        logger.info("Navigating to login page...")
        page.goto(sd_url, wait_until="domcontentloaded", timeout=60000)

        logger.info("Waiting for 'Log In' button...")
        page.locator(
            "xpath=//*[@id='module-116']/div/div/div/div/a"
        ).click(timeout=20000)
        logger.info("'Log In' button clicked.")

        logger.info("Waiting for 'Arbejdsplads-Login-View' button...")
        try:
            with page.context.expect_page(timeout=20000) as page_info:
                page.locator("#arbejdspladsButton").click(timeout=20000)
            active_page = page_info.value
        except PlaywrightTimeoutError:
            active_page = page
        logger.info("'Arbejdsplads-Login-View' button clicked.")
        active_page.wait_for_load_state("domcontentloaded", timeout=30000)

        logger.info("Selecting IDP in iframe 'iframe-oiosaml'...")
        idp_select = active_page.frame_locator(
            "iframe#iframe-oiosaml"
        ).locator("#oiosaml-idp")
        logger.info("Waiting for Dropdown menu element to be clickable...")
        idp_select.wait_for(state="visible", timeout=30000)
        logger.info("Dropdown menu element found. Selecting 'Randers Kommune'...")
        idp_select.select_option(label="Randers Kommune")
        logger.info("Clicked on Randers Kommune IDP option.")

        logger.info("Waiting for Arbejdsplads-log in Button to be clickable...")
        active_page.frame_locator("iframe#iframe-oiosaml").locator(
            "#oiosaml-login-btn"
        ).click(timeout=30000)
        logger.info("Arbejdsplads Login button clicked.")

        logger.info("Entering username...")
        active_page.locator("#userNameInput").fill(username, timeout=30000)
        logger.info("Entered username successfully.")

        logger.info("Entering password...")
        active_page.locator("#passwordInput").fill(password, timeout=30000)
        logger.info("Entered password successfully.")

        logger.info("Waiting on the login button...")
        pre_submit_pages = list(active_page.context.pages)
        active_page.locator("#submitButton").click(timeout=30000)
        logger.info("Clicked on the login button.")

        active_page = _resolve_post_login_page(
            context=active_page.context,
            previous_pages=pre_submit_pages,
            fallback_page=active_page,
        )
        logger.info("Resolved active page after login submit")

        logger.info("Locating page with Personaleweb tile")
        _log_open_pages(context=active_page.context, prefix="Before Personaleweb lookup")
        personaleweb_host_page = _find_page_with_selector(
            context=active_page.context,
            selector=PERSONALEWEB_TILE_SELECTOR,
            timeout_ms=90000,
        )
        if personaleweb_host_page is None:
            _log_open_pages(context=active_page.context, prefix="Lookup failed")
            raise PlaywrightTimeoutError(
                "Could not find any open page containing Personaleweb tile"
            )

        logger.info("Opening Personaleweb")
        personaleweb_tile = personaleweb_host_page.locator(
            PERSONALEWEB_TILE_SELECTOR
        ).first
        personaleweb_tile.wait_for(state="visible", timeout=30000)

        before_personaleweb = list(personaleweb_host_page.context.pages)
        personaleweb_tile.click(timeout=30000)
        personaleweb_host_page.wait_for_timeout(1200)
        pw_pages = [
            p
            for p in personaleweb_host_page.context.pages
            if p not in before_personaleweb
        ]

        personaleweb_page = (
            pw_pages[-1] if pw_pages else personaleweb_host_page
        )
        personaleweb_page.wait_for_load_state(
            "domcontentloaded",
            timeout=30000,
        )

        _log_open_pages(context=active_page.context, prefix="After Personaleweb click")
        logger.info("Login completed and switched to Personaleweb tab")
        return personaleweb_page
    except PlaywrightTimeoutError:
        logger.exception("Timeout during SD login")
        return None
    except Exception:
        logger.exception("SD login failed")
        return None


def process_person_playwright(
    page: Page,
    tjenestenummer: str,
    institution: str,
    beloeb: str,
    loenart: str,
) -> bool:
    """
    Process one person row in SD Personaleweb.

    :param page: Active Personaleweb page.
    :param tjenestenummer: Employee service number.
    :param institution: Institution identifier.
    :param beloeb: Amount value.
    :param loenart: Wage type value.
    :return: True when flow succeeds, otherwise False.
    """
    try:
        logger.info("Entering SD Personaleweb...")

        logger.info("Clicking on the search field...")
        search_field = page.locator(
            "xpath=/html/body/div[3]/div/div/div/div/div[2]/div[2]/input"
        )
        search_field.wait_for(state="visible", timeout=20000)
        search_field.fill("")
        search_field.fill(f"{tjenestenummer} {institution}")
        page.wait_for_timeout(1500)  # Wait for search suggestions to appear
        search_field.click()
        logger.info(f"Searching for person with tjenestenummer: {tjenestenummer} and institution: {institution} and loenart: {loenart}")

        logger.info("Clicking on the first name in the search results...")
        page.locator("xpath=/html/body/ul/li[1]").click(timeout=20000)

        logger.info("Switching to frame under 'jbFrameset'...")
        nav_frame = page.frame_locator("#insideiframe").frame_locator(
            "xpath=//*[@id='jbFrameset']/frame"
        )

        logger.info("Clicking on Indberetning...")
        nav_frame.locator("#fe20").click(timeout=20000)

        logger.info("Clicking on Merarbejde...")
        nav_frame.locator("#tab_2737").click(timeout=20000)

        logger.info("Switching to 'merarbejde' frame...")
        merarbejde_frame = page.frame_locator("#insideiframe").frame_locator(
            "xpath=//frameset[@id='innerFrameset']/frame[@name='insidemain']"
        ).frame_locator("frame[name='merarbejde']")

        logger.info("Waiting for the amount input field...")
        beloeb_input = merarbejde_frame.locator("#pageForm\\:beloeb")
        beloeb_input.wait_for(state="visible", timeout=20000)
        beloeb_input.fill(str(beloeb))
        logger.info(f"Amount input set to: {beloeb} kr.")

        logger.info("Waiting for the wage type input field...")
        loenart_input = merarbejde_frame.locator("#pageForm\\:loenart_input")
        loenart_input.wait_for(state="visible", timeout=20000)
        loenart_value = str(loenart)

        # Type slowly so dropdown suggestions have time to load.
        loenart_input.click(timeout=20000)
        loenart_input.fill("")
        loenart_input.type(loenart_value, delay=140)

        # Ensure full value is present before confirming with Enter.
        deadline = time.time() + 8
        while time.time() < deadline:
            current_value = loenart_input.input_value()
            if current_value.strip() == loenart_value:
                break
            page.wait_for_timeout(150)
        else:
            raise PlaywrightTimeoutError(
                "Wage type input did not contain the expected value before Enter"
            )

        page.wait_for_timeout(400)
        loenart_input.press("Enter")
        logger.info(f"Wage type input set to: {loenart_value}")

        page.wait_for_timeout(2000)

        logger.info("Waiting for the approved input field...")
        godkendt_input = merarbejde_frame.locator("#pageForm\\:godkendt")
        godkendt_input.wait_for(state="visible", timeout=20000)
        godkendt_input.click(timeout=20000)
        logger.info("Approved input clicked.")
        page.wait_for_timeout(2500)

        # Commented out while testing so it does not actually submit the form during development.
        # logger.info("Waiting for the save button...")
        # gem_button = merarbejde_frame.locator("#pageForm\\:j_idt108")
        # gem_button.wait_for(state="visible", timeout=20000)
        # gem_button.click(timeout=20000)
        # logger.info("Save button clicked.")
        # page.wait_for_timeout(2500)

        logger.info(f"✅ {tjenestenummer} ({institution}) behandlet med beløb {beloeb} og lønart {loenart}.")
        return True
    except PlaywrightTimeoutError:
        logger.exception(f"Timeout while processing {tjenestenummer} {institution}")
        return False


def run_sd_fleksjobrefusion_job(
    username: str,
    password: str,
    persons: list[dict[str, str]],
    sd_url: str = "https://www.silkeborgdata.dk",
    headless: bool = True,
) -> tuple[bool, list[dict[str, str]]]:
    """
    Run the full SD Fleksjobrefusion Playwright flow for all persons.

    :param username: ADFS username.
    :param password: ADFS password.
    :param persons: Person rows to process.
    :param sd_url: SD landing page URL.
    :param headless: Whether to run browser headless.
    :return: Tuple containing success flag and list of failed rows.
    """
    logger.info("Starting SD Fleksjobrefusion browser flow")

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
                return False, persons

            failures: list[dict[str, str]] = []

            for person in persons:
                processed = process_person_playwright(
                    page=active_page,
                    tjenestenummer=person["tjenestenummer"],
                    institution=person["institution"],
                    beloeb=person["beloeb"],
                    loenart=person["loenart"],
                )

                if not processed:
                    failures.append(person)

            if failures:
                logger.error(f"The following persons failed for {len(failures)} persons:")
                for failed in failures:
                    logger.error(
                        "- %s (%s): %s - %s",
                        failed["tjenestenummer"],
                        failed["institution"],
                        failed["beloeb"],
                        failed["loenart"],
                    )
                return False, failures

            active_page.wait_for_timeout(10000)
            logger.info("SD Fleksjobrefusion browser flow completed")
            return True, []
        finally:
            context.close()
            browser.close()
