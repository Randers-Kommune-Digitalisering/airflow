import logging
import time

from playwright.sync_api import (
    BrowserContext,
    Error as PlaywrightError,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

logger = logging.getLogger(__name__)

PERSONALEWEB_TILE_SELECTOR = (
    "#product-cf662da2-9d3c-0108-e043-0a10f6400108 "
    "div[role='button'][aria-label='Personaleweb']"
)


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

        logger.info(f"{prefix} page[{index}]: title='{title}' url='{url}'")


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
        logger.info("'Log Ind' button clicked.")

        logger.info("Waiting for 'Arbejdsplads-Login-View' button...")
        existing_pages = list(page.context.pages)
        page.locator("#arbejdspladsButton").click(timeout=20000)
        page.wait_for_timeout(1200)
        logger.info("'Arbejdsplads-Login-View' button clicked.")

        # Wait for new page to open after clicking the button
        new_pages = [p for p in page.context.pages if p not in existing_pages]
        active_page = new_pages[-1] if new_pages else page
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


def run_sd_fleksjobrefusion_login(
    username: str,
    password: str,
    sd_url: str = "https://www.silkeborgdata.dk",
) -> bool:
    """
    Run SD Fleksjobrefusion login test flow in headless incognito mode.

    :param username: ADFS username.
    :param password: ADFS password.
    :param sd_url: SD landing page URL.
    :return: True when login flow succeeds, otherwise False.
    """
    logger.info("Starting SD Fleksjobrefusion login flow")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
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
                return False

            active_page.wait_for_timeout(10000)
            logger.info("SD Fleksjobrefusion login flow completed")
            return True
        finally:
            context.close()
            browser.close()
