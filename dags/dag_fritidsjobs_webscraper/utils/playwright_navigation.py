import asyncio
import re
import time
from dataclasses import dataclass
from collections.abc import Iterable, Mapping
from typing import Any

from .playwright_diagnostics import (  # pyright: ignore[reportMissingImports]
    is_timeout_error,
    log_list_wait_timeout_diagnostics,
    log_wait_timeout_diagnostics,
)


@dataclass
class RouteResult:
    """
    Result of applying a configured list route.

    :param scope: Active Playwright interaction scope (Page, Frame, or FrameLocator)
    :param frame_config: Last frame configuration selected during route execution
    """
    scope: Any
    frame_config: Any | None = None


async def follow_list_route(page: Any, list_route: Iterable[Any]) -> RouteResult:
    """
    Execute configured browser interactions before scraping.

    :param page: Playwright page used for dynamic navigation
    :param list_route: Ordered browser interaction steps to apply before scraping
    :return: Route execution result with final scope and selected frame configuration
    """
    scope: Any = page
    frame_config: Any | None = None

    for route_step in list_route:
        if isinstance(route_step, str):
            await scope.locator(route_step).click()
            await _wait_for_route_step_settle(page)
            continue

        if isinstance(route_step, Mapping):
            handled = False

            if "frame" in route_step:
                frame_config = route_step["frame"]
                scope = _resolve_interaction_scope(page, frame_config)
                handled = True

            if "wait_for" in route_step:
                await _wait_for_in_scope(scope, route_step["wait_for"])
                handled = True

            if "click" in route_step:
                await scope.locator(route_step["click"]).click()
                await _wait_for_route_step_settle(page)
                handled = True

            if "select" in route_step:
                select_config = route_step["select"]
                await _select_option_in_scope(scope, select_config)
                await _wait_for_route_step_settle(page)
                handled = True

            if handled:
                continue

        if isinstance(route_step, Iterable) and not isinstance(route_step, (str, bytes, Mapping)):
            for selector in route_step:
                if not isinstance(selector, str):
                    raise TypeError(f"Unsupported selector in list_route: {selector!r}")
                await scope.locator(selector).click()
                await _wait_for_route_step_settle(page)
            continue

        raise TypeError(f"Unsupported list_route step: {route_step!r}")

    return RouteResult(scope=scope, frame_config=frame_config)


async def _wait_for_route_step_settle(page: Any, timeout_ms: int = 30000) -> None:
    """
    Wait for the page to settle after a route interaction.

    Some pages keep long-lived requests open (analytics, trackers, websockets),
    which can prevent `networkidle` from ever being reached. In that case,
    fall back to DOM readiness so route execution can continue.

    :param page: Playwright page used for dynamic navigation
    :param timeout_ms: Timeout in milliseconds for waiting on networkidle
    :return: None
    """
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)
        return
    except Exception as exc:
        if exc.__class__.__name__ != "TimeoutError":
            raise

    await page.wait_for_load_state("domcontentloaded")


async def wait_for_list_elements(page: Any, route_result: RouteResult, list_config: Mapping[str, Any]) -> None:
    """
    Wait for configured list element selectors before extracting HTML.

    :param page: Playwright page used for dynamic navigation
    :param route_result: Result returned from route execution
    :param list_config: List configuration for the current scrape
    :return: None
    """
    list_elements = list_config.get("list_elements", {})
    if not isinstance(list_elements, Mapping):
        return

    wait_selectors = _get_wait_selectors(list_elements)
    if not wait_selectors:
        return

    timeout_ms = list_config.get("wait_for_list_elements_timeout_ms", 30000)
    wait_state = str(list_config.get("wait_for_list_elements_state", "attached")).strip().lower()
    if wait_state not in {"attached", "detached", "visible", "hidden"}:
        wait_state = "attached"

    await _wait_for_frame_scope_selector(page, list_elements, timeout_ms)
    target_scope = _resolve_wait_scope(page, route_result.scope, list_elements)

    for wait_selector in wait_selectors:
        try:
            await _wait_for_in_scope(target_scope, wait_selector, timeout_ms, wait_state)
        except Exception as exc:
            if is_timeout_error(exc):
                await log_list_wait_timeout_diagnostics(
                    target_scope,
                    wait_selector,
                    list_elements,
                    timeout_ms,
                    wait_state,
                )
            raise

    if list_config.get("wait_for_list_update_after_route", True) is not False:
        update_selector = _resolve_list_update_selector(list_config, list_elements, wait_selectors)
        if update_selector is not None:
            stability_ms = _coerce_positive_int(list_config.get("wait_for_list_update_stability_ms"), 800)
            poll_ms = _coerce_positive_int(list_config.get("wait_for_list_update_poll_ms"), 150)
            await _wait_for_selector_stability(
                target_scope,
                update_selector,
                timeout_ms,
                stability_ms=stability_ms,
                poll_ms=poll_ms,
            )


async def extract_scope_html(page: Any, route_result: RouteResult, list_config: Mapping[str, Any]) -> str:
    """
    Extract HTML from the selected content scope.

    :param page: Playwright page used for dynamic navigation
    :param route_result: Result returned from route execution
    :param list_config: List configuration for the current scrape
    :return: Rendered HTML string used by scrapy selectors
    """
    list_elements = list_config.get("list_elements", {})
    extraction_frame_config = None

    if isinstance(list_elements, Mapping):
        visible_rows_only = list_config.get("extract_visible_rows_only", True) is not False
        row_selector = list_elements.get("row")
        if visible_rows_only and isinstance(row_selector, str):
            normalized_row_selector = row_selector.strip()
            if normalized_row_selector:
                target_scope = _resolve_wait_scope(page, route_result.scope, list_elements)
                try:
                    await _annotate_visible_rows_in_scope(target_scope, normalized_row_selector)
                except Exception:
                    pass

        extraction_frame_config = list_elements.get("frame")

    if extraction_frame_config is None:
        extraction_frame_config = route_result.frame_config

    if extraction_frame_config is not None:
        content_frame = await _resolve_content_frame(page, extraction_frame_config)
        return await content_frame.content()

    if hasattr(route_result.scope, "content"):
        return await route_result.scope.content()

    return await page.content()


async def capture_row_links_via_click(
    page: Any,
    route_result: RouteResult,
    list_config: Mapping[str, Any],
) -> list[dict[str, str]]:
    """
    Capture row target URLs by clicking rows when no direct link selector is configured.

    Each row is clicked in the same tab, the destination URL is captured,
    then browser history returns to the list before continuing.

    :param page: Playwright page used for dynamic navigation
    :param route_result: Result returned from route execution
    :param list_config: List configuration for the current scrape
    :return: Captured title/link mappings in row order
    """
    list_elements = list_config.get("list_elements", {})
    if not isinstance(list_elements, Mapping):
        return []

    row_selector = list_elements.get("row")
    if not isinstance(row_selector, str) or not row_selector.strip():
        return []
    row_selector = row_selector.strip()

    title_selector = list_elements.get("title")
    if not isinstance(title_selector, str) or not title_selector.strip():
        title_selector = None
    else:
        title_selector = title_selector.strip()

    click_scope = _resolve_click_capture_scope(page, route_result, list_elements)
    row_count = await click_scope.locator(row_selector).count()
    if row_count <= 0:
        return []

    captured_links: list[dict[str, str]] = []
    for row_index in range(row_count):
        click_scope = _resolve_click_capture_scope(page, route_result, list_elements)
        row_locator = click_scope.locator(row_selector).nth(row_index)

        row_title = await _read_row_title_for_capture(row_locator, title_selector)
        captured_url = await _capture_url_for_row_click(
            page,
            row_locator,
            title_selector,
        )

        if not captured_url:
            continue

        await wait_for_list_elements(page, route_result, list_config)

        captured_item: dict[str, str] = {"link": captured_url}
        if row_title:
            captured_item["title"] = row_title
        captured_links.append(captured_item)

    return captured_links


async def _annotate_visible_rows_in_scope(scope: Any, row_selector: str) -> None:
    """
    Annotate matched rows with visibility markers in the live DOM.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param row_selector: CSS selector used to identify list rows
    :return: None
    """
    try:
        await scope.locator(row_selector).evaluate_all(
            """(elements) => elements
                .forEach((element) => {
                    const style = window.getComputedStyle(element);
                    const isVisibleByStyle = !(
                        style.display === "none" ||
                        style.visibility === "hidden" ||
                        style.visibility === "collapse"
                    );
                    const isHiddenAttribute = !!element.hidden;
                    const isAriaHidden = (element.getAttribute("aria-hidden") || "").toLowerCase() === "true";

                    const rect = element.getBoundingClientRect();
                    const hasSize = rect.width > 0 && rect.height > 0;
                    const isVisible = isVisibleByStyle && !isHiddenAttribute && !isAriaHidden && hasSize;

                    element.setAttribute("data-rk-visible-row", "1");
                    element.setAttribute("data-rk-visible-row-state", isVisible ? "visible" : "hidden");
                })
            """
        )
    except Exception:
        return


def _resolve_click_capture_scope(page: Any, route_result: RouteResult, list_elements: Mapping[str, Any]) -> Any:
    """
    Resolve click scope for row URL capture.

    :param page: Playwright page used for dynamic navigation
    :param route_result: Result returned from route execution
    :param list_elements: Field-to-selector mapping for extraction
    :return: Playwright scope used for row clicking
    """
    frame_config = list_elements.get("frame")
    if frame_config is None:
        frame_config = route_result.frame_config

    if frame_config is None:
        return page

    return _resolve_interaction_scope(page, frame_config)


async def _read_row_title_for_capture(row_locator: Any, title_selector: str | None) -> str | None:
    """
    Read row title text for matching captured links back to scraped items.

    :param row_locator: Locator scoped to a single list row
    :param title_selector: Optional configured title selector
    :return: Normalized row title text, or None when unavailable
    """
    if title_selector:
        try:
            title_text = await row_locator.locator(title_selector).first.text_content()
            normalized_title = _normalize_capture_text(title_text)
            if normalized_title:
                return normalized_title
        except Exception:
            pass

    try:
        return _normalize_capture_text(await row_locator.text_content())
    except Exception:
        return None


async def _capture_url_for_row_click(
    page: Any,
    row_locator: Any,
    title_selector: str | None,
) -> str | None:
    """
    Capture one row URL by same-tab click and history back navigation.

    :param page: Playwright page used for dynamic navigation
    :param row_locator: Locator scoped to a single list row
    :param title_selector: Optional configured title selector
    :return: Captured URL, or None when no navigation occurred
    """
    before_url = page.url

    clicked = False
    if title_selector:
        try:
            await row_locator.locator(title_selector).first.click()
            clicked = True
        except Exception:
            clicked = False

    if not clicked:
        try:
            await row_locator.click()
            clicked = True
        except Exception:
            return None

    await _wait_for_route_step_settle(page)
    after_url = page.url
    if not after_url or after_url == before_url:
        return None

    try:
        await page.go_back(wait_until="domcontentloaded")
        await _wait_for_route_step_settle(page)
    except Exception:
        return after_url

    return after_url


def _normalize_capture_text(value: Any) -> str | None:
    """
    Normalize extracted text for stable matching.

    :param value: Raw text value
    :return: Normalized text or None
    """
    if not isinstance(value, str):
        return None

    normalized_value = " ".join(value.split()).strip()
    return normalized_value or None


async def _wait_for_in_scope(
    scope: Any,
    selector: str,
    timeout_ms: int | None = None,
    wait_state: str = "attached",
) -> None:
    """
    Wait for a selector in the current interaction scope.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param selector: CSS selector that must become available
    :param timeout_ms: Optional timeout in milliseconds
    :param wait_state: Playwright wait state (attached/detached/visible/hidden)
    :return: None
    """
    try:
        if hasattr(scope, "wait_for_selector"):
            wait_kwargs: dict[str, Any] = {"state": wait_state}
            if timeout_ms is not None:
                wait_kwargs["timeout"] = timeout_ms
            await scope.wait_for_selector(selector, **wait_kwargs)
            return

        wait_args: dict[str, Any] = {"state": wait_state}
        if timeout_ms is not None:
            wait_args["timeout"] = timeout_ms
        await scope.locator(selector).first.wait_for(**wait_args)
    except Exception as exc:
        if is_timeout_error(exc):
            await log_wait_timeout_diagnostics(scope, selector, timeout_ms, wait_state)
        raise


async def _wait_for_selector_stability(
    scope: Any,
    selector: str,
    timeout_ms: int,
    stability_ms: int = 800,
    poll_ms: int = 150,
) -> None:
    """
    Wait for selector content to stop changing for a short stability window.

    This helps pages where list elements already exist, but filtering updates
    happen asynchronously after select interactions.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param selector: CSS selector representing list rows or list container
    :param timeout_ms: Maximum time to wait for stabilization
    :param stability_ms: Required stable duration in milliseconds
    :param poll_ms: Polling interval in milliseconds
    :return: None
    """
    timeout_seconds = max(timeout_ms, 1) / 1000
    deadline = time.monotonic() + timeout_seconds
    stable_seconds = max(stability_ms, 1) / 1000
    sleep_seconds = max(poll_ms, 25) / 1000

    last_snapshot = await _snapshot_selector_state(scope, selector)
    last_change = time.monotonic()

    while True:
        now = time.monotonic()
        if now - last_change >= stable_seconds:
            return

        if now >= deadline:
            return

        await asyncio.sleep(sleep_seconds)
        current_snapshot = await _snapshot_selector_state(scope, selector)
        if current_snapshot != last_snapshot:
            last_snapshot = current_snapshot
            last_change = time.monotonic()


async def _snapshot_selector_state(scope: Any, selector: str) -> str:
    """
    Build a compact selector snapshot for stability comparisons.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param selector: CSS selector used for snapshot sampling
    :return: JSON string containing count and normalized text sample
    """
    return await scope.locator(selector).evaluate_all(
        r"""(elements) => JSON.stringify({
            count: elements.length,
            sample: elements.slice(0, 8).map((element) =>
                (element.textContent || "").replace(/\s+/g, " ").trim().slice(0, 120)
            )
        })"""
    )


def _resolve_list_update_selector(
    list_config: Mapping[str, Any],
    list_elements: Mapping[str, Any],
    wait_selectors: Iterable[str],
) -> str | None:
    """
    Resolve selector used to detect list updates after route interactions.

    :param list_config: List configuration for the current scrape
    :param list_elements: Field-to-selector mapping for extraction
    :param wait_selectors: Selectors already used for initial readiness checks
    :return: Selector string for update tracking, or None
    """
    configured_selector = list_config.get("wait_for_list_update_selector")
    if isinstance(configured_selector, str):
        normalized_configured = configured_selector.strip()
        if normalized_configured:
            return normalized_configured

    row_selector = list_elements.get("row")
    if isinstance(row_selector, str):
        normalized_row_selector = row_selector.strip()
        if normalized_row_selector:
            return normalized_row_selector

    for wait_selector in wait_selectors:
        normalized_wait_selector = wait_selector.strip()
        if normalized_wait_selector:
            return normalized_wait_selector

    return None


def _coerce_positive_int(value: Any, default: int) -> int:
    """
    Coerce config values to positive integers with fallback.

    :param value: Raw configured value
    :param default: Fallback integer when coercion fails
    :return: Positive integer
    """
    if isinstance(value, int) and value > 0:
        return value

    if isinstance(value, float) and value > 0:
        return int(value)

    if isinstance(value, str):
        stripped_value = value.strip()
        if stripped_value.isdigit():
            parsed_value = int(stripped_value)
            if parsed_value > 0:
                return parsed_value

    return default


def _get_wait_selectors(list_elements: Mapping[str, Any]) -> list[str]:
    """
    Collect unique field selectors used to detect rendered list content.

    :param list_elements: Field-to-selector mapping for extraction
    :return: Ordered selectors to wait for
    """
    wait_selectors: list[str] = []
    seen: set[str] = set()
    metadata_fields = {"frame"}

    for field_name, css_selector in list_elements.items():
        if field_name in metadata_fields:
            continue
        if not isinstance(css_selector, str):
            continue

        normalized_selector = css_selector.strip()
        if not normalized_selector or normalized_selector in seen:
            continue

        wait_selectors.append(normalized_selector)
        seen.add(normalized_selector)

    row_selector = list_elements.get("row")
    if isinstance(row_selector, str):
        normalized_row_selector = row_selector.strip()
        if normalized_row_selector and normalized_row_selector not in seen:
            wait_selectors.append(normalized_row_selector)

    return wait_selectors


def _resolve_wait_scope(page: Any, fallback_scope: Any, list_elements: Mapping[str, Any]) -> Any:
    """
    Resolve the proper scope for waiting on extracted list elements.

    :param page: Playwright page used for dynamic navigation
    :param fallback_scope: Active Playwright scope returned from route execution
    :param list_elements: Field-to-selector mapping for extraction
    :return: Playwright scope used for waiting
    """
    frame_config = list_elements.get("frame")
    if frame_config is not None:
        return _resolve_interaction_scope(page, frame_config)

    return fallback_scope


async def _wait_for_frame_scope_selector(
    page: Any,
    list_elements: Mapping[str, Any],
    timeout_ms: int | None,
) -> None:
    """
    Wait for iframe scope selector before resolving frame-located waits.

    Frame selectors define interaction scope, not row/content fields, so they are
    handled separately from field wait selectors.

    :param page: Playwright page used for dynamic navigation
    :param list_elements: Field-to-selector mapping for extraction
    :param timeout_ms: Optional timeout in milliseconds
    :return: None
    """
    frame_config = list_elements.get("frame")
    if frame_config is None:
        return

    mode, value = _parse_frame_config(frame_config)
    if mode != "selector":
        return

    wait_kwargs: dict[str, Any] = {"state": "attached"}
    if timeout_ms is not None:
        wait_kwargs["timeout"] = timeout_ms
    await page.locator(value).first.wait_for(**wait_kwargs)


async def _select_option_in_scope(scope: Any, select_config: Mapping[str, Any]) -> None:
    """
    Select an option in the current scope with tolerant label matching fallback.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param select_config: Select step configuration from list_route
    :return: None
    """
    selector = str(select_config["selector"])
    locator = scope.locator(selector)
    option_kwargs = {
        key: value
        for key, value in {
            "value": select_config.get("value"),
            "label": select_config.get("label"),
            "index": select_config.get("index"),
        }.items()
        if value is not None
    }

    requested_label = select_config.get("label")
    has_only_label = set(option_kwargs.keys()) == {"label"}
    if isinstance(requested_label, str) and requested_label and has_only_label:
        current_options = await _read_select_options(locator)
        current_match_value = _match_option_value_for_label(current_options, requested_label)
        if current_match_value is not None:
            await locator.select_option(value=current_match_value)
            return

    try:
        await locator.select_option(**option_kwargs)
        return
    except Exception:
        if not (isinstance(requested_label, str) and requested_label and has_only_label):
            raise

        options = await _read_select_options(locator)
        matched_value = _match_option_value_for_label(options, requested_label)
        if matched_value is None:
            raise
        await locator.select_option(value=matched_value)


async def _read_select_options(locator: Any) -> list[dict[str, str]]:
    """
    Read option labels and values from a select element.

    :param locator: Playwright locator for a single select element
    :return: List of option mappings with value and label
    """
    options = await locator.evaluate(
        """(el) => Array.from(el.options || []).map((option) => ({
            value: option.value,
            label: option.label || option.textContent || ""
        }))"""
    )
    if not isinstance(options, list):
        return []

    normalized_options: list[dict[str, str]] = []
    for option in options:
        if not isinstance(option, Mapping):
            continue

        option_value = option.get("value")
        option_label = option.get("label")
        if not isinstance(option_value, str) or not isinstance(option_label, str):
            continue

        normalized_options.append({"value": option_value, "label": option_label})

    return normalized_options


def _match_option_value_for_label(options: Iterable[Any], requested_label: str) -> str | None:
    """
    Match an option value against a requested label using normalized comparisons.

    :param options: Iterable of option dicts containing value and label keys
    :param requested_label: Desired option label from configuration
    :return: Matched option value, or None when no label match is found
    """
    target_label = _normalize_label(requested_label)
    if not target_label:
        return None

    normalized_options: list[tuple[str, str]] = []
    for option in options:
        if not isinstance(option, Mapping):
            continue

        option_value = option.get("value")
        option_label = option.get("label")
        if not isinstance(option_value, str) or not isinstance(option_label, str):
            continue

        normalized_label = _normalize_label(option_label)
        if not normalized_label:
            continue

        normalized_options.append((option_value, normalized_label))
        if normalized_label == target_label:
            return option_value

    for option_value, normalized_label in normalized_options:
        if target_label in normalized_label:
            return option_value

        # Avoid weak reverse matches like single-character labels.
        if len(normalized_label) >= 3 and normalized_label in target_label:
            return option_value

    return None


def _normalize_label(value: str) -> str:
    """
    Normalize labels for resilient string matching.

    :param value: Raw label value
    :return: Lowercased label with collapsed whitespace
    """
    return " ".join(value.split()).casefold()


def _resolve_interaction_scope(page: Any, frame_config: Any) -> Any:
    """
    Resolve a frame scope for click/wait/select interactions.

    :param page: Playwright page used for dynamic navigation
    :param frame_config: Frame selector string or mapping with selector/url_contains/name
    :return: Playwright scope for interactions
    """
    mode, value = _parse_frame_config(frame_config)

    if mode == "selector":
        return page.frame_locator(value)

    if mode == "url_contains":
        frame = page.frame(url=re.compile(f".*{re.escape(value)}.*"))
        if frame is None:
            raise RuntimeError(f"No iframe matched url_contains={value!r}")
        return frame

    frame = page.frame(name=value)
    if frame is None:
        raise RuntimeError(f"No iframe matched name={value!r}")
    return frame


async def _resolve_content_frame(page: Any, frame_config: Any) -> Any:
    """
    Resolve a Playwright frame object used to extract iframe HTML content.

    :param page: Playwright page used for dynamic navigation
    :param frame_config: Frame selector string or mapping with selector/url_contains/name
    :return: Playwright frame object
    """
    mode, value = _parse_frame_config(frame_config)

    if mode == "selector":
        frame_element = await page.query_selector(value)
        if frame_element is None:
            raise RuntimeError(f"No iframe matched selector={value!r}")

        frame = await frame_element.content_frame()
        if frame is None:
            raise RuntimeError(f"Could not resolve frame content for selector={value!r}")
        return frame

    if mode == "url_contains":
        frame = page.frame(url=re.compile(f".*{re.escape(value)}.*"))
        if frame is None:
            raise RuntimeError(f"No iframe matched url_contains={value!r}")
        return frame

    frame = page.frame(name=value)
    if frame is None:
        raise RuntimeError(f"No iframe matched name={value!r}")
    return frame


def _parse_frame_config(frame_config: Any) -> tuple[str, str]:
    """
    Parse frame config into a normalized lookup strategy.

    :param frame_config: Raw frame configuration entry
    :return: Tuple containing lookup mode and value
    """
    if isinstance(frame_config, str):
        if not frame_config:
            raise ValueError("Frame selector must be a non-empty string")
        return "selector", frame_config

    if not isinstance(frame_config, Mapping):
        raise TypeError(f"Unsupported frame configuration: {frame_config!r}")

    selector = frame_config.get("selector")
    if isinstance(selector, str) and selector:
        return "selector", selector

    url_contains = frame_config.get("url_contains")
    if isinstance(url_contains, str) and url_contains:
        return "url_contains", url_contains

    frame_name = frame_config.get("name")
    if isinstance(frame_name, str) and frame_name:
        return "name", frame_name

    raise ValueError("Frame configuration must include selector, url_contains, or name")
