import re
from dataclasses import dataclass
from collections.abc import Iterable, Mapping
from typing import Any


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
            await page.wait_for_load_state("networkidle")
            continue

        if isinstance(route_step, Mapping):
            if "frame" in route_step:
                frame_config = route_step["frame"]
                scope = _resolve_interaction_scope(page, frame_config)
                if len(route_step) == 1:
                    continue

            if "wait_for" in route_step:
                await _wait_for_in_scope(scope, route_step["wait_for"])
                continue

            if "click" in route_step:
                await scope.locator(route_step["click"]).click()
                await page.wait_for_load_state("networkidle")
                continue

            if "select" in route_step:
                select_config = route_step["select"]
                option_kwargs = {
                    key: value
                    for key, value in {
                        "value": select_config.get("value"),
                        "label": select_config.get("label"),
                        "index": select_config.get("index"),
                    }.items()
                    if value is not None
                }
                await scope.locator(select_config["selector"]).select_option(**option_kwargs)
                await page.wait_for_load_state("networkidle")
                continue

        if isinstance(route_step, Iterable) and not isinstance(route_step, (str, bytes, Mapping)):
            for selector in route_step:
                if not isinstance(selector, str):
                    raise TypeError(f"Unsupported selector in list_route: {selector!r}")
                await scope.locator(selector).click()
                await page.wait_for_load_state("networkidle")
            continue

        raise TypeError(f"Unsupported list_route step: {route_step!r}")

    return RouteResult(scope=scope, frame_config=frame_config)


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
    target_scope = _resolve_wait_scope(page, route_result.scope, list_elements)

    for wait_selector in wait_selectors:
        await _wait_for_in_scope(target_scope, wait_selector, timeout_ms)


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
        extraction_frame_config = list_elements.get("frame")

    if extraction_frame_config is None:
        extraction_frame_config = route_result.frame_config

    if extraction_frame_config is not None:
        content_frame = await _resolve_content_frame(page, extraction_frame_config)
        return await content_frame.content()

    if hasattr(route_result.scope, "content"):
        return await route_result.scope.content()

    return await page.content()


async def _wait_for_in_scope(scope: Any, selector: str, timeout_ms: int | None = None) -> None:
    """
    Wait for a selector in the current interaction scope.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param selector: CSS selector that must become available
    :param timeout_ms: Optional timeout in milliseconds
    :return: None
    """
    if hasattr(scope, "wait_for_selector"):
        if timeout_ms is None:
            await scope.wait_for_selector(selector)
        else:
            await scope.wait_for_selector(selector, timeout=timeout_ms)
        return

    wait_args: dict[str, Any] = {"state": "visible"}
    if timeout_ms is not None:
        wait_args["timeout"] = timeout_ms
    await scope.locator(selector).first.wait_for(**wait_args)


def _get_wait_selectors(list_elements: Mapping[str, Any]) -> list[str]:
    """
    Collect unique field selectors used to detect rendered list content.

    :param list_elements: Field-to-selector mapping for extraction
    :return: Ordered selectors to wait for
    """
    wait_selectors: list[str] = []
    seen: set[str] = set()

    for field_name, css_selector in list_elements.items():
        if field_name in {"frame", "row"}:
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
