import importlib
import logging
import re
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlparse, urljoin

logger = logging.getLogger(__name__)


SCRAPY_PLAYWRIGHT_SETTINGS = {
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "DOWNLOAD_HANDLERS": {
        "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    },
    "PLAYWRIGHT_BROWSER_TYPE": "chromium",
    "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
    "PLAYWRIGHT_CONTEXTS": {
        "default": {
            "java_script_enabled": True,
        }
    },
    "LOG_ENABLED": False,
}


def scrape_sites(site_configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Run the configured sites through a local scrapy-playwright crawler.

    :param site_configs: Site scraping configuration entries
    :return: Scraped site results grouped by site and list
    """
    try:
        scrapy = importlib.import_module("scrapy")
        crawler_module = importlib.import_module("scrapy.crawler")
        importlib.import_module("scrapy_playwright.handler")
    except ImportError as exc:
        raise ModuleNotFoundError(
            "scrapy-playwright and its scrapy/playwright dependencies are required for fritidsjobs_webscraper."
        ) from exc

    scraped_sites: list[dict[str, Any]] = []

    spider_class = _build_config_spider(scrapy.Spider, site_configs, scraped_sites)
    process = crawler_module.CrawlerProcess(settings=SCRAPY_PLAYWRIGHT_SETTINGS)
    crawler = process.create_crawler(spider_class)
    process.crawl(crawler)
    process.start()
    _raise_for_crawl_errors(crawler.stats.get_stats())

    return scraped_sites


def _build_config_spider(spider_base_class: type, site_configs: list[dict[str, Any]], scraped_sites: list[dict[str, Any]]) -> type:
    """
    Create a one-off spider class bound to the provided site configuration.

    :param spider_base_class: Base Scrapy spider class to inherit from
    :param site_configs: Site scraping configuration entries
    :param scraped_sites: Mutable result container updated during scraping
    :return: Configured spider class for the current scrape run
    """
    class ConfigurableFritidsjobsSpider(spider_base_class):
        name = "fritidsjobs_config_spider"

        def start_requests(self) -> Iterable[Any]:
            """Yield one fresh request per configured list.

            :return: Scrapy requests that start from each configured site URL
            """
            request_class = importlib.import_module("scrapy").Request

            for site_config in site_configs:
                for list_config in site_config.get("lists", []):
                    yield request_class(
                        url=site_config["site_url"],
                        callback=self.parse_list,
                        meta={
                            "playwright": True,
                            "playwright_include_page": True,
                            "playwright_page_init_callback": _init_page,
                            "site_config": site_config,
                            "list_config": list_config,
                        },
                        dont_filter=True,
                    )

        async def parse_list(self, response: Any) -> None:
            """
            Apply the configured route steps and store the extracted list items.

            :param response: Scrapy response containing the Playwright page reference
            :return: None
            """
            page = response.meta["playwright_page"]
            site_config = response.meta["site_config"]
            list_config = response.meta["list_config"]

            try:
                scope = await _follow_list_route(page, list_config.get("list_route", []))
                await _wait_for_list_elements(page, scope, list_config)
                html = await _extract_scope_html(page, scope, list_config)
            finally:
                await page.close()

            scraped_items = _extract_list_items(
                html=html,
                base_url=site_config["site_url"],
                list_elements=list_config.get("list_elements", {}),
            )
            logger.info("Extracted %d items for site=%s list=%s", len(scraped_items), site_config.get("site_name"), list_config.get("list_name"))
            _store_list_results(scraped_sites, site_config, list_config, scraped_items)

    return ConfigurableFritidsjobsSpider


async def _follow_list_route(page: Any, list_route: Iterable[Any]) -> Any:
    """
    Execute each configured browser interaction required before scraping.

    :param page: Playwright page used for dynamic navigation
    :param list_route: Ordered browser interaction steps to apply before scraping
    :return: The active Playwright scope (page or frame) used for final extraction
    """
    scope: Any = page

    for route_step in list_route:
        if isinstance(route_step, str):
            await scope.locator(route_step).click()
            await page.wait_for_load_state("networkidle")
            continue

        if isinstance(route_step, Mapping):
            if "frame" in route_step:
                scope = _resolve_frame_scope(page, route_step["frame"])
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

    return scope


async def _wait_for_in_scope(scope: Any, selector: str) -> None:
    """
    Wait for a selector inside the current interaction scope.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param selector: CSS selector that must become available
    :return: None
    """
    if hasattr(scope, "wait_for_selector"):
        await scope.wait_for_selector(selector)
        return

    await scope.locator(selector).first.wait_for(state="visible")


async def _wait_for_list_elements(page: Any, scope: Any, list_config: Mapping[str, Any]) -> None:
    """
    Wait for configured list element selectors before extracting HTML.

    :param page: Playwright page used for dynamic navigation
    :param scope: Active Playwright scope returned from route execution
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
    target_scope = _resolve_wait_scope(page, scope, list_elements)

    for wait_selector in wait_selectors:
        if hasattr(target_scope, "wait_for_selector"):
            await target_scope.wait_for_selector(wait_selector, timeout=timeout_ms)
            continue

        await target_scope.locator(wait_selector).first.wait_for(state="visible", timeout=timeout_ms)


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
        return _resolve_frame_scope(page, frame_config)

    return fallback_scope


def _resolve_frame_scope(page: Any, frame_config: Any) -> Any:
    """
    Resolve a frame locator from a frame selector or frame configuration mapping.

    :param page: Playwright page used for dynamic navigation
    :param frame_config: Frame selector string or mapping with selector/url_contains/name
    :return: Playwright frame locator scope for element interactions
    """
    if isinstance(frame_config, str):
        return page.frame_locator(frame_config)

    if not isinstance(frame_config, Mapping):
        raise TypeError(f"Unsupported frame configuration: {frame_config!r}")

    selector = frame_config.get("selector")
    if isinstance(selector, str) and selector:
        return page.frame_locator(selector)

    url_contains = frame_config.get("url_contains")
    if isinstance(url_contains, str) and url_contains:
        frame = page.frame(url=re.compile(f".*{re.escape(url_contains)}.*"))
        if frame is None:
            raise RuntimeError(f"No iframe matched url_contains={url_contains!r}")
        return frame

    frame_name = frame_config.get("name")
    if isinstance(frame_name, str) and frame_name:
        frame = page.frame(name=frame_name)
        if frame is None:
            raise RuntimeError(f"No iframe matched name={frame_name!r}")
        return frame

    raise ValueError("Frame configuration must include selector, url_contains, or name")


async def _extract_scope_html(page: Any, scope: Any, list_config: Mapping[str, Any]) -> str:
    """
    Extract HTML from either the current scope or an explicitly configured content frame.

    :param page: Playwright page used for dynamic navigation
    :param scope: Active Playwright scope returned from route execution
    :param list_config: List configuration for the current scrape
    :return: Rendered HTML string used by scrapy selectors
    """
    list_elements = list_config.get("list_elements", {})
    extraction_frame_config = None

    if isinstance(list_elements, Mapping):
        extraction_frame_config = list_elements.get("frame")

    if extraction_frame_config is not None:
        explicit_frame = await _resolve_content_frame(page, extraction_frame_config)
        return await explicit_frame.content()

    if hasattr(scope, "content"):
        return await scope.content()

    return await page.content()


async def _resolve_content_frame(page: Any, frame_config: Any) -> Any:
    """
    Resolve a Playwright frame object used to extract iframe HTML content.

    :param page: Playwright page used for dynamic navigation
    :param frame_config: Frame selector string or mapping with selector/url_contains/name
    :return: Playwright frame object
    """
    if isinstance(frame_config, str):
        frame_element = await page.query_selector(frame_config)
        if frame_element is None:
            raise RuntimeError(f"No iframe matched selector={frame_config!r}")

        frame = await frame_element.content_frame()
        if frame is None:
            raise RuntimeError(f"Could not resolve frame content for selector={frame_config!r}")

        return frame

    if not isinstance(frame_config, Mapping):
        raise TypeError(f"Unsupported content_frame configuration: {frame_config!r}")

    selector = frame_config.get("selector")
    if isinstance(selector, str) and selector:
        return await _resolve_content_frame(page, selector)

    url_contains = frame_config.get("url_contains")
    if isinstance(url_contains, str) and url_contains:
        frame = page.frame(url=re.compile(f".*{re.escape(url_contains)}.*"))
        if frame is None:
            raise RuntimeError(f"No iframe matched url_contains={url_contains!r}")
        return frame

    frame_name = frame_config.get("name")
    if isinstance(frame_name, str) and frame_name:
        frame = page.frame(name=frame_name)
        if frame is None:
            raise RuntimeError(f"No iframe matched name={frame_name!r}")
        return frame

    raise ValueError("content_frame must include selector, url_contains, or name")


async def _configure_request_blocking(
    page: Any,
    site_config: Mapping[str, Any],
    list_config: Mapping[str, Any],
) -> None:
    """
    Restrict browser requests to the configured site host and allowed domains.

    :param page: Playwright page used for dynamic navigation
    :param site_config: Site configuration for the current scrape
    :param list_config: List configuration for the current scrape
    :return: None
    """
    if not _should_block_external_requests(site_config, list_config):
        logger.info("External request blocking disabled for this scrape configuration.")
        return

    allowed_domains = _collect_allowed_domains(site_config, list_config)

    async def handle_route(route: Any) -> None:
        request = route.request
        if _is_allowed_request_url(request.url, allowed_domains):
            await route.continue_()
            return

        logger.debug("Blocking external request: %s", request.url)
        await route.abort()

    await page.route("**/*", handle_route)


def _should_block_external_requests(
    site_config: Mapping[str, Any],
    list_config: Mapping[str, Any],
) -> bool:
    """
    Determine whether external request blocking should be active.

    :param site_config: Site configuration for the current scrape
    :param list_config: List configuration for the current scrape
    :return: True when external requests should be blocked
    """
    list_override = list_config.get("block_external_requests")
    if isinstance(list_override, bool):
        return list_override

    site_default = site_config.get("block_external_requests")
    if isinstance(site_default, bool):
        return site_default

    return True


async def _init_page(page: Any, request: Any) -> None:
    """
    Initialize a newly created page before its first navigation.

    :param page: Newly created Playwright page
    :param request: Scrapy request carrying site and list configuration
    :return: None
    """
    await _configure_request_blocking(
        page,
        request.meta["site_config"],
        request.meta["list_config"],
    )


def _collect_allowed_domains(
    site_config: Mapping[str, Any],
    list_config: Mapping[str, Any],
) -> set[str]:
    """
    Collect the allowed domains for one scrape from site and list configuration.

    :param site_config: Site configuration for the current scrape
    :param list_config: List configuration for the current scrape
    :return: Normalized set of allowed hostnames
    """
    site_hostname = urlparse(site_config["site_url"]).hostname
    configured_domains = [
        *site_config.get("allowed_domains", []),
        *list_config.get("allowed_domains", []),
    ]

    allowed_domains = {
        _normalize_allowed_domain(domain)
        for domain in configured_domains
        if isinstance(domain, str) and domain
    }
    if site_hostname:
        allowed_domains.add(site_hostname.lower())

    return allowed_domains


def _normalize_allowed_domain(domain: str) -> str:
    """
    Normalize allowed domain entries from hostnames, wildcards, or full URLs.

    :param domain: Raw domain entry from configuration
    :return: Normalized hostname or wildcard domain suffix
    """
    normalized = domain.strip().lower()

    if normalized.startswith("*."):
        return normalized

    parsed = urlparse(normalized)
    if parsed.scheme:
        return (parsed.hostname or normalized).lower()

    host = normalized.split("/", 1)[0]
    host = host.split(":", 1)[0]
    return host


def _is_allowed_request_url(request_url: str, allowed_domains: set[str]) -> bool:
    """
    Check whether a request URL matches the configured allowed domains.

    :param request_url: Request URL emitted by Playwright
    :param allowed_domains: Normalized set of allowed hostnames
    :return: True when the request should be allowed, otherwise False
    """
    parsed_url = urlparse(request_url)
    hostname = parsed_url.hostname

    if parsed_url.scheme in {"about", "data"}:
        return True
    if not hostname:
        return True

    normalized_hostname = hostname.lower()

    for allowed_domain in allowed_domains:
        if allowed_domain.startswith("*."):
            wildcard_domain = allowed_domain[2:]
            if normalized_hostname == wildcard_domain or normalized_hostname.endswith(f".{wildcard_domain}"):
                return True

    return any(
        normalized_hostname == allowed_domain or normalized_hostname.endswith(f".{allowed_domain}")
        for allowed_domain in allowed_domains
    )


def _raise_for_crawl_errors(stats: Mapping[str, Any]) -> None:
    """
    Raise an error when the crawl logged downloader or spider exceptions.

    :param stats: Scrapy crawler stats collected after the crawl finished
    :return: None
    """
    download_exception_count = stats.get("downloader/exception_count", 0)
    spider_exception_count = stats.get("spider_exceptions/count", 0)

    if download_exception_count or spider_exception_count:
        raise RuntimeError(
            "fritidsjobs_webscraper failed during crawl: "
            f"{download_exception_count} downloader exceptions, "
            f"{spider_exception_count} spider exceptions."
        )


def _store_list_results(
    scraped_sites: list[dict[str, Any]],
    site_config: Mapping[str, Any],
    list_config: Mapping[str, Any],
    scraped_items: list[dict[str, str]],
) -> None:
    """
    Append scraped list results under the matching site result entry.

    :param scraped_sites: Mutable result container updated during scraping
    :param site_config: Site configuration for the current list
    :param list_config: List configuration for the current list scrape
    :param scraped_items: Extracted items for the current list
    :return: None
    """
    site_result = next(
        (site for site in scraped_sites if site["site_name"] == site_config["site_name"]),
        None,
    )
    if site_result is None:
        site_result = {
            "site_name": site_config["site_name"],
            "site_url": site_config["site_url"],
            "lists": [],
        }
        scraped_sites.append(site_result)

    site_result["lists"].append(
        {
            "list_name": list_config["list_name"],
            "items": scraped_items,
        }
    )


def _extract_list_items(
    html: str,
    base_url: str,
    list_elements: Mapping[str, str],
) -> list[dict[str, str]]:
    """
    Extract configured fields from the rendered HTML into item dictionaries.

    :param html: Rendered page HTML to parse
    :param base_url: Base URL used to normalize relative links
    :param list_elements: Field-to-selector mapping for extraction
    :return: Extracted items for the current list
    """
    selector_class = importlib.import_module("scrapy").Selector
    selector = selector_class(text=html)
    field_selectors = {
        field_name: css_selector
        for field_name, css_selector in list_elements.items()
        if field_name not in {"row", "frame"}
    }
    if not field_selectors:
        return []

    row_selector = list_elements.get("row")
    if row_selector:
        row_nodes = selector.css(row_selector)
        return [
            _build_item_from_node(row_node, selector, base_url, field_selectors, index)
            for index, row_node in enumerate(row_nodes)
        ]

    primary_field_name, primary_selector = next(iter(field_selectors.items()))
    primary_nodes = selector.css(primary_selector)
    return [
        _build_item_from_node(node, selector, base_url, field_selectors, index, primary_field_name)
        for index, node in enumerate(primary_nodes)
    ]


def _build_item_from_node(
    node: Any,
    root_selector: Any,
    base_url: str,
    field_selectors: Mapping[str, str],
    index: int,
    primary_field_name: str | None = None,
) -> dict[str, str]:
    """
    Build one scraped item from a selector node and its configured fields.

    :param node: Selector node representing the current item or primary field
    :param root_selector: Root selector for page-level fallback lookups
    :param base_url: Base URL used to normalize relative links
    :param field_selectors: Field-to-selector mapping for extraction
    :param index: Current item index used for fallback alignment
    :param primary_field_name: Field name used to anchor item extraction
    :return: Extracted item values for the current node
    """
    item: dict[str, str] = {}

    for field_name, css_selector in field_selectors.items():
        field_value = _extract_field_value(
            node=node,
            root_selector=root_selector,
            field_name=field_name,
            css_selector=css_selector,
            base_url=base_url,
            index=index,
            prefer_node_text=field_name == primary_field_name,
        )
        if field_value:
            item[field_name] = field_value

    return item


def _extract_field_value(
    node: Any,
    root_selector: Any,
    field_name: str,
    css_selector: str,
    base_url: str,
    index: int,
    prefer_node_text: bool,
) -> str | None:
    """
    Extract one field value from a selector node.

    :param node: Selector node representing the current item or primary field
    :param root_selector: Root selector for page-level fallback lookups
    :param field_name: Field name being extracted
    :param css_selector: CSS selector configured for the field
    :param base_url: Base URL used to normalize relative links
    :param index: Current item index used for fallback alignment
    :param prefer_node_text: Whether to prefer direct node text extraction
    :return: Extracted field value or None when not found
    """
    is_link_field = field_name.lower().endswith("link")
    if is_link_field:
        value = node.css(f"{css_selector}::attr(href)").get()
        if not value:
            value = node.xpath("ancestor-or-self::a[1]/@href").get()
        if not value:
            href_values = root_selector.css(f"{css_selector}::attr(href)").getall()
            value = href_values[index] if index < len(href_values) else None
        return urljoin(base_url, value.strip()) if value else None

    if prefer_node_text:
        value = node.css("::text").get()
    else:
        value = node.css(f"{css_selector}::text").get()

    if not value:
        text_values = root_selector.css(f"{css_selector}::text").getall()
        value = text_values[index] if index < len(text_values) else None

    return value.strip() if value else None
