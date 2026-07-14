import importlib
import logging
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlparse

from dag_fritidsjobs_webscraper.item_extraction import extract_list_items
from dag_fritidsjobs_webscraper.playwright_navigation import extract_scope_html, follow_list_route, wait_for_list_elements

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
                route_result = await follow_list_route(page, list_config.get("list_route", []))
                await wait_for_list_elements(page, route_result, list_config)
                html = await extract_scope_html(page, route_result, list_config)
            finally:
                await page.close()

            scraped_items = extract_list_items(
                html=html,
                base_url=site_config["site_url"],
                list_elements=list_config.get("list_elements", {}),
            )
            logger.info(
                "Extracted %d items for site=%s list=%s",
                len(scraped_items),
                site_config.get("site_name"),
                list_config.get("list_name"),
            )
            _store_list_results(scraped_sites, site_config, list_config, scraped_items)

    return ConfigurableFritidsjobsSpider


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
