from dag_fritidsjobs_webscraper import scrapy_client
import pytest


def test_should_capture_click_links_only_when_link_key_missing() -> None:
    assert scrapy_client._should_capture_click_links({"row": "div.row", "title": "h3"}) is True
    assert scrapy_client._should_capture_click_links({"row": "div.row", "title": "h3", "link": "a"}) is False
    assert scrapy_client._should_capture_click_links(None) is False


def test_attach_captured_links_matches_by_title_before_fallback() -> None:
    scraped_items = [
        {"title": "Job B"},
        {"title": "Job A"},
        {"title": "Job C"},
    ]
    captured_links = [
        {"title": "Job A", "link": "https://example.com/a"},
        {"title": "Job B", "link": "https://example.com/b"},
        {"title": "Job X", "link": "https://example.com/x"},
    ]

    scrapy_client._attach_captured_links(scraped_items, captured_links)

    assert scraped_items == [
        {"title": "Job B", "link": "https://example.com/b"},
        {"title": "Job A", "link": "https://example.com/a"},
        {"title": "Job C", "link": "https://example.com/x"},
    ]


def test_attach_captured_links_keeps_existing_links_untouched() -> None:
    scraped_items = [
        {"title": "Job A", "link": "https://existing.example.com/a"},
        {"title": "Job B"},
    ]
    captured_links = [
        {"title": "Job A", "link": "https://example.com/a"},
        {"title": "Job B", "link": "https://example.com/b"},
    ]

    scrapy_client._attach_captured_links(scraped_items, captured_links)

    assert scraped_items == [
        {"title": "Job A", "link": "https://existing.example.com/a"},
        {"title": "Job B", "link": "https://example.com/b"},
    ]


def test_attach_captured_links_treats_url_key_as_missing_link() -> None:
    scraped_items = [
        {"title": "Job A", "url": "https://legacy.example.com/a"},
    ]
    captured_links = [
        {"title": "Job A", "link": "https://example.com/a"},
    ]

    scrapy_client._attach_captured_links(scraped_items, captured_links)

    assert scraped_items == [
        {
            "title": "Job A",
            "url": "https://legacy.example.com/a",
            "link": "https://example.com/a",
        }
    ]


def test_raise_for_crawl_errors_allows_small_playwright_exception_ratio() -> None:
    stats = {
        "downloader/exception_count": 1,
        "spider_exceptions/count": 0,
        "downloader/request_count": 20,
        "downloader/response_count": 19,
        "downloader/exception_type_count/playwright._impl._errors.Error": 1,
    }

    scrapy_client._raise_for_crawl_errors(stats)


def test_raise_for_crawl_errors_raises_when_playwright_exception_ratio_is_high() -> None:
    stats = {
        "downloader/exception_count": 3,
        "spider_exceptions/count": 0,
        "downloader/request_count": 10,
        "downloader/response_count": 7,
        "downloader/exception_type_count/playwright._impl._errors.Error": 3,
    }

    with pytest.raises(RuntimeError, match="failed during crawl"):
        scrapy_client._raise_for_crawl_errors(stats)


def test_raise_for_crawl_errors_raises_on_non_playwright_downloader_exception() -> None:
    stats = {
        "downloader/exception_count": 1,
        "spider_exceptions/count": 0,
        "downloader/request_count": 10,
        "downloader/response_count": 9,
        "downloader/exception_type_count/twisted.internet.error.TimeoutError": 1,
    }

    with pytest.raises(RuntimeError, match="failed during crawl"):
        scrapy_client._raise_for_crawl_errors(stats)
