from dag_fritidsjobs_webscraper import scapy_client


def test_should_capture_click_links_only_when_link_key_missing() -> None:
    assert scapy_client._should_capture_click_links({"row": "div.row", "title": "h3"}) is True
    assert scapy_client._should_capture_click_links({"row": "div.row", "title": "h3", "link": "a"}) is False
    assert scapy_client._should_capture_click_links(None) is False


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

    scapy_client._attach_captured_links(scraped_items, captured_links)

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

    scapy_client._attach_captured_links(scraped_items, captured_links)

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

    scapy_client._attach_captured_links(scraped_items, captured_links)

    assert scraped_items == [
        {
            "title": "Job A",
            "url": "https://legacy.example.com/a",
            "link": "https://example.com/a",
        }
    ]
