import types

import pytest

from dag_fritidsjobs_webscraper.utils import item_extraction


def _patch_scrapy_selector_import(monkeypatch: pytest.MonkeyPatch) -> None:
    parsel = pytest.importorskip("parsel")
    selector_class = parsel.Selector

    real_import_module = item_extraction.importlib.import_module

    def _import_module(name: str):
        if name == "scrapy":
            return types.SimpleNamespace(Selector=selector_class)
        return real_import_module(name)

    monkeypatch.setattr(item_extraction.importlib, "import_module", _import_module)


def test_extract_list_items_filters_rows_with_regex_match(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_scrapy_selector_import(monkeypatch)

    html = """
    <div class='job-row'>
        <span class='location'>Aarhus</span>
        <a class='job-link' href='/jobs/a'>Butiksmedarbejder Aarhus</a>
    </div>
    <div class='job-row'>
        <span class='location'>Randers</span>
        <a class='job-link' href='/jobs/r'>Butiksmedarbejder Randers</a>
    </div>
    """

    items = item_extraction.extract_list_items(
        html=html,
        base_url="https://example.com",
        list_elements={
            "row": ".job-row",
            "title": "a.job-link",
            "link": "a.job-link",
            "regex": {
                "selector": "span.location",
                "pattern": ".*randers.*",
            },
        },
    )

    assert items == [
        {
            "title": "Butiksmedarbejder Randers",
            "link": "https://example.com/jobs/r",
        }
    ]


def test_extract_list_items_regex_uses_index_fallback_when_selector_is_outside_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_scrapy_selector_import(monkeypatch)

    html = """
    <div class='locations'>
        <span class='location'>Aarhus</span>
        <span class='location'>Randers</span>
    </div>
    <div class='job-row'>
        <a class='job-link' href='/jobs/a'>Aarhus Job</a>
    </div>
    <div class='job-row'>
        <a class='job-link' href='/jobs/r'>Randers Job</a>
    </div>
    """

    items = item_extraction.extract_list_items(
        html=html,
        base_url="https://example.com",
        list_elements={
            "row": ".job-row",
            "title": "a.job-link",
            "link": "a.job-link",
            "regex": {
                "selector": "span.location",
                "pattern": "randers",
            },
        },
    )

    assert items == [
        {
            "title": "Randers Job",
            "link": "https://example.com/jobs/r",
        }
    ]


def test_extract_list_items_raises_for_invalid_regex_config(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_scrapy_selector_import(monkeypatch)

    html = """
    <div class='job-row'>
        <span class='location'>Randers</span>
        <a class='job-link' href='/jobs/r'>Randers Job</a>
    </div>
    """

    with pytest.raises(TypeError):
        item_extraction.extract_list_items(
            html=html,
            base_url="https://example.com",
            list_elements={
                "row": ".job-row",
                "title": "a.job-link",
                "link": "a.job-link",
                "regex": {
                    "selector": "span.location",
                },
            },
        )


def test_extract_list_items_skips_rows_marked_hidden(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_scrapy_selector_import(monkeypatch)

    html = """
    <div class='job-row' data-rk-visible-row-state='hidden'>
        <a class='job-link' href='/jobs/h'>Hidden Job</a>
    </div>
    <div class='job-row' data-rk-visible-row-state='visible'>
        <a class='job-link' href='/jobs/v'>Visible Job</a>
    </div>
    """

    items = item_extraction.extract_list_items(
        html=html,
        base_url="https://example.com",
        list_elements={
            "row": ".job-row",
            "title": "a.job-link",
            "link": "a.job-link",
        },
    )

    assert items == [
        {
            "title": "Visible Job",
            "link": "https://example.com/jobs/v",
        }
    ]


def test_extract_list_items_preserves_root_fallback_index_when_hidden_rows_are_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_scrapy_selector_import(monkeypatch)

    html = """
    <div class='job-row' data-rk-visible-row-state='hidden'></div>
    <div class='job-row' data-rk-visible-row-state='visible'></div>

    <span class='global-title'>Hidden Title</span>
    <span class='global-title'>Visible Title</span>

    <a class='global-link' href='/jobs/h'>Hidden Link</a>
    <a class='global-link' href='/jobs/v'>Visible Link</a>
    """

    items = item_extraction.extract_list_items(
        html=html,
        base_url="https://example.com",
        list_elements={
            "row": ".job-row",
            "title": ".global-title",
            "link": ".global-link",
        },
    )

    assert items == [
        {
            "title": "Visible Title",
            "link": "https://example.com/jobs/v",
        }
    ]
