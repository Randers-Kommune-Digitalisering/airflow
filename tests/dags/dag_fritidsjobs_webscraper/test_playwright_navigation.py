import asyncio

import pytest

import dag_fritidsjobs_webscraper.utils.playwright_navigation as navigation
from dag_fritidsjobs_webscraper.utils.playwright_navigation import RouteResult
from dag_fritidsjobs_webscraper.utils.playwright_navigation import _match_option_value_for_label
from dag_fritidsjobs_webscraper.utils.playwright_navigation import _wait_for_in_scope
from dag_fritidsjobs_webscraper.utils.playwright_navigation import _wait_for_route_step_settle
from dag_fritidsjobs_webscraper.utils.playwright_navigation import capture_row_links_via_click
from dag_fritidsjobs_webscraper.utils.playwright_navigation import extract_scope_html
from dag_fritidsjobs_webscraper.utils.playwright_navigation import wait_for_list_elements


def test_match_option_value_for_label_exact_match() -> None:
    options = [
        {"value": "region-mid", "label": "Region Midtjylland"},
        {"value": "region-syd", "label": "Region Syddanmark"},
    ]

    assert _match_option_value_for_label(options, "Region Midtjylland") == "region-mid"


def test_match_option_value_for_label_normalizes_whitespace_and_case() -> None:
    options = [
        {"value": "region-mid", "label": " Region   Midtjylland "},
    ]

    assert _match_option_value_for_label(options, "region midtjylland") == "region-mid"


def test_match_option_value_for_label_supports_contains_fallback() -> None:
    options = [
        {"value": "region-mid", "label": "Region Midtjylland (alle byer)"},
    ]

    assert _match_option_value_for_label(options, "Region Midtjylland") == "region-mid"


def test_match_option_value_for_label_returns_none_when_no_match() -> None:
    options = [
        {"value": "region-syd", "label": "Region Syddanmark"},
    ]

    assert _match_option_value_for_label(options, "Region Midtjylland") is None


def test_match_option_value_for_label_ignores_empty_placeholder_label() -> None:
    options = [
        {"value": "", "label": "   "},
        {"value": "randers", "label": "Randers Kommune"},
    ]

    assert _match_option_value_for_label(options, "Randers") == "randers"


def test_match_option_value_for_label_avoids_weak_reverse_contains_match() -> None:
    options = [
        {"value": "r", "label": "R"},
    ]

    assert _match_option_value_for_label(options, "Randers") is None


def test_wait_for_route_step_settle_falls_back_when_networkidle_times_out() -> None:
    class TimeoutError(Exception):
        pass

    class _FakePage:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int | None]] = []

        async def wait_for_load_state(self, state: str, timeout: int | None = None) -> None:
            self.calls.append((state, timeout))
            if state == "networkidle":
                raise TimeoutError("Timeout 30000ms exceeded")

    page = _FakePage()
    asyncio.run(_wait_for_route_step_settle(page))

    assert page.calls == [
        ("networkidle", 30000),
        ("domcontentloaded", None),
    ]


def test_wait_for_route_step_settle_raises_non_timeout_errors() -> None:
    class _FakePage:
        async def wait_for_load_state(self, state: str, timeout: int | None = None) -> None:
            raise RuntimeError("unexpected playwright failure")

    page = _FakePage()
    with pytest.raises(RuntimeError):
        asyncio.run(_wait_for_route_step_settle(page))


def test_wait_for_in_scope_passes_wait_state_to_wait_for_selector() -> None:
    class _FakeScope:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object]]] = []

        async def wait_for_selector(self, selector: str, **kwargs: object) -> None:
            self.calls.append((selector, kwargs))

    scope = _FakeScope()
    asyncio.run(_wait_for_in_scope(scope, "div[role='listitem']", 1234, "attached"))

    assert scope.calls == [
        (
            "div[role='listitem']",
            {
                "state": "attached",
                "timeout": 1234,
            },
        )
    ]


def test_wait_for_in_scope_passes_wait_state_to_locator_wait() -> None:
    class _FakeFirst:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def wait_for(self, **kwargs: object) -> None:
            self.calls.append(kwargs)

    class _FakeLocator:
        def __init__(self, first: _FakeFirst) -> None:
            self.first = first

    class _FakeScope:
        def __init__(self, locator_obj: _FakeLocator) -> None:
            self._locator_obj = locator_obj

        def locator(self, _selector: str) -> _FakeLocator:
            return self._locator_obj

    first = _FakeFirst()
    scope = _FakeScope(_FakeLocator(first))
    asyncio.run(_wait_for_in_scope(scope, "div[role='listitem']", 4321, "attached"))

    assert first.calls == [
        {
            "state": "attached",
            "timeout": 4321,
        }
    ]


def test_wait_for_list_elements_waits_for_list_update_stability_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    wait_calls: list[tuple[str, int | None, str]] = []
    stability_calls: list[tuple[str, int, int, int]] = []

    async def fake_wait_for_in_scope(
        _scope: object,
        selector: str,
        timeout_ms: int | None = None,
        wait_state: str = "attached",
    ) -> None:
        wait_calls.append((selector, timeout_ms, wait_state))

    async def fake_wait_for_selector_stability(
        _scope: object,
        selector: str,
        timeout_ms: int,
        stability_ms: int = 800,
        poll_ms: int = 150,
    ) -> None:
        stability_calls.append((selector, timeout_ms, stability_ms, poll_ms))

    monkeypatch.setattr(navigation, "_wait_for_in_scope", fake_wait_for_in_scope)
    monkeypatch.setattr(navigation, "_wait_for_selector_stability", fake_wait_for_selector_stability)

    route_result = RouteResult(scope=object())
    list_config = {
        "list_elements": {
            "row": "div[role='listitem']",
            "title": "div.project-title",
            "link": "div.project-title",
        }
    }

    asyncio.run(wait_for_list_elements(object(), route_result, list_config))

    assert wait_calls
    assert stability_calls == [("div[role='listitem']", 30000, 800, 150)]


def test_wait_for_list_elements_can_disable_update_stability_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    stability_calls: list[str] = []

    async def fake_wait_for_in_scope(
        _scope: object,
        _selector: str,
        _timeout_ms: int | None = None,
        _wait_state: str = "attached",
    ) -> None:
        return

    async def fake_wait_for_selector_stability(
        _scope: object,
        selector: str,
        _timeout_ms: int,
        _stability_ms: int = 800,
        _poll_ms: int = 150,
    ) -> None:
        stability_calls.append(selector)

    monkeypatch.setattr(navigation, "_wait_for_in_scope", fake_wait_for_in_scope)
    monkeypatch.setattr(navigation, "_wait_for_selector_stability", fake_wait_for_selector_stability)

    route_result = RouteResult(scope=object())
    list_config = {
        "list_elements": {
            "row": "div[role='listitem']",
            "title": "div.project-title",
        },
        "wait_for_list_update_after_route": False,
    }

    asyncio.run(wait_for_list_elements(object(), route_result, list_config))

    assert stability_calls == []


def test_follow_list_route_runs_wait_and_select_in_same_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    action_calls: list[tuple[str, object]] = []

    async def fake_wait_for_in_scope(
        _scope: object,
        selector: str,
        _timeout_ms: int | None = None,
        _wait_state: str = "attached",
    ) -> None:
        action_calls.append(("wait", selector))

    async def fake_select_option_in_scope(_scope: object, select_config: object) -> None:
        action_calls.append(("select", select_config))

    async def fake_wait_for_route_step_settle(_page: object, _timeout_ms: int = 30000) -> None:
        action_calls.append(("settle", None))

    monkeypatch.setattr(navigation, "_wait_for_in_scope", fake_wait_for_in_scope)
    monkeypatch.setattr(navigation, "_select_option_in_scope", fake_select_option_in_scope)
    monkeypatch.setattr(navigation, "_wait_for_route_step_settle", fake_wait_for_route_step_settle)

    list_route = [
        {
            "wait_for": "select#location_ddfilter",
            "select": {
                "selector": "select#location_ddfilter",
                "label": "Region Midtjylland",
            },
        }
    ]

    asyncio.run(navigation.follow_list_route(object(), list_route))

    assert action_calls == [
        ("wait", "select#location_ddfilter"),
        (
            "select",
            {
                "selector": "select#location_ddfilter",
                "label": "Region Midtjylland",
            },
        ),
        ("settle", None),
    ]


def test_extract_scope_html_prefers_visible_rows_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_extract_visible_rows_html(_scope: object, _row_selector: str) -> str | None:
        return "<div data-visible-rows='1'><div role='listitem'>Filtered</div></div>"

    monkeypatch.setattr(navigation, "_extract_visible_rows_html", fake_extract_visible_rows_html)

    route_result = RouteResult(scope=object())
    list_config = {
        "list_elements": {
            "row": "div[role='listitem']",
            "title": "div.project-title",
        }
    }

    extracted_html = asyncio.run(extract_scope_html(object(), route_result, list_config))

    assert "Filtered" in extracted_html


def test_extract_scope_html_falls_back_to_scope_content_when_visible_rows_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_extract_visible_rows_html(_scope: object, _row_selector: str) -> str | None:
        return None

    monkeypatch.setattr(navigation, "_extract_visible_rows_html", fake_extract_visible_rows_html)

    class _FakeScope:
        async def content(self) -> str:
            return "<html><body>Full content</body></html>"

    route_result = RouteResult(scope=_FakeScope())
    list_config = {
        "list_elements": {
            "row": "div[role='listitem']",
            "title": "div.project-title",
        }
    }

    extracted_html = asyncio.run(extract_scope_html(object(), route_result, list_config))

    assert "Full content" in extracted_html


def test_capture_row_links_via_click_returns_empty_when_row_selector_is_missing() -> None:
    route_result = RouteResult(scope=object())
    list_config = {
        "list_elements": {
            "title": "h3",
        }
    }

    captured = asyncio.run(capture_row_links_via_click(object(), route_result, list_config))

    assert captured == []


def test_capture_row_links_via_click_collects_title_and_link(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeRowLocator:
        def __init__(self, index: int) -> None:
            self.index = index

    class _FakeRowsLocator:
        def __init__(self, count: int) -> None:
            self._count = count

        async def count(self) -> int:
            return self._count

        def nth(self, index: int) -> _FakeRowLocator:
            return _FakeRowLocator(index)

    class _FakeScope:
        def locator(self, _selector: str) -> _FakeRowsLocator:
            return _FakeRowsLocator(2)

    async def fake_read_row_title_for_capture(row_locator: _FakeRowLocator, _title_selector: str | None) -> str:
        return f"Job {row_locator.index + 1}"

    async def fake_capture_url_for_row_click(_page: object, row_locator: _FakeRowLocator, _title_selector: str | None) -> str:
        return f"https://example.com/jobs/{row_locator.index + 1}"

    async def fake_wait_for_list_elements(
        _page: object,
        _route_result: RouteResult,
        _list_config: dict[str, object],
    ) -> None:
        return

    monkeypatch.setattr(navigation, "_resolve_click_capture_scope", lambda _page, _route_result, _list_elements: _FakeScope())
    monkeypatch.setattr(navigation, "_read_row_title_for_capture", fake_read_row_title_for_capture)
    monkeypatch.setattr(navigation, "_capture_url_for_row_click", fake_capture_url_for_row_click)
    monkeypatch.setattr(navigation, "wait_for_list_elements", fake_wait_for_list_elements)

    route_result = RouteResult(scope=object())
    list_config = {
        "list_elements": {
            "row": "div.row",
            "title": "h3",
        }
    }

    captured = asyncio.run(capture_row_links_via_click(object(), route_result, list_config))

    assert captured == [
        {
            "title": "Job 1",
            "link": "https://example.com/jobs/1",
        },
        {
            "title": "Job 2",
            "link": "https://example.com/jobs/2",
        },
    ]


def test_capture_row_links_via_click_waits_for_list_restore_after_same_tab_navigation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeRowLocator:
        def __init__(self, index: int) -> None:
            self.index = index

    class _FakeRowsLocator:
        def __init__(self, count: int) -> None:
            self._count = count

        async def count(self) -> int:
            return self._count

        def nth(self, index: int) -> _FakeRowLocator:
            return _FakeRowLocator(index)

    class _FakeScope:
        def locator(self, _selector: str) -> _FakeRowsLocator:
            return _FakeRowsLocator(1)

    restore_calls: list[tuple[object, RouteResult, dict[str, object]]] = []

    async def fake_wait_for_list_elements(page: object, route_result: RouteResult, list_config: dict[str, object]) -> None:
        restore_calls.append((page, route_result, list_config))

    async def fake_read_row_title_for_capture(_row_locator: _FakeRowLocator, _title_selector: str | None) -> str:
        return "Job 1"

    async def fake_capture_url_for_row_click(_page: object, _row_locator: _FakeRowLocator, _title_selector: str | None) -> str:
        return "https://example.com/jobs/1"

    monkeypatch.setattr(navigation, "_resolve_click_capture_scope", lambda _page, _route_result, _list_elements: _FakeScope())
    monkeypatch.setattr(navigation, "_read_row_title_for_capture", fake_read_row_title_for_capture)
    monkeypatch.setattr(navigation, "_capture_url_for_row_click", fake_capture_url_for_row_click)
    monkeypatch.setattr(navigation, "wait_for_list_elements", fake_wait_for_list_elements)

    page = object()
    route_result = RouteResult(scope=object())
    list_config: dict[str, object] = {
        "list_elements": {
            "row": "div.row",
            "title": "h3",
        }
    }

    captured = asyncio.run(capture_row_links_via_click(page, route_result, list_config))

    assert captured == [
        {
            "title": "Job 1",
            "link": "https://example.com/jobs/1",
        }
    ]
    assert len(restore_calls) == 1
