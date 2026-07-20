import logging
import re
from collections.abc import Mapping
from typing import Any


logger = logging.getLogger(__name__)


def is_timeout_error(exc: Exception) -> bool:
    """
    Determine whether an exception is a Playwright timeout.

    :param exc: Raised exception instance
    :return: True when exception is Playwright timeout
    """
    return exc.__class__.__name__ == "TimeoutError"


async def log_wait_timeout_diagnostics(
    scope: Any,
    selector: str,
    timeout_ms: int | None,
    wait_state: str,
) -> None:
    """
    Log diagnostics for a selector wait timeout.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param selector: CSS selector that timed out
    :param timeout_ms: Configured timeout value in milliseconds
    :param wait_state: Playwright wait state (attached/detached/visible/hidden)
    :return: None
    """
    logger.error(
        "Timeout waiting for selector in scope. selector=%r state=%s timeout_ms=%s",
        selector,
        wait_state,
        timeout_ms,
    )
    await _log_selector_snapshot(scope, selector, "timed_out_selector")


async def log_list_wait_timeout_diagnostics(
    scope: Any,
    failed_selector: str,
    list_elements: Mapping[str, Any],
    timeout_ms: int | None,
    wait_state: str,
) -> None:
    """
    Log list/row specific diagnostics when list element waits time out.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param failed_selector: Selector that triggered timeout
    :param list_elements: Field-to-selector mapping for extraction
    :param timeout_ms: Configured timeout value in milliseconds
    :param wait_state: Playwright wait state (attached/detached/visible/hidden)
    :return: None
    """
    logger.error(
        "List wait timeout diagnostics enabled. failed_selector=%r state=%s timeout_ms=%s",
        failed_selector,
        wait_state,
        timeout_ms,
    )

    row_selector = list_elements.get("row")
    if isinstance(row_selector, str):
        normalized_row_selector = row_selector.strip()
        if normalized_row_selector:
            await _log_selector_snapshot(scope, normalized_row_selector, "row_selector")
            target_selectors = _collect_target_selectors_for_row_diagnostics(list_elements, failed_selector)
            if target_selectors:
                await _log_rows_missing_target_selectors(
                    scope,
                    normalized_row_selector,
                    target_selectors,
                )


async def _log_selector_snapshot(scope: Any, selector: str, label: str) -> None:
    """
    Log count plus first matching HTML snippets for a selector.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param selector: CSS selector used for diagnostics
    :param label: Label used to identify diagnostic entries
    :return: None
    """
    try:
        locator = scope.locator(selector)
        count = await locator.count()
        logger.error("Selector diagnostic [%s]: selector=%r count=%s", label, selector, count)
    except Exception as snapshot_exc:
        logger.warning(
            "Selector diagnostic failed [%s]: selector=%r error=%s",
            label,
            selector,
            snapshot_exc,
        )
        return

    if count <= 0:
        return

    try:
        matched_html = await locator.evaluate_all(
            r"""(elements) => elements.slice(0, 3).map((element) => ({
                html: (element.outerHTML || "").slice(0, 2000),
                text: (element.textContent || "").replace(/\s+/g, " ").trim().slice(0, 300)
            }))"""
        )
    except Exception as html_exc:
        logger.warning(
            "Selector html snapshot failed [%s]: selector=%r error=%s",
            label,
            selector,
            html_exc,
        )
        return

    if not isinstance(matched_html, list):
        logger.warning("Selector html snapshot returned non-list [%s]: selector=%r", label, selector)
        return

    for index, entry in enumerate(matched_html, start=1):
        if not isinstance(entry, Mapping):
            continue

        html_snippet = _truncate_for_log(str(entry.get("html", "")), 2000)
        text_snippet = _truncate_for_log(str(entry.get("text", "")), 300)
        logger.error(
            "Selector snapshot [%s] match=%s selector=%r text=%r html=%s",
            label,
            index,
            selector,
            text_snippet,
            html_snippet,
        )


async def _log_rows_missing_target_selectors(
    scope: Any,
    row_selector: str,
    target_selectors: list[str],
) -> None:
    """
    Log row elements missing one or more target field selectors.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param row_selector: CSS selector identifying each list row
    :param target_selectors: Selectors expected inside each row (title/url/link)
    :return: None
    """
    row_locator = scope.locator(row_selector)
    try:
        row_count = await row_locator.count()
    except Exception as exc:
        logger.warning("Row count diagnostic failed: row_selector=%r error=%s", row_selector, exc)
        return

    if row_count <= 0:
        logger.error(
            "Row diagnostics: no rows found. row_selector=%r target_selectors=%r",
            row_selector,
            target_selectors,
        )
        await _log_nearest_row_container_snapshot(scope, row_selector)
        relaxed_row_selector = _build_relaxed_row_selector(row_selector)
        if relaxed_row_selector is not None:
            await _log_selector_snapshot(scope, relaxed_row_selector, "row_selector_relaxed")
        return

    try:
        row_diagnostics = await row_locator.evaluate_all(
            r"""(rows, selectors) => rows.slice(0, 20).map((row, index) => {
                const missingSelectors = [];
                for (const selector of selectors) {
                    try {
                        if (!row.querySelector(selector)) {
                            missingSelectors.push(selector);
                        }
                    } catch {
                        missingSelectors.push(`${selector} (invalid selector)`);
                    }
                }

                return {
                index,
                missingSelectors,
                html: (row.outerHTML || "").slice(0, 2000),
                text: (row.textContent || "").replace(/\s+/g, " ").trim().slice(0, 300)
                };
            })""",
            target_selectors,
        )
    except Exception as exc:
        logger.warning(
            "Row diagnostic failed: row_selector=%r target_selectors=%r error=%s",
            row_selector,
            target_selectors,
            exc,
        )
        return

    if not isinstance(row_diagnostics, list):
        logger.warning(
            "Row diagnostic returned non-list: row_selector=%r target_selectors=%r",
            row_selector,
            target_selectors,
        )
        return

    missing_rows_logged = 0
    for row_entry in row_diagnostics:
        if not isinstance(row_entry, Mapping):
            continue

        missing_selectors = row_entry.get("missingSelectors")
        if not isinstance(missing_selectors, list):
            continue

        normalized_missing_selectors = [value for value in missing_selectors if isinstance(value, str) and value]
        if not normalized_missing_selectors:
            continue

        row_index = row_entry.get("index")
        row_html = _truncate_for_log(str(row_entry.get("html", "")), 2000)
        row_text = _truncate_for_log(str(row_entry.get("text", "")), 300)
        logger.error(
            "Row missing target selectors: row_selector=%r row_index=%s missing=%r text=%r html=%s",
            row_selector,
            row_index,
            normalized_missing_selectors,
            row_text,
            row_html,
        )
        missing_rows_logged += 1

    if missing_rows_logged == 0:
        logger.error(
            "All sampled rows contain target selectors. row_selector=%r target_selectors=%r sampled_rows=%s",
            row_selector,
            target_selectors,
            min(row_count, 20),
        )


def _collect_target_selectors_for_row_diagnostics(
    list_elements: Mapping[str, Any],
    failed_selector: str,
) -> list[str]:
    """
    Collect row-nested selectors to verify when list waits time out.

    :param list_elements: Field-to-selector mapping for extraction
    :param failed_selector: Selector that triggered timeout
    :return: Ordered unique selectors for row-level diagnostics
    """
    selectors: list[str] = []
    seen: set[str] = set()

    for field_name in ("title", "url", "link"):
        value = list_elements.get(field_name)
        if not isinstance(value, str):
            continue

        normalized_value = value.strip()
        if not normalized_value or normalized_value in seen:
            continue

        selectors.append(normalized_value)
        seen.add(normalized_value)

    normalized_failed_selector = failed_selector.strip()
    if normalized_failed_selector and normalized_failed_selector not in seen:
        selectors.append(normalized_failed_selector)

    return selectors


def _build_relaxed_row_selector(row_selector: str) -> str | None:
    """
    Relax strict child-combinator selectors for debug-only fallback sampling.

    :param row_selector: Configured row selector
    :return: Relaxed selector string, or None when unchanged
    """
    relaxed_selector = re.sub(r"\s*>\s*", " ", row_selector).strip()
    if not relaxed_selector or relaxed_selector == row_selector:
        return None

    return relaxed_selector


async def _log_nearest_row_container_snapshot(scope: Any, row_selector: str) -> None:
    """
    Log nearest matched container HTML when row selector has zero matches.

    :param scope: Playwright interaction scope (Page, Frame, or FrameLocator)
    :param row_selector: Configured row selector
    :return: None
    """
    candidate_selectors = _build_row_container_candidates(row_selector)
    if not candidate_selectors:
        logger.error("No container candidates could be derived for row_selector=%r", row_selector)
        return

    for candidate_selector in candidate_selectors:
        try:
            container_locator = scope.locator(candidate_selector)
            container_count = await container_locator.count()
        except Exception as exc:
            logger.warning(
                "Container candidate check failed: row_selector=%r candidate=%r error=%s",
                row_selector,
                candidate_selector,
                exc,
            )
            continue

        if container_count <= 0:
            continue

        try:
            container_info = await container_locator.first.evaluate(
                r"""(element) => ({
                    html: (element?.outerHTML || "").slice(0, 3500),
                    text: (element?.textContent || "").replace(/\s+/g, " ").trim().slice(0, 500),
                    childCount: element?.children?.length || 0
                })"""
            )
        except Exception as exc:
            logger.warning(
                "Container html snapshot failed: row_selector=%r candidate=%r error=%s",
                row_selector,
                candidate_selector,
                exc,
            )
            return

        if not isinstance(container_info, Mapping):
            logger.warning(
                "Container html snapshot returned non-mapping: row_selector=%r candidate=%r",
                row_selector,
                candidate_selector,
            )
            return

        container_html = _truncate_for_log(str(container_info.get("html", "")), 3500)
        container_text = _truncate_for_log(str(container_info.get("text", "")), 500)
        child_count = container_info.get("childCount")

        logger.error(
            "Nearest container snapshot: row_selector=%r container_selector=%r container_count=%s child_count=%s text=%r html=%s",
            row_selector,
            candidate_selector,
            container_count,
            child_count,
            container_text,
            container_html,
        )
        return

    logger.error(
        "No container candidates matched in scope. row_selector=%r candidates=%r",
        row_selector,
        candidate_selectors,
    )


def _build_row_container_candidates(row_selector: str) -> list[str]:
    """
    Build nearest-to-farthest container selector candidates from a row selector.

    :param row_selector: Configured row selector
    :return: Ordered candidate selectors (nearest container first)
    """
    normalized_selector = row_selector.strip()
    if not normalized_selector:
        return []

    child_parts = [part.strip() for part in normalized_selector.split(">") if part.strip()]
    if len(child_parts) > 1:
        candidates: list[str] = []
        for part_count in range(len(child_parts) - 1, 0, -1):
            candidate = " > ".join(child_parts[:part_count]).strip()
            if candidate and candidate not in candidates:
                candidates.append(candidate)
        return candidates

    descendant_parts = [part for part in re.split(r"\s+", normalized_selector) if part]
    if len(descendant_parts) > 1:
        candidates = []
        for part_count in range(len(descendant_parts) - 1, 0, -1):
            candidate = " ".join(descendant_parts[:part_count]).strip()
            if candidate and candidate not in candidates:
                candidates.append(candidate)
        return candidates

    return []


def _truncate_for_log(value: str, limit: int) -> str:
    """
    Truncate long log values to avoid oversized log lines.

    :param value: Raw log value
    :param limit: Maximum number of characters to retain
    :return: Truncated value
    """
    if len(value) <= limit:
        return value

    return value[:limit] + f"... [truncated {len(value) - limit} chars]"
