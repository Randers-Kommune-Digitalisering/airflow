import importlib
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urljoin


VISIBLE_ROW_STATE_ATTR = "data-rk-visible-row-state"


def extract_list_items(
    html: str,
    base_url: str,
    list_elements: Mapping[str, Any],
) -> list[dict[str, str]]:
    """
    Extract configured fields from rendered HTML into item dictionaries.

    :param html: Rendered page HTML to parse
    :param base_url: Base URL used to normalize relative links
    :param list_elements: Field-to-selector mapping for extraction
    :return: Extracted items for the current list
    """
    selector_class = importlib.import_module("scrapy").Selector
    selector = selector_class(text=html)
    regex_config = _normalize_regex_config(list_elements.get("regex"))
    field_selectors = {
        field_name: css_selector
        for field_name, css_selector in list_elements.items()
        if field_name not in {"row", "frame", "regex"} and isinstance(css_selector, str)
    }
    if not field_selectors:
        return []

    row_selector = list_elements.get("row")
    if row_selector:
        row_nodes = selector.css(row_selector)
        items: list[dict[str, str]] = []
        for index, row_node in enumerate(row_nodes):
            if not _should_include_row_node(row_node):
                continue

            item = _build_item_from_node(
                row_node,
                selector,
                base_url,
                field_selectors,
                index,
                regex_config=regex_config,
            )
            if item is not None:
                items.append(item)
        return items

    primary_field_name, primary_selector = next(iter(field_selectors.items()))
    primary_nodes = selector.css(primary_selector)
    items = []
    for index, node in enumerate(primary_nodes):
        item = _build_item_from_node(
            node,
            selector,
            base_url,
            field_selectors,
            index,
            primary_field_name,
            regex_config=regex_config,
        )
        if item is not None:
            items.append(item)
    return items


def _should_include_row_node(row_node: Any) -> bool:
    """
    Decide whether a row should be included based on optional visibility markers.

    When no marker is present, keep backward-compatible behavior and include the row.

    :param row_node: Selector node representing the current row
    :return: True when row should be considered for extraction
    """
    marker_state = row_node.xpath(f"@{VISIBLE_ROW_STATE_ATTR}").get()
    if not isinstance(marker_state, str):
        return True

    normalized_state = marker_state.strip().lower()
    if not normalized_state:
        return True

    return normalized_state == "visible"


def _build_item_from_node(
    node: Any,
    root_selector: Any,
    base_url: str,
    field_selectors: Mapping[str, str],
    index: int,
    primary_field_name: str | None = None,
    regex_config: Mapping[str, str] | None = None,
) -> dict[str, str] | None:
    """
    Build one scraped item from a selector node and its configured fields.

    :param node: Selector node representing the current item or primary field
    :param root_selector: Root selector for page-level fallback lookups
    :param base_url: Base URL used to normalize relative links
    :param field_selectors: Field-to-selector mapping for extraction
    :param index: Current item index used for fallback alignment
    :param primary_field_name: Field name used to anchor item extraction
    :param regex_config: Optional selector+pattern filter evaluated per row
    :return: Extracted item values for the current node or None when filtered out
    """
    if regex_config and not _matches_regex_filter(node, root_selector, index, regex_config):
        return None

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


def _normalize_regex_config(raw_regex_config: Any) -> dict[str, str] | None:
    """
    Validate and normalize optional regex filter configuration.

    :param raw_regex_config: Raw value from list_elements["regex"]
    :return: Normalized regex config containing selector and pattern
    """
    if raw_regex_config is None:
        return None

    if not isinstance(raw_regex_config, Mapping):
        raise TypeError("list_elements['regex'] must be an object with 'selector' and 'pattern'")

    selector = raw_regex_config.get("selector")
    pattern = raw_regex_config.get("pattern")
    if not isinstance(selector, str) or not selector.strip():
        raise TypeError("list_elements['regex']['selector'] must be a non-empty string")
    if not isinstance(pattern, str) or not pattern.strip():
        raise TypeError("list_elements['regex']['pattern'] must be a non-empty string")

    return {
        "selector": selector.strip(),
        "pattern": pattern,
    }


def _matches_regex_filter(
    node: Any,
    root_selector: Any,
    index: int,
    regex_config: Mapping[str, str],
) -> bool:
    """
    Evaluate row-level regex filtering using existing selector fallback behavior.

    :param node: Selector node representing the current item row
    :param root_selector: Root selector for page-level fallback lookups
    :param index: Current item index used for fallback alignment
    :param regex_config: Selector and regex pattern used for row filtering
    :return: True when row should be kept, False when row should be skipped
    """
    candidate_text = _extract_field_value(
        node=node,
        root_selector=root_selector,
        field_name="regex_filter_value",
        css_selector=regex_config["selector"],
        base_url="",
        index=index,
        prefer_node_text=False,
    )
    if not candidate_text:
        return False

    try:
        return re.search(regex_config["pattern"], candidate_text, flags=re.IGNORECASE) is not None
    except re.error as exc:
        raise ValueError(f"Invalid regex pattern in list_elements['regex']['pattern']: {exc}") from exc


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
            onclick_value = node.css(f"{css_selector}::attr(onclick)").get()
            value = _extract_url_from_onclick(onclick_value) if onclick_value else None
        if not value:
            onclick_value = node.xpath("ancestor-or-self::*[@onclick][1]/@onclick").get()
            value = _extract_url_from_onclick(onclick_value) if onclick_value else None
        if not value:
            href_values = root_selector.css(f"{css_selector}::attr(href)").getall()
            value = href_values[index] if index < len(href_values) else None
        if not value:
            value = _extract_indexed_onclick_url(root_selector, css_selector, index)
        return urljoin(base_url, value.strip()) if value else None

    if prefer_node_text:
        value = node.css("::text").get()
    else:
        value = node.css(f"{css_selector}::text").get()

    if not value:
        text_values = root_selector.css(f"{css_selector}::text").getall()
        value = text_values[index] if index < len(text_values) else None

    return value.strip() if value else None


def _extract_indexed_onclick_url(
    root_selector: Any,
    css_selector: str,
    index: int,
) -> str | None:
    """
    Extract URL from onclick on the indexed root-level selector match.

    :param root_selector: Root selector for page-level lookups
    :param css_selector: CSS selector configured for the field
    :param index: Current item index used for fallback alignment
    :return: URL extracted from onclick or None
    """
    nodes = root_selector.css(css_selector)
    if index >= len(nodes):
        return None

    onclick_value = nodes[index].xpath("ancestor-or-self::*[@onclick][1]/@onclick").get()
    if not onclick_value:
        return None

    return _extract_url_from_onclick(onclick_value)


def _extract_url_from_onclick(onclick_value: str) -> str | None:
    """
    Parse common JavaScript onclick navigation patterns into a URL.

    :param onclick_value: Raw onclick JavaScript attribute value
    :return: Extracted URL string or None when pattern is unsupported
    """
    patterns = (
        r"(?:window\.|document\.)?location(?:\.href)?\s*=\s*['\"]([^'\"]+)['\"]",
        r"(?:window\.|document\.)?location\.(?:assign|replace)\(\s*['\"]([^'\"]+)['\"]\s*\)",
        r"(?:window\.)?open\(\s*['\"]([^'\"]+)['\"]",
    )

    for pattern in patterns:
        match = re.search(pattern, onclick_value)
        if match:
            return match.group(1)

    return None
