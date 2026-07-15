import importlib
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urljoin


def extract_list_items(
    html: str,
    base_url: str,
    list_elements: Mapping[str, str],
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
