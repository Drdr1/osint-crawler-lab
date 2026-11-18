"""Parse pages and decide which links are worth following."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlsplit

from bs4 import BeautifulSoup, Tag

from crawler.urls import normalize


@dataclass
class Page:
    title: str = ""
    canonical: str | None = None
    links: list[str] = field(default_factory=list)
    text: str = ""
    meta_robots: str = ""


def parse(html: str, base_url: str) -> Page:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    page = Page()
    if soup.title and soup.title.string:
        page.title = soup.title.string.strip()[:500]
    canon = soup.find("link", rel=lambda v: bool(v) and "canonical" in v)
    if isinstance(canon, Tag) and canon.get("href"):
        page.canonical = normalize(str(canon["href"]), base_url)
    robots = soup.find("meta", attrs={"name": re.compile("^robots$", re.I)})
    if isinstance(robots, Tag) and robots.get("content"):
        page.meta_robots = str(robots["content"]).lower()
    seen: set[str] = set()
    if "nofollow" not in page.meta_robots:
        for a in soup.find_all("a", href=True):
            if "nofollow" in (a.get("rel") or []):
                continue
            url = normalize(str(a["href"]), base_url)
            if url and url not in seen:
                seen.add(url)
                page.links.append(url)
    page.text = " ".join(soup.get_text(" ").split())[:20_000]
    return page


def trap_reason(
    url: str, *, max_path_depth: int = 12, max_query_params: int = 6, max_segment_repeats: int = 3
) -> str | None:
    """Spot URL shapes that generate endless pages (calendars, faceted search, loops)."""
    parts = urlsplit(url)
    segments = [s for s in parts.path.split("/") if s]
    if len(segments) > max_path_depth:
        return "path_too_deep"
    if segments and Counter(segments).most_common(1)[0][1] > max_segment_repeats:
        return "repeating_path_segment"
    if len(parse_qsl(parts.query)) > max_query_params:
        return "query_explosion"
    if len(url) > 2000:
        return "url_too_long"
    return None
