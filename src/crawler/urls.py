"""URL normalisation and scope.

Two URLs that point at the same page must produce the same key, or the
frontier fills with near-duplicates and the crawl never finishes. Everything
that does not change the response is stripped: fragments, tracking
parameters, default ports, parameter order, host case.
"""

from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

TRACKING_PREFIXES = ("utm_",)
TRACKING_PARAMS = {"gclid", "fbclid", "msclkid", "mc_cid", "mc_eid", "ref", "_ga", "igshid"}
DEFAULT_PORTS = {"http": 80, "https": 443}


def normalize(url: str, base: str | None = None) -> str | None:
    """Return a canonical absolute http(s) URL, or None if it cannot be crawled."""
    if base is not None:
        url = urljoin(base, url)
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    if scheme not in DEFAULT_PORTS or not parts.hostname:
        return None  # mailto:, javascript:, tel:, data: and friends
    host = parts.hostname.lower().rstrip(".")
    port = parts.port
    netloc = host if port in (None, DEFAULT_PORTS[scheme]) else f"{host}:{port}"
    query = sorted(
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() not in TRACKING_PARAMS and not k.lower().startswith(TRACKING_PREFIXES)
    )
    path = parts.path or "/"
    return urlunsplit((scheme, netloc, path, urlencode(query), ""))


_DIGITS = re.compile(r"\d+")


def pattern_of(url: str) -> str:
    """URL shape with numbers masked and query values dropped.

    /calendar?month=7 and /calendar?month=8 share a pattern; capping pages
    per pattern stops calendars and paginated archives from eating the
    whole crawl budget while still sampling them.
    """
    parts = urlsplit(url)
    keys = sorted({k for k, _ in parse_qsl(parts.query, keep_blank_values=True)})
    return f"{parts.netloc}{_DIGITS.sub('{n}', parts.path)}?{'&'.join(keys)}"


def domain_of(url: str) -> str:
    """The politeness and identity key: host plus non-default port."""
    return urlsplit(url).netloc.lower()


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def in_scope(url: str, allowed_domains: frozenset[str]) -> bool:
    domain = domain_of(url)
    host = urlsplit(url).hostname or ""
    return domain in allowed_domains or any(host.endswith("." + d) for d in allowed_domains)
