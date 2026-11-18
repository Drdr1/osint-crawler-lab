"""Classify a response before trusting it.

A 200 is not proof of success: block pages, CAPTCHA challenges and empty
JavaScript shells all come back as 200. Storing those as real content
silently poisons an OSINT dataset, so every response gets a verdict first.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum


class Verdict(StrEnum):
    OK = "ok"
    JS_REQUIRED = "js_required"  # empty shell; needs a real browser
    CAPTCHA = "captcha"  # challenge page; park the domain, never hammer it
    SOFT_BLOCK = "soft_block"  # 200 with an access-denied page
    RATE_LIMITED = "rate_limited"  # 429 / 503 with Retry-After
    FORBIDDEN = "forbidden"  # 401 / 403
    NOT_FOUND = "not_found"  # 404 / 410: permanent
    SERVER_ERROR = "server_error"  # 5xx: transient
    NOT_HTML = "not_html"


CAPTCHA_MARKERS = re.compile(
    r"g-recaptcha|h-captcha|cf-turnstile|cf-challenge|captcha-container|verify you are human|"
    r"are you a robot|challenge-platform",
    re.I,
)
BLOCK_MARKERS = re.compile(
    r"access denied|request blocked|you have been blocked|unusual traffic|"
    r"automated (queries|requests)|forbidden by administrative rules",
    re.I,
)
JS_SHELL_MARKERS = re.compile(r"enable javascript|requires javascript|<noscript>", re.I)
TAG_RE = re.compile(r"<script.*?</script>|<style.*?</style>|<[^>]+>", re.I | re.S)


@dataclass(frozen=True)
class Assessment:
    verdict: Verdict
    retry_after: float | None = None
    detail: str = ""


def visible_text_length(html: str) -> int:
    return len(" ".join(TAG_RE.sub(" ", html).split()))


def assess(status: int, headers: dict[str, str], body: str, *, rendered: bool = False) -> Assessment:
    from crawler.politeness import parse_retry_after

    lower_headers = {k.lower(): v for k, v in headers.items()}
    if status in (429, 503):
        return Assessment(
            Verdict.RATE_LIMITED, parse_retry_after(lower_headers.get("retry-after")), f"http_{status}"
        )
    if status in (401, 403):
        # Many challenge pages are served as 403; prefer the more specific verdict.
        if CAPTCHA_MARKERS.search(body):
            return Assessment(Verdict.CAPTCHA, detail=f"http_{status}")
        return Assessment(Verdict.FORBIDDEN, detail=f"http_{status}")
    if status in (404, 410):
        return Assessment(Verdict.NOT_FOUND, detail=f"http_{status}")
    if status >= 500:
        return Assessment(Verdict.SERVER_ERROR, detail=f"http_{status}")
    ctype = lower_headers.get("content-type", "text/html")
    if "html" not in ctype:
        return Assessment(Verdict.NOT_HTML, detail=ctype)
    if CAPTCHA_MARKERS.search(body):
        return Assessment(Verdict.CAPTCHA, detail="challenge_markup")
    text_len = visible_text_length(body)
    if BLOCK_MARKERS.search(body) and text_len < 2000:
        return Assessment(Verdict.SOFT_BLOCK, detail="block_page_markup")
    if not rendered and text_len < 200 and (JS_SHELL_MARKERS.search(body) or "<script" in body.lower()):
        return Assessment(Verdict.JS_REQUIRED, detail=f"visible_text_{text_len}")
    return Assessment(Verdict.OK)
