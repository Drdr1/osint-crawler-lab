"""Pure logic: URL keys, verdicts, extraction, traps, pacing."""

from __future__ import annotations

import pytest

from crawler.detect import Verdict, assess
from crawler.extract import parse, trap_reason
from crawler.politeness import Politeness, parse_retry_after
from crawler.urls import in_scope, normalize, pattern_of


class FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("HTTP://Example.COM:80/a?b=2&a=1#frag", "http://example.com/a?a=1&b=2"),
        ("https://example.com:443", "https://example.com/"),
        ("https://example.com/p?utm_source=x&id=7&fbclid=y", "https://example.com/p?id=7"),
        ("https://example.com:8443/x", "https://example.com:8443/x"),
        ("mailto:a@b.c", None),
        ("javascript:void(0)", None),
    ],
)
def test_normalize(raw: str, expected: str | None) -> None:
    assert normalize(raw) == expected


def test_relative_links_resolve_against_base() -> None:
    assert normalize("../b?x=1", "https://ex.com/a/c/") == "https://ex.com/a/b?x=1"


def test_scope_includes_subdomains_only_when_allowed() -> None:
    allowed = frozenset({"example.com"})
    assert in_scope("https://news.example.com/x", allowed)
    assert not in_scope("https://example.com.evil.net/x", allowed)


def test_pattern_masks_numbers_and_query_values() -> None:
    assert pattern_of("http://h/calendar?month=7") == pattern_of("http://h/calendar?month=12")
    assert pattern_of("http://h/post/12/comments") == "h/post/{n}/comments?"


@pytest.mark.parametrize(
    ("status", "headers", "body", "verdict"),
    [
        (200, {}, "<html><body>" + "real content " * 50 + "</body></html>", Verdict.OK),
        (200, {}, "<div class='g-recaptcha'></div>", Verdict.CAPTCHA),
        (403, {}, "<div class='cf-turnstile'></div>", Verdict.CAPTCHA),
        (200, {}, "<h1>Access denied</h1><p>unusual traffic</p>", Verdict.SOFT_BLOCK),
        (
            200,
            {},
            "<div id=root></div><noscript>enable JavaScript</noscript><script>x()</script>",
            Verdict.JS_REQUIRED,
        ),
        (429, {"Retry-After": "7"}, "", Verdict.RATE_LIMITED),
        (403, {}, "nope", Verdict.FORBIDDEN),
        (404, {}, "", Verdict.NOT_FOUND),
        (502, {}, "", Verdict.SERVER_ERROR),
        (200, {"content-type": "application/pdf"}, "%PDF", Verdict.NOT_HTML),
    ],
)
def test_assess(status: int, headers: dict[str, str], body: str, verdict: Verdict) -> None:
    assert assess(status, headers, body).verdict == verdict


def test_rate_limit_carries_retry_after() -> None:
    assert assess(429, {"Retry-After": "7"}, "").retry_after == 7.0


def test_rendered_pages_are_not_flagged_as_js_shells_again() -> None:
    shell = "<div id=root></div><script>x()</script>"
    assert assess(200, {}, shell, rendered=True).verdict == Verdict.OK


def test_long_article_mentioning_access_denied_is_not_a_block() -> None:
    body = "<p>" + "A report about access denied errors in government portals. " * 60 + "</p>"
    assert assess(200, {}, body).verdict == Verdict.OK


def test_parse_respects_nofollow_and_dedupes() -> None:
    html = (
        '<title> T </title><link rel="canonical" href="/c"><a href="/a">1</a><a href="/a#x">2</a>'
        '<a href="/n" rel="nofollow">3</a><a href="mailto:x@y">4</a>'
    )
    page = parse(html, "http://h/")
    assert page.title == "T" and page.canonical == "http://h/c"
    assert page.links == ["http://h/a"]


def test_meta_robots_nofollow_blocks_all_links() -> None:
    page = parse('<meta name="robots" content="noindex,nofollow"><a href="/a">a</a>', "http://h/")
    assert page.links == []


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("http://h/loop/loop/loop/loop/", "repeating_path_segment"),
        ("http://h/" + "/".join(f"s{i}" for i in range(15)), "path_too_deep"),
        ("http://h/search?" + "&".join(f"f{i}=1" for i in range(8)), "query_explosion"),
        ("http://h/fine/page?id=1", None),
    ],
)
def test_traps(url: str, reason: str | None) -> None:
    assert trap_reason(url) == reason


def test_retry_after_http_date() -> None:
    assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0  # in the past -> now
    assert parse_retry_after("12") == 12.0
    assert parse_retry_after("soon") is None


def test_politeness_spacing_backoff_and_parking() -> None:
    clock = FakeClock()
    p = Politeness(min_delay=1.0, max_backoff=60, park_seconds=600, max_strikes=3, jitter=0.0, clock=clock)
    p.record("a", Verdict.OK)
    assert p.wait_time("a") == pytest.approx(1.0)
    assert p.wait_time("b") == 0.0  # other domains are unaffected
    p.record("a", Verdict.RATE_LIMITED, retry_after=10)
    assert p.wait_time("a") == pytest.approx(10.0)  # Retry-After beats the 2s backoff
    p.record("a", Verdict.RATE_LIMITED)
    assert p.wait_time("a") == pytest.approx(4.0)  # exponential: 1 * 2^2
    p.record("a", Verdict.RATE_LIMITED)
    assert p.is_parked("a") and p.state("a").park_reason.startswith("3_consecutive")
    clock.t += 601
    assert not p.is_parked("a")


def test_success_resets_strikes() -> None:
    p = Politeness(min_delay=1.0, jitter=0.0, clock=FakeClock())
    p.record("a", Verdict.SERVER_ERROR)
    p.record("a", Verdict.OK)
    assert p.state("a").strikes == 0


def test_captcha_parks_immediately() -> None:
    p = Politeness(park_seconds=900, clock=FakeClock())
    p.record("a", Verdict.CAPTCHA)
    assert p.is_parked("a") and p.state("a").park_reason == "captcha_challenge"


def test_crawl_delay_from_robots_is_a_floor() -> None:
    p = Politeness(min_delay=0.5, jitter=0.0, clock=FakeClock())
    p.state("a").crawl_delay = 5
    p.record("a", Verdict.OK)
    assert p.wait_time("a") == pytest.approx(5.0)
