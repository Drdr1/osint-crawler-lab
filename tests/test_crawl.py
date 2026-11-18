"""End-to-end: crawl the hostile site and check what happened, from both sides.

Outcomes are read from PostgreSQL; behaviour toward the site is read from
the site's own request log (what was fetched, how often, how fast).
"""

from __future__ import annotations

import uuid
from itertools import pairwise
from typing import Any

from sqlalchemy import select

from conftest import Rig, build_worker
from crawler import store
from crawler.models import domain_state, pages
from crawler.queue import DLQ, FRONTIER


async def outcomes(rig: Rig, crawl_id: uuid.UUID) -> dict[str, dict[str, Any]]:
    async with rig.sm() as session:
        rows = (await session.execute(select(pages).where(pages.c.crawl_id == crawl_id))).mappings().all()
    base = rig.site.base
    return {
        r["url"].replace(base, "").replace(f"http://{rig.site.other_host}", "OTHER"): dict(r) for r in rows
    }


async def crawled(rig: Rig) -> tuple[uuid.UUID, dict[str, dict[str, Any]]]:
    crawl_id = await rig.start(max_depth=3, max_pages=100)
    await rig.worker.run_until_idle(deadline=90)
    return crawl_id, await outcomes(rig, crawl_id)


async def test_full_crawl_outcomes_and_site_side_behaviour(rig: Rig) -> None:
    crawl_id, pages_by_url = await crawled(rig)
    o = {url: row["outcome"] for url, row in pages_by_url.items()}
    log = rig.site.log
    host = rig.site.main_host

    # Plain pages, fetched over HTTP.
    assert o["/"] == "ok" and o["/static/1"] == "ok" and o["/static/2"] == "ok"
    assert pages_by_url["/static/1"]["render_mode"] == "http"

    # URL normalisation: #fragment and utm_ variants of /static/1 were one fetch.
    assert log.count("/static/1", host) == 1

    # JS-only page: HTTP saw an empty shell, Chromium rendered it, and the link that
    # only exists after rendering (/static/3) was discovered and crawled.
    assert o["/js-only"] == "ok" and pages_by_url["/js-only"]["render_mode"] == "browser"
    assert pages_by_url["/js-only"]["title"] == "App"
    assert o["/static/3"] == "ok"

    # 429 with Retry-After: retried later, not hammered, then succeeded.
    assert o["/flaky"] == "ok" and log.count("/flaky", host) == 3
    flaky = [h.at for h in log.hits if h.path == "/flaky"]
    assert min(b - a for a, b in pairwise(flaky)) >= 0.95  # honoured Retry-After: 1

    # Cookie-gated area: works because cookies and user agent stay consistent per domain.
    assert o["/guard/area"] == "ok"

    # 200 "access denied" page: classified as a block, never stored as content.
    assert o["/soft-block"] == "soft_block" and pages_by_url["/soft-block"]["content_sha256"] is None
    assert log.count("/soft-block", host) == 3  # max_attempts, with backoff between

    # robots.txt: the disallowed page was never requested at all.
    assert o["/private/secret"] == "robots_disallowed"
    assert log.count("/private/secret") == 0

    # Mirrors: two URLs, one stored object.
    assert pages_by_url["/dup-a"]["content_sha256"] == pages_by_url["/dup-b"]["content_sha256"]

    # Traps: calendar capped by the per-pattern budget, /loop/loop/... stopped by shape.
    calendar_hits = [h for h in log.hits if h.path.startswith("/calendar")]
    assert 1 <= len(calendar_hits) <= 5
    assert not any(h.path.count("loop") > 3 for h in log.hits)

    # Permanent failures recorded once, without retry storms.
    assert o["/gone"] == "not_found" and log.count("/gone", host) == 1
    assert o["/redirect-loop"] == "redirect_loop"
    # Transient 500: retried and recovered.
    assert o["/error-once"] == "ok" and log.count("/error-once", host) == 2
    # Timeouts: bounded retries, then a recorded failure.
    assert o["/slow"] == "failed_timeout" and log.count("/slow", host) == 3

    # Scope: external and nofollow links never fetched.
    assert not any("elsewhere" in h.path for h in log.hits)
    assert log.count("/nofollow-me") == 0

    # Politeness: every request to the main host was at least min_delay apart.
    times = sorted(h.at for h in log.for_host(host) if h.path != "/favicon.ico")
    gaps = [b - a for a, b in pairwise(times)]
    hosts_hits = sorted((h for h in log.for_host(host) if h.path != "/favicon.ico"), key=lambda h: h.at)
    fast = [(a.path, b.path, round(b.at - a.at, 3)) for a, b in pairwise(hosts_hits) if b.at - a.at < 0.28]
    assert min(gaps) >= 0.28, f"too fast: {fast}"

    # Every request carried the same declared user agent.
    assert {h.user_agent for h in log.hits} == {
        "OSINTLabBot/1.0 (+https://github.com/Drdr1/osint-crawler-lab)"
    }

    async with rig.sm() as session:
        summary = await store.crawl_summary(session, crawl_id)
    assert summary["deferred"] == 0


async def test_captcha_parks_the_domain_and_stops_requests(rig: Rig) -> None:
    _, pages_by_url = await crawled(rig)
    other = rig.site.other_host
    assert pages_by_url["OTHER/captcha"]["outcome"] == "captcha"
    async with rig.sm() as session:
        park = (
            (await session.execute(select(domain_state).where(domain_state.c.domain == other)))
            .mappings()
            .one()
        )
    assert park["park_reason"] == "captcha_challenge"
    # After each challenge the domain went quiet for the whole park period (2s in tests).
    captcha_times = [h.at for h in rig.site.log.for_host(other) if h.path == "/captcha"]
    assert len(captcha_times) == 3  # max_attempts, then it gave up instead of looping
    assert all(b - a >= 1.9 for a, b in pairwise(captcha_times))


async def test_inconsistent_identity_is_refused_by_the_guarded_area(rig: Rig) -> None:
    """Control experiment: drop the cookie between requests and the site says 403."""
    await rig.start(max_depth=0, max_pages=1)
    identity = rig.worker.identities.for_domain(rig.site.main_host)
    await rig.worker.http.fetch(rig.site.base + "/guard/start", identity)
    identity.cookies.clear()
    result = await rig.worker.http.fetch(rig.site.base + "/guard/area", identity)
    assert result.status == 403


async def test_crash_before_commit_redelivers_without_duplicates(rig: Rig) -> None:
    """Process a batch, 'crash' before committing offsets, restart from committed offsets."""
    crawl_id = await rig.start(max_depth=1, max_pages=100)
    consumer = rig.make_consumer()
    await store.publish_outbox(rig.sm, rig.producer)
    messages = await consumer.poll()
    for m in messages:  # do the work...
        await rig.worker.process(m.value)
    # ...but never commit. A new consumer in the same group gets them all again.
    replay = await rig.make_consumer().poll()
    assert [m.value["url"] for m in replay] == [m.value["url"] for m in messages]
    before = rig.site.log.count("/", rig.site.main_host)
    outcomes_ = [await rig.worker.process(m.value) for m in replay]
    assert set(outcomes_) == {"already_done"}
    assert rig.site.log.count("/", rig.site.main_host) == before  # no second fetch
    async with rig.sm() as session:
        rows = (await session.execute(select(pages).where(pages.c.crawl_id == crawl_id))).all()
    assert len(rows) == len({m.value["url"] for m in messages})


async def test_page_budget_is_a_hard_cap(rig: Rig) -> None:
    crawl_id = await rig.start(max_depth=3, max_pages=5)
    await rig.worker.run_until_idle(deadline=60)
    async with rig.sm() as session:
        summary = await store.crawl_summary(session, crawl_id)
    assert summary["enqueued"] == 5
    assert sum(summary["outcomes"].values()) == 5


async def test_parked_domain_survives_a_worker_restart(rig: Rig) -> None:
    await rig.start(max_depth=0, max_pages=1)
    async with rig.sm.begin() as session:
        await store.park_domain(session, rig.site.main_host, store.later(30), "captcha_challenge")
    # A brand-new worker (fresh in-memory politeness) must still respect the park.
    fresh = build_worker(rig.sm, rig.blobs, rig.producer, rig.make_consumer())
    try:
        handled = await fresh.run_once()
        assert handled == 1 and fresh.stats.outcomes["deferred"] == 1
        assert rig.site.log.count("/", rig.site.main_host) == 0
    finally:
        await fresh.http.close()
        await fresh.browser.close()


async def test_malformed_message_goes_to_dead_letter_and_partition_keeps_moving(rig: Rig) -> None:
    await rig.producer.send(FRONTIER, rig.site.main_host, {"url": "no crawl id here"})
    crawl_id = await rig.start(max_depth=0, max_pages=1)
    await rig.worker.run_until_idle(deadline=30)
    assert rig.worker.stats.outcomes["dead_lettered"] == 1
    broker = rig.producer.broker
    dead = [m for (topic, _), log in broker.log.items() if topic == DLQ for m in log]
    assert len(dead) == 1 and "KeyError" in dead[0].value["reason"]
    async with rig.sm() as session:
        summary = await store.crawl_summary(session, crawl_id)
    assert summary["outcomes"] == {"ok": 1}  # the real message behind it was still processed
