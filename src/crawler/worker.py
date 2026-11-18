"""Frontier consumer: one URL in, one stored outcome out, never two.

Per message:
  scope/robots -> politeness gate -> HTTP fetch -> verdict
  -> (JS shell? render in Chromium) -> store page + new links (one transaction)
  -> commit the Kafka offset.

Delivery is at-least-once, so the database makes processing idempotent:
a URL whose outcome is final is acknowledged without being fetched again.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler import store
from crawler.blobs import BlobStore
from crawler.detect import Assessment, Verdict, assess
from crawler.extract import parse, trap_reason
from crawler.fetch import BrowserFetcher, FetchResult, HttpFetcher
from crawler.identity import IdentityPool
from crawler.politeness import Politeness
from crawler.queue import Consumer, Message, Producer
from crawler.robots import RobotsCache
from crawler.urls import domain_of, in_scope, normalize, url_hash

log = logging.getLogger("crawler.worker")

RETRYABLE = {Verdict.RATE_LIMITED, Verdict.SERVER_ERROR, Verdict.FORBIDDEN, Verdict.SOFT_BLOCK}


@dataclass
class WorkerConfig:
    max_inline_wait: float = 2.0  # wait this long in-line; anything longer is deferred
    max_attempts: int = 4
    retry_base: float = 1.0
    max_per_pattern: int = 25
    concurrency: int = 8  # domains processed in parallel; one at a time per domain


@dataclass
class Stats:
    outcomes: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    skipped_links: dict[str, int] = field(default_factory=lambda: defaultdict(int))


class Worker:
    def __init__(
        self,
        sm: async_sessionmaker[AsyncSession],
        producer: Producer,
        consumer: Consumer,
        blobs: BlobStore,
        http: HttpFetcher,
        browser: BrowserFetcher,
        identities: IdentityPool,
        politeness: Politeness,
        robots: RobotsCache,
        config: WorkerConfig | None = None,
    ) -> None:
        self.sm, self.producer, self.consumer, self.blobs = sm, producer, consumer, blobs
        self.http, self.browser, self.identities = http, browser, identities
        self.politeness, self.robots = politeness, robots
        self.config = config or WorkerConfig()
        self.stats = Stats()
        self._crawls: dict[uuid.UUID, store.Crawl] = {}
        self._sem = asyncio.Semaphore(self.config.concurrency)

    # -- loop ----------------------------------------------------------------

    async def run_once(self, timeout: float = 0.5) -> int:
        async with self.sm.begin() as session:
            await store.release_due(session)
        await store.publish_outbox(self.sm, self.producer)
        messages = await self.consumer.poll(timeout=timeout)
        if not messages:
            return 0
        by_domain: dict[str, list[Message]] = defaultdict(list)
        for m in messages:
            by_domain[m.key].append(m)
        await asyncio.gather(*(self._domain_batch(batch) for batch in by_domain.values()))
        # Offsets are committed only after every message in the batch is stored.
        await self.consumer.commit(messages)
        await store.publish_outbox(self.sm, self.producer)
        return len(messages)

    async def run_until_idle(self, idle_rounds: int = 6, deadline: float = 120.0) -> None:
        """Test/ops helper: drain the frontier, including deferred work that comes due."""
        quiet = 0
        end = time.monotonic() + deadline
        while time.monotonic() < end:
            handled = await self.run_once(timeout=0.3)
            async with self.sm() as session:
                pending = await store.deferred_count(session)
            quiet = quiet + 1 if handled == 0 and not pending else 0
            if quiet >= idle_rounds:
                return
            if handled == 0 and pending:
                await asyncio.sleep(0.2)
        raise TimeoutError("crawl did not go idle before the deadline")

    async def _domain_batch(self, batch: list[Message]) -> None:
        async with self._sem:
            for m in batch:  # sequential within a domain: politeness is per domain
                outcome = await self.process(m.value)
                self.stats.outcomes[outcome] += 1

    # -- one URL -------------------------------------------------------------

    async def _crawl(self, crawl_id: uuid.UUID) -> store.Crawl | None:
        if crawl_id not in self._crawls:
            async with self.sm() as session:
                crawl = await store.load_crawl(session, crawl_id)
            if crawl is None:
                return None
            self._crawls[crawl_id] = crawl
        return self._crawls[crawl_id]

    async def process(self, payload: dict[str, Any]) -> str:
        crawl = await self._crawl(uuid.UUID(payload["crawl_id"]))
        if crawl is None:
            return "unknown_crawl"
        url: str = payload["url"]
        depth: int = payload["depth"]
        attempt: int = payload.get("attempt", 0)
        uhash = url_hash(url)
        domain = domain_of(url)

        async with self.sm() as session:
            if await store.page_is_final(session, crawl.id, uhash):
                return "already_done"  # redelivery after a crash: no second fetch
            parked = await store.parked_until(session, domain)
        if parked:
            async with self.sm.begin() as session:
                await store.defer(session, payload, parked, "domain_parked")
            return "deferred"

        identity = self.identities.for_domain(domain)
        if await self._defer_if_busy(payload, domain):
            return "deferred"
        allowed, crawl_delay, fetched = await self.robots.allowed(url, urlsplit(url).scheme, identity)
        if fetched:  # robots.txt was a real request: it counts against the domain's pace
            self.politeness.record(domain, Verdict.OK)
        if crawl_delay:
            self.politeness.state(domain).crawl_delay = crawl_delay
        if not allowed:
            await self._record(crawl, url, depth, "robots_disallowed", attempt)
            return "robots_disallowed"

        if await self._defer_if_busy(payload, domain):
            return "deferred"

        hop_delay = max(self.politeness.min_delay, self.politeness.state(domain).crawl_delay)
        result = await self.http.fetch(url, identity, hop_delay=hop_delay)
        if result.redirect_to:
            return await self._handoff_redirect(crawl, url, depth, attempt, result)
        verdict = self._verdict(result)
        if verdict.verdict == Verdict.JS_REQUIRED:
            # Escalate only when the page proved it needs a browser.
            self.politeness.record(domain, Verdict.OK)
            await asyncio.sleep(self.politeness.wait_time(domain))
            result = await self.browser.fetch(url, identity)
            verdict = self._verdict(result, rendered=True)

        if result.error:
            return await self._transport_failure(crawl, payload, result)

        self.politeness.record(domain, verdict.verdict, verdict.retry_after)
        if self.politeness.is_parked(domain):
            state = self.politeness.state(domain)
            park_end = datetime.now(UTC).timestamp() + (state.parked_until - time.monotonic())
            async with self.sm.begin() as session:
                await store.park_domain(
                    session, domain, datetime.fromtimestamp(park_end, UTC), state.park_reason
                )

        if verdict.verdict == Verdict.OK:
            return await self._store_ok(crawl, url, depth, attempt, result)
        if verdict.verdict == Verdict.CAPTCHA:
            # Record what happened; keep the URL for after the park expires, but
            # give up after max_attempts rather than knocking on a closed door.
            async with self.sm.begin() as session:
                await store.record_page(
                    session, self._row(crawl, url, depth, "captcha", attempt, result, detail=verdict.detail)
                )
                if attempt + 1 < self.config.max_attempts:
                    until = await store.parked_until(session, domain) or store.later(3600)
                    await store.defer(session, {**payload, "attempt": attempt + 1}, until, "captcha")
            return "captcha"
        if verdict.verdict in RETRYABLE and attempt + 1 < self.config.max_attempts:
            delay = max(self.politeness.wait_time(domain), self.config.retry_base * 2**attempt)
            async with self.sm.begin() as session:
                await store.defer(
                    session, {**payload, "attempt": attempt + 1}, store.later(delay), verdict.verdict.value
                )
            return "retry"
        await self._record(crawl, url, depth, verdict.verdict.value, attempt, result, verdict.detail)
        return verdict.verdict.value

    # -- helpers -------------------------------------------------------------

    async def _defer_if_busy(self, payload: dict[str, Any], domain: str) -> bool:
        """Wait briefly in-line; hand longer waits to the deferred table instead of blocking."""
        wait = self.politeness.wait_time(domain)
        if wait > self.config.max_inline_wait:
            async with self.sm.begin() as session:
                await store.defer(session, payload, store.later(wait), "politeness")
            return True
        if wait:
            await asyncio.sleep(wait)
        return False

    @staticmethod
    def _verdict(result: FetchResult, rendered: bool = False) -> Assessment:
        if result.error:
            return Assessment(Verdict.SERVER_ERROR, detail=result.error)
        return assess(result.status, result.headers, result.body, rendered=rendered)

    async def _transport_failure(
        self, crawl: store.Crawl, payload: dict[str, Any], result: FetchResult
    ) -> str:
        assert result.error is not None
        domain = domain_of(payload["url"])
        attempt = payload.get("attempt", 0)
        permanent = result.error == "redirect_loop"
        self.politeness.record(domain, Verdict.SERVER_ERROR)
        if not permanent and attempt + 1 < self.config.max_attempts:
            async with self.sm.begin() as session:
                await store.defer(
                    session,
                    {**payload, "attempt": attempt + 1},
                    store.later(self.config.retry_base * 2**attempt),
                    result.error,
                )
            return "retry"
        outcome = result.error if permanent else f"failed_{result.error}"
        await self._record(crawl, payload["url"], payload["depth"], outcome[:32], attempt, result)
        return outcome

    def _row(
        self,
        crawl: store.Crawl,
        url: str,
        depth: int,
        outcome: str,
        attempt: int,
        result: FetchResult | None = None,
        detail: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        row: dict[str, Any] = {
            "crawl_id": crawl.id,
            "url": url,
            "url_hash": url_hash(url),
            "domain": domain_of(url),
            "depth": depth,
            "outcome": outcome,
            "attempts": attempt + 1,
            "detail": detail,
        }
        if result is not None:
            row.update(
                status_code=result.status or None, final_url=result.final_url, render_mode=result.render_mode
            )
        row.update(extra)
        return row

    async def _record(
        self,
        crawl: store.Crawl,
        url: str,
        depth: int,
        outcome: str,
        attempt: int,
        result: FetchResult | None = None,
        detail: str | None = None,
    ) -> None:
        async with self.sm.begin() as session:
            await store.record_page(session, self._row(crawl, url, depth, outcome, attempt, result, detail))

    async def _store_ok(
        self, crawl: store.Crawl, url: str, depth: int, attempt: int, result: FetchResult
    ) -> str:
        sha = await self.blobs.put_html(result.body)  # content-addressed: safe to repeat
        page = parse(result.body, result.final_url)
        async with self.sm.begin() as session:
            await store.record_page(
                session,
                self._row(
                    crawl,
                    url,
                    depth,
                    "ok",
                    attempt,
                    result,
                    content_sha256=sha,
                    title=page.title,
                    canonical=page.canonical,
                    link_count=len(page.links),
                ),
            )
            if depth < crawl.max_depth:
                for link in page.links:
                    reason = self._skip_reason(link, crawl)
                    if reason is None:
                        reason = await store.accept_url(
                            session, crawl, link, depth + 1, self.config.max_per_pattern
                        )
                    if reason != "accepted":
                        self.stats.skipped_links[reason] += 1
        return "ok"

    async def _handoff_redirect(
        self, crawl: store.Crawl, url: str, depth: int, attempt: int, result: FetchResult
    ) -> str:
        """Off-domain redirect: record it and let the target go through scope and its own pacing."""
        self.politeness.record(domain_of(url), Verdict.OK)
        assert result.redirect_to is not None
        target = normalize(result.redirect_to)
        async with self.sm.begin() as session:
            await store.record_page(
                session, self._row(crawl, url, depth, "redirect", attempt, result, detail=target)
            )
            reason: str | None = self._skip_reason(target, crawl) if target else "uncrawlable"
            if reason is None and target is not None:
                reason = await store.accept_url(session, crawl, target, depth, self.config.max_per_pattern)
            if reason and reason != "accepted":
                self.stats.skipped_links[reason] += 1
        return "redirect"

    @staticmethod
    def _skip_reason(link: str, crawl: store.Crawl) -> str | None:
        if not in_scope(link, crawl.allowed_domains):
            return "out_of_scope"
        return trap_reason(link)
