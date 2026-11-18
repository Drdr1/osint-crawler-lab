"""Database operations. Every write that matters is one transaction."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from crawler.models import crawls, deferred, domain_state, frontier_seen, outbox, pages
from crawler.queue import FRONTIER, Producer
from crawler.urls import domain_of, pattern_of, url_hash

# Outcomes after which a URL is never fetched again in the same crawl.
RETRYABLE_OUTCOMES = {"captcha"}


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_size=10, max_overflow=5, pool_pre_ping=True)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class Crawl:
    id: uuid.UUID
    allowed_domains: frozenset[str]
    max_depth: int
    max_pages: int


class BudgetExhausted(Exception):
    pass


async def create_crawl(
    session: AsyncSession, seeds: list[str], allowed: list[str], max_depth: int, max_pages: int
) -> uuid.UUID:
    crawl_id = uuid.uuid4()
    await session.execute(
        insert(crawls).values(
            id=crawl_id,
            seeds=seeds,
            allowed_domains=allowed,
            max_depth=max_depth,
            max_pages=max_pages,
        )
    )
    return crawl_id


async def load_crawl(session: AsyncSession, crawl_id: uuid.UUID) -> Crawl | None:
    row = (await session.execute(select(crawls).where(crawls.c.id == crawl_id))).mappings().first()
    if row is None:
        return None
    return Crawl(row["id"], frozenset(row["allowed_domains"]), row["max_depth"], row["max_pages"])


async def accept_url(session: AsyncSession, crawl: Crawl, url: str, depth: int, max_per_pattern: int) -> str:
    """Admit a URL to the frontier once. Returns 'accepted' or why not."""
    pattern = pattern_of(url)
    same_shape = await session.scalar(
        select(func.count())
        .select_from(frontier_seen)
        .where(frontier_seen.c.crawl_id == crawl.id, frontier_seen.c.pattern == pattern)
    )
    if (same_shape or 0) >= max_per_pattern:
        return "pattern_budget"
    try:
        async with session.begin_nested():
            inserted = await session.execute(
                pg_insert(frontier_seen)
                .values(crawl_id=crawl.id, url_hash=url_hash(url), url=url, depth=depth, pattern=pattern)
                .on_conflict_do_nothing()
                .returning(frontier_seen.c.url_hash)
            )
            if inserted.first() is None:
                return "duplicate"
            ok = await session.execute(
                update(crawls)
                .where(crawls.c.id == crawl.id, crawls.c.enqueued < crawls.c.max_pages)
                .values(enqueued=crawls.c.enqueued + 1)
                .returning(crawls.c.id)
            )
            if ok.first() is None:
                raise BudgetExhausted  # rolls back the frontier_seen insert
    except BudgetExhausted:
        return "page_budget"
    await session.execute(
        insert(outbox).values(
            topic=FRONTIER,
            key=domain_of(url),
            payload={"crawl_id": str(crawl.id), "url": url, "depth": depth, "attempt": 0},
        )
    )
    return "accepted"


async def page_is_final(session: AsyncSession, crawl_id: uuid.UUID, uhash: str) -> bool:
    outcome = await session.scalar(
        select(pages.c.outcome).where(pages.c.crawl_id == crawl_id, pages.c.url_hash == uhash)
    )
    return outcome is not None and outcome not in RETRYABLE_OUTCOMES


async def record_page(session: AsyncSession, values: dict[str, Any]) -> None:
    stmt = pg_insert(pages).values(**values)
    cols: dict[str, Any] = {k: stmt.excluded[k] for k in values if k not in ("crawl_id", "url_hash")}
    cols["fetched_at"] = func.now()
    await session.execute(stmt.on_conflict_do_update(constraint="uq_pages_crawl_url", set_=cols))
    await session.execute(
        delete(deferred).where(
            deferred.c.crawl_id == values["crawl_id"], deferred.c.url_hash == values["url_hash"]
        )
    )


async def defer(session: AsyncSession, payload: dict[str, Any], not_before: datetime, reason: str) -> None:
    values = {
        "crawl_id": uuid.UUID(payload["crawl_id"]),
        "url_hash": url_hash(payload["url"]),
        "domain": domain_of(payload["url"]),
        "payload": payload,
        "not_before": not_before,
        "reason": reason,
    }
    stmt = pg_insert(deferred).values(**values)
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=["crawl_id", "url_hash"],
            set_={
                "payload": stmt.excluded.payload,
                "not_before": stmt.excluded.not_before,
                "reason": stmt.excluded.reason,
            },
        )
    )


async def release_due(session: AsyncSession, limit: int = 100) -> int:
    """Move deferred work whose time has come back to the frontier, via the outbox."""
    rows = (
        (
            await session.execute(
                select(deferred)
                .where(deferred.c.not_before <= func.now())
                .order_by(deferred.c.not_before)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        .mappings()
        .all()
    )
    for r in rows:
        await session.execute(insert(outbox).values(topic=FRONTIER, key=r["domain"], payload=r["payload"]))
        await session.execute(
            delete(deferred).where(deferred.c.crawl_id == r["crawl_id"], deferred.c.url_hash == r["url_hash"])
        )
    return len(rows)


async def publish_outbox(sm: async_sessionmaker[AsyncSession], producer: Producer, limit: int = 500) -> int:
    async with sm.begin() as session:
        rows = (
            (
                await session.execute(
                    select(outbox)
                    .where(outbox.c.published.is_(False))
                    .order_by(outbox.c.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            )
            .mappings()
            .all()
        )
        for r in rows:
            await producer.send(r["topic"], r["key"], r["payload"])
        if rows:
            await session.execute(
                update(outbox).where(outbox.c.id.in_([r["id"] for r in rows])).values(published=True)
            )
    return len(rows)


async def park_domain(session: AsyncSession, domain: str, until: datetime, reason: str) -> None:
    stmt = pg_insert(domain_state).values(domain=domain, parked_until=until, park_reason=reason)
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=["domain"],
            set_={
                "parked_until": stmt.excluded.parked_until,
                "park_reason": stmt.excluded.park_reason,
                "updated_at": func.now(),
            },
        )
    )


async def parked_until(session: AsyncSession, domain: str) -> datetime | None:
    until = await session.scalar(select(domain_state.c.parked_until).where(domain_state.c.domain == domain))
    return until if until and until > utcnow() else None


async def crawl_summary(session: AsyncSession, crawl_id: uuid.UUID) -> dict[str, Any]:
    outcomes: dict[str, int] = dict(
        (
            await session.execute(
                select(pages.c.outcome, func.count())
                .where(pages.c.crawl_id == crawl_id)
                .group_by(pages.c.outcome)
            )
        )
        .tuples()
        .all()
    )
    waiting = await session.scalar(
        select(func.count()).select_from(deferred).where(deferred.c.crawl_id == crawl_id)
    )
    unique_content = await session.scalar(
        select(func.count(func.distinct(pages.c.content_sha256))).where(
            pages.c.crawl_id == crawl_id, pages.c.content_sha256.is_not(None)
        )
    )
    enqueued = await session.scalar(select(crawls.c.enqueued).where(crawls.c.id == crawl_id))
    return {
        "enqueued": enqueued or 0,
        "outcomes": outcomes,
        "deferred": waiting or 0,
        "unique_content": unique_content or 0,
    }


async def deferred_count(session: AsyncSession) -> int:
    return int(await session.scalar(select(func.count()).select_from(deferred)) or 0)


def later(seconds: float) -> datetime:
    return utcnow() + timedelta(seconds=seconds)
