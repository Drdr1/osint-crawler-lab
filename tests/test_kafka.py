"""The same guarantees on a real Kafka broker. Runs when KAFKA_BOOTSTRAP is set (CI)."""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import Rig, build_worker
from crawler.blobs import BlobStore
from crawler.models import pages
from crawler.queue import FRONTIER, KafkaConsumer, KafkaProducer, ensure_topics, partition_for
from target_site import TargetSite

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_BOOTSTRAP not set")


@pytest_asyncio.fixture
async def kafka_rig(
    sm: async_sessionmaker[AsyncSession], blobs: BlobStore, site: TargetSite
) -> AsyncIterator[Rig]:
    assert BOOTSTRAP
    await ensure_topics(BOOTSTRAP)
    group = f"crawler-{uuid.uuid4().hex[:8]}"
    producer = KafkaProducer(BOOTSTRAP)
    consumers: list[KafkaConsumer] = []

    def make_consumer() -> Any:
        c = KafkaConsumer(BOOTSTRAP, group, [FRONTIER])
        consumers.append(c)
        return c

    worker = build_worker(sm, blobs, producer, make_consumer())
    yield Rig(worker, sm, blobs, site, producer, make_consumer)
    for c in consumers:
        await c.close()
    await producer.close()
    await worker.http.close()
    await worker.browser.close()


async def test_uncommitted_messages_are_redelivered_to_a_new_consumer() -> None:
    assert BOOTSTRAP
    await ensure_topics(BOOTSTRAP)
    topic_key = f"redelivery-{uuid.uuid4().hex[:6]}.test"
    group = f"g-{uuid.uuid4().hex[:8]}"
    producer = KafkaProducer(BOOTSTRAP)
    for i in range(3):
        await producer.send(FRONTIER, topic_key, {"n": i, "tag": topic_key})
    await producer.close()

    async def drain(consumer: KafkaConsumer) -> list[Any]:
        got: list[Any] = []
        for _ in range(20):
            got += [m for m in await consumer.poll(timeout=1.0) if m.value.get("tag") == topic_key]
            if len(got) >= 3:
                break
        return got

    first = KafkaConsumer(BOOTSTRAP, group, [FRONTIER])
    seen = await drain(first)
    assert [m.value["n"] for m in seen] == [0, 1, 2]  # same key -> same partition -> in order
    assert {m.partition for m in seen} == {partition_for(topic_key, 4)} or len(
        {m.partition for m in seen}
    ) == 1
    await first.close()  # closed without committing

    second = KafkaConsumer(BOOTSTRAP, group, [FRONTIER])
    again = await drain(second)
    assert [m.value["n"] for m in again] == [0, 1, 2]
    await second.commit(again)
    await second.close()

    third = KafkaConsumer(BOOTSTRAP, group, [FRONTIER])
    leftovers = [m for m in await third.poll(timeout=2.0) if m.value.get("tag") == topic_key]
    assert leftovers == []  # committed means done
    await third.close()


async def test_crawl_through_real_kafka(kafka_rig: Rig) -> None:
    crawl_id = await kafka_rig.start(max_depth=1, max_pages=30)
    await kafka_rig.worker.run_until_idle(idle_rounds=8, deadline=120)
    async with kafka_rig.sm() as session:
        rows = (
            await session.execute(select(pages.c.url, pages.c.outcome).where(pages.c.crawl_id == crawl_id))
        ).all()
    o = {u.replace(kafka_rig.site.base, ""): out for u, out in rows}
    assert o["/"] == "ok" and o["/static/1"] == "ok"
    assert o["/js-only"] == "ok" and o["/flaky"] == "ok"
    assert o["/private/secret"] == "robots_disallowed"
    assert kafka_rig.site.log.count("/private/secret") == 0
    assert kafka_rig.site.log.count("/static/1", kafka_rig.site.main_host) == 1
