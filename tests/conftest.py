"""Real PostgreSQL (built by the Alembic migration), S3 via MinIO in CI or moto locally,
real Chromium, and the hostile target site. Kafka is used when KAFKA_BOOTSTRAP is set."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any

import aioboto3
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from crawler import store
from crawler.blobs import BlobStore
from crawler.fetch import BrowserFetcher, HttpFetcher
from crawler.identity import IdentityPool
from crawler.politeness import Politeness
from crawler.queue import FRONTIER, InMemoryBroker
from crawler.robots import RobotsCache
from crawler.worker import Worker, WorkerConfig
from target_site import TargetSite

DATABASE_URL = os.environ.get(
    "CRAWLER_TEST_DATABASE_URL", "postgresql+asyncpg://postgres@127.0.0.1:55432/crawl"
)
S3_ENDPOINT = os.environ.get("CRAWLER_TEST_S3_ENDPOINT")  # MinIO in CI
UA = "OSINTLabBot/1.0 (+https://github.com/Drdr1/osint-crawler-lab)"

os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="session")
def migrated_db() -> str:
    env = {**os.environ, "CRAWLER_DATABASE_URL": DATABASE_URL}
    root = os.path.dirname(os.path.dirname(__file__))
    for args in (["downgrade", "base"], ["upgrade", "head"]):
        subprocess.run([sys.executable, "-m", "alembic", *args], cwd=root, env=env, check=True)  # noqa: S603
    return DATABASE_URL


@pytest_asyncio.fixture(scope="session")
async def engine(migrated_db: str) -> AsyncIterator[AsyncEngine]:
    eng = store.make_engine(migrated_db)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def sm(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    async with engine.begin() as conn:
        await conn.execute(
            text("TRUNCATE outbox, deferred, pages, frontier_seen, domain_state, crawls CASCADE")
        )
    return store.make_sessionmaker(engine)


@pytest.fixture(scope="session")
def s3_endpoint() -> Iterator[str]:
    if S3_ENDPOINT:
        yield S3_ENDPOINT
        return
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=5066, verbose=False)
    server.start()
    yield "http://127.0.0.1:5066"
    server.stop()


@pytest_asyncio.fixture
async def blobs(s3_endpoint: str) -> AsyncIterator[BlobStore]:
    session = aioboto3.Session()
    async with session.client("s3", endpoint_url=s3_endpoint, region_name="us-east-1") as s3:
        store_ = BlobStore(s3, f"pages-{uuid.uuid4().hex[:8]}")
        await store_.ensure_bucket()
        yield store_


@pytest.fixture
def site() -> Iterator[TargetSite]:
    s = TargetSite()
    s.start()
    yield s
    s.stop()


@dataclass
class Rig:
    worker: Worker
    sm: async_sessionmaker[AsyncSession]
    blobs: BlobStore
    site: TargetSite
    producer: Any
    make_consumer: Any

    async def start(self, max_depth: int = 3, max_pages: int = 100) -> uuid.UUID:
        allowed = [self.site.main_host, self.site.other_host]
        async with self.sm.begin() as session:
            crawl_id = await store.create_crawl(
                session, [self.site.base + "/"], allowed, max_depth, max_pages
            )
            crawl = await store.load_crawl(session, crawl_id)
            assert crawl is not None
            await store.accept_url(
                session, crawl, self.site.base + "/", 0, self.worker.config.max_per_pattern
            )
        await store.publish_outbox(self.sm, self.producer)
        return crawl_id


def build_worker(
    sm: async_sessionmaker[AsyncSession], blobs: BlobStore, producer: Any, consumer: Any, **politeness: Any
) -> Worker:
    http = HttpFetcher(timeout=1.0)
    return Worker(
        sm,
        producer,
        consumer,
        blobs,
        http,
        BrowserFetcher(timeout=10.0),
        IdentityPool([UA]),
        Politeness(
            **{"min_delay": 0.3, "max_backoff": 2.0, "park_seconds": 2.0, "jitter": 0.0, **politeness}
        ),
        RobotsCache(http, agent_token="OSINTLabBot"),
        WorkerConfig(max_inline_wait=1.0, max_attempts=3, retry_base=0.5, max_per_pattern=5),
    )


@pytest_asyncio.fixture
async def rig(sm: async_sessionmaker[AsyncSession], blobs: BlobStore, site: TargetSite) -> AsyncIterator[Rig]:
    broker = InMemoryBroker()
    producer = broker.producer()

    def make_consumer() -> Any:
        return broker.consumer("crawler", [FRONTIER])

    worker = build_worker(sm, blobs, producer, make_consumer())
    yield Rig(worker, sm, blobs, site, producer, make_consumer)
    await worker.http.close()
    await worker.browser.close()
