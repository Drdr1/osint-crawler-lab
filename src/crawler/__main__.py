"""`python -m crawler worker` consumes the frontier; `python -m crawler api` serves FastAPI.

Configuration comes from the environment:
  CRAWLER_DATABASE_URL, KAFKA_BOOTSTRAP, CRAWLER_S3_ENDPOINT, CRAWLER_S3_BUCKET,
  CRAWLER_USER_AGENTS (|-separated), CRAWLER_PROXIES (comma-separated, optional),
  CRAWLER_MIN_DELAY (seconds, default 2).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

import aioboto3

from crawler import store
from crawler.blobs import BlobStore
from crawler.fetch import BrowserFetcher, HttpFetcher
from crawler.identity import IdentityPool
from crawler.politeness import Politeness
from crawler.queue import FRONTIER, KafkaConsumer, KafkaProducer, ensure_topics
from crawler.robots import RobotsCache
from crawler.worker import Worker

DEFAULT_UA = "OSINTLabBot/1.0 (+https://github.com/Drdr1/osint-crawler-lab)"


async def run_worker() -> None:
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    await ensure_topics(bootstrap)
    engine = store.make_engine(os.environ["CRAWLER_DATABASE_URL"])
    user_agents = os.environ.get("CRAWLER_USER_AGENTS", DEFAULT_UA).split("|")
    proxies = [p for p in os.environ.get("CRAWLER_PROXIES", "").split(",") if p]
    http = HttpFetcher()
    session = aioboto3.Session()
    async with session.client("s3", endpoint_url=os.environ.get("CRAWLER_S3_ENDPOINT")) as s3:
        blobs = BlobStore(s3, os.environ.get("CRAWLER_S3_BUCKET", "crawler-pages"))
        await blobs.ensure_bucket()
        worker = Worker(
            store.make_sessionmaker(engine),
            KafkaProducer(bootstrap),
            KafkaConsumer(bootstrap, "crawler", [FRONTIER]),
            blobs,
            http,
            BrowserFetcher(),
            IdentityPool(user_agents, proxies),
            Politeness(min_delay=float(os.environ.get("CRAWLER_MIN_DELAY", 2))),
            RobotsCache(http, agent_token=user_agents[0].split("/")[0]),
        )
        while True:
            await worker.run_once(timeout=1.0)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    if sys.argv[1:] == ["worker"]:
        asyncio.run(run_worker())
    elif sys.argv[1:] == ["api"]:
        import uvicorn

        from crawler.api import create_app

        producer = KafkaProducer(os.environ["KAFKA_BOOTSTRAP"])
        uvicorn.run(create_app(os.environ["CRAWLER_DATABASE_URL"], producer), host="0.0.0.0", port=8000)  # noqa: S104
    else:
        sys.exit("usage: python -m crawler {worker|api}")


if __name__ == "__main__":
    main()
