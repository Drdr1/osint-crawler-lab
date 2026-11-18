"""Alembic env: async engine, URL from CRAWLER_DATABASE_URL."""

from __future__ import annotations

import asyncio
import os

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from crawler.models import metadata

target_metadata = metadata


def _run(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def _main() -> None:
    engine = create_async_engine(os.environ["CRAWLER_DATABASE_URL"])
    async with engine.connect() as conn:
        await conn.run_sync(_run)
        await conn.commit()
    await engine.dispose()


asyncio.run(_main())
