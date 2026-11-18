"""FastAPI control plane: start crawls, read results, inspect domains."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from crawler import store
from crawler.models import domain_state, pages
from crawler.queue import Producer
from crawler.urls import domain_of, normalize


class CrawlRequest(BaseModel):
    seeds: list[str] = Field(min_length=1, max_length=100)
    allowed_domains: list[str] | None = None  # default: the seeds' own domains
    max_depth: int = Field(default=2, ge=0, le=10)
    max_pages: int = Field(default=200, ge=1, le=100_000)

    @field_validator("seeds")
    @classmethod
    def _valid_seeds(cls, seeds: list[str]) -> list[str]:
        out = []
        for s in seeds:
            n = normalize(s)
            if n is None:
                raise ValueError(f"not a crawlable http(s) URL: {s}")
            out.append(n)
        return out


class CrawlCreated(BaseModel):
    crawl_id: uuid.UUID
    accepted: int


class PageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    url: str
    depth: int
    outcome: str
    status_code: int | None
    render_mode: str | None
    title: str | None
    canonical: str | None
    content_sha256: str | None
    attempts: int
    detail: str | None
    fetched_at: datetime


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sm() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


def create_app(database_url: str, producer: Producer, max_per_pattern: int = 25) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = store.make_engine(database_url)
        app.state.sm = store.make_sessionmaker(engine)
        app.state.producer = producer
        yield
        await engine.dispose()

    app = FastAPI(title="OSINT crawler lab", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz(session: SessionDep) -> dict[str, str]:
        await session.execute(select(1))
        return {"status": "ok"}

    @app.post("/crawls", status_code=202)
    async def create_crawl(req: CrawlRequest, request: Request) -> CrawlCreated:
        allowed = sorted({d.lower() for d in (req.allowed_domains or [domain_of(s) for s in req.seeds])})
        async with request.app.state.sm.begin() as session:
            crawl_id = await store.create_crawl(session, req.seeds, allowed, req.max_depth, req.max_pages)
            crawl = await store.load_crawl(session, crawl_id)
            assert crawl is not None
            results = [await store.accept_url(session, crawl, s, 0, max_per_pattern) for s in req.seeds]
        await store.publish_outbox(request.app.state.sm, request.app.state.producer)
        return CrawlCreated(crawl_id=crawl_id, accepted=results.count("accepted"))

    @app.get("/crawls/{crawl_id}")
    async def crawl_status(crawl_id: uuid.UUID, session: SessionDep) -> dict[str, Any]:
        if await store.load_crawl(session, crawl_id) is None:
            raise HTTPException(404, "crawl not found")
        return await store.crawl_summary(session, crawl_id)

    @app.get("/crawls/{crawl_id}/pages")
    async def crawl_pages(
        crawl_id: uuid.UUID,
        session: SessionDep,
        outcome: str | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    ) -> list[PageOut]:
        stmt = (
            select(pages)
            .where(pages.c.crawl_id == crawl_id)
            .order_by(pages.c.depth, pages.c.url)
            .limit(limit)
        )
        if outcome:
            stmt = stmt.where(pages.c.outcome == outcome)
        rows = (await session.execute(stmt)).mappings().all()
        return [PageOut.model_validate(dict(r)) for r in rows]

    @app.get("/domains")
    async def domains(session: SessionDep) -> list[dict[str, Any]]:
        rows = (await session.execute(select(domain_state).order_by(domain_state.c.domain))).mappings().all()
        return [dict(r) for r in rows]

    return app
