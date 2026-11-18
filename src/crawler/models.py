"""PostgreSQL schema (SQLAlchemy Core). migrations/versions/0001 creates exactly this."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

crawls = Table(
    "crawls",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("seeds", JSONB, nullable=False),
    Column("allowed_domains", JSONB, nullable=False),
    Column("max_depth", Integer, nullable=False),
    Column("max_pages", Integer, nullable=False),
    Column("enqueued", Integer, nullable=False, server_default="0"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# Every URL ever accepted into a crawl. The primary key is the dedupe guard.
frontier_seen = Table(
    "frontier_seen",
    metadata,
    Column("crawl_id", UUID(as_uuid=True), ForeignKey("crawls.id", ondelete="CASCADE"), nullable=False),
    Column("url_hash", String(64), nullable=False),
    Column("url", Text, nullable=False),
    Column("depth", Integer, nullable=False),
    Column("pattern", Text, nullable=False),  # URL shape with numbers masked, for trap budgets
    PrimaryKeyConstraint("crawl_id", "url_hash"),
    Index("ix_frontier_pattern", "crawl_id", "pattern"),
)

pages = Table(
    "pages",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("crawl_id", UUID(as_uuid=True), ForeignKey("crawls.id", ondelete="CASCADE"), nullable=False),
    Column("url", Text, nullable=False),
    Column("url_hash", String(64), nullable=False),
    Column("domain", String(255), nullable=False),
    Column("depth", Integer, nullable=False),
    Column("outcome", String(32), nullable=False),
    Column("status_code", Integer),
    Column("final_url", Text),
    Column("render_mode", String(16)),
    Column("content_sha256", String(64)),
    Column("title", Text),
    Column("canonical", Text),
    Column("link_count", Integer),
    Column("attempts", Integer, nullable=False, server_default="1"),
    Column("detail", Text),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("crawl_id", "url_hash", name="uq_pages_crawl_url"),
    Index("ix_pages_crawl_outcome", "crawl_id", "outcome"),
    Index("ix_pages_content", "content_sha256"),
)

# Transactional outbox: rows written in the same transaction as the page, then
# published to Kafka. A crash between the two can only cause a re-publish,
# never a lost link.
outbox = Table(
    "outbox",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("topic", String(128), nullable=False),
    Column("key", String(255), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("published", Boolean, nullable=False, server_default="false"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Index("ix_outbox_unpublished", "published", "id"),
)

# Work that must wait (throttled domain, parked domain, transient failure).
deferred = Table(
    "deferred",
    metadata,
    Column("crawl_id", UUID(as_uuid=True), ForeignKey("crawls.id", ondelete="CASCADE"), nullable=False),
    Column("url_hash", String(64), nullable=False),
    Column("domain", String(255), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("not_before", DateTime(timezone=True), nullable=False),
    Column("reason", String(64), nullable=False),
    PrimaryKeyConstraint("crawl_id", "url_hash"),
    Index("ix_deferred_due", "not_before"),
)

# Survives worker restarts, so a parked domain stays parked.
domain_state = Table(
    "domain_state",
    metadata,
    Column("domain", String(255), primary_key=True),
    Column("parked_until", DateTime(timezone=True)),
    Column("park_reason", String(64)),
    Column("crawl_delay", Integer),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
