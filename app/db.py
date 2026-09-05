"""Async database engine, session factory and schema bootstrap."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings
from app.models import Base

logger = logging.getLogger(__name__)

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Return the process-wide engine, creating it on first use."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.database_url,
            pool_size=settings.db_pool_size or max(settings.concurrency, 10),
            max_overflow=settings.db_max_overflow,
            pool_pre_ping=True,  # drop connections killed by a DB restart
            future=True,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the process-wide session factory."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=get_engine(), expire_on_commit=False, autoflush=False
        )
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """Yield a session and always close it afterwards."""
    async with get_session_factory()() as session:
        yield session


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency returning a request-scoped session."""
    async with session_scope() as session:
        yield session


async def init_db() -> None:
    """Create tables if they are missing.

    Deliberately `create_all` rather than Alembic: the schema is a single
    table and this keeps `docker compose up` a one-step operation. A real
    deployment would run migrations instead (see README trade-offs).
    """
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("database schema ready")


async def dispose_engine() -> None:
    """Close all pooled connections (called on shutdown)."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
