"""Shared fixtures.

The suite runs entirely in-process: SQLite via aiosqlite stands in for
Postgres and `fakeredis` for Redis, so `pytest` needs no Docker.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import Settings
from app.models import Base
from app.rates import ExchangeRateProvider, RateSource
from app.schemas import TransactionEvent

BASE_TIME = datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def settings() -> Settings:
    """Test settings: no real infrastructure, fast retries."""
    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        redis_url="redis://localhost:6379/15",
        consumer_name="test-consumer",
        stream_name="test-transactions",
        dlq_stream_name="test-transactions.dlq",
        consumer_group="test-processors",
        batch_size=10,
        # fakeredis has no working BLOCK support, so poll instead.
        block_ms=0,
        idle_poll_interval=0.005,
        concurrency=4,
        max_attempts=3,
        retry_base_delay=0.001,
        retry_max_delay=0.005,
        reclaim_idle_ms=10,
        reclaim_interval=0.05,
        metrics_refresh_interval=0.05,
        max_deliveries=3,
        rates_cache_ttl=60,
    )


@pytest_asyncio.fixture
async def engine(tmp_path):
    """A fresh SQLite database per test, on disk rather than in memory.

    An in-memory SQLite database only exists inside one connection, which
    would force every session in a test to share a single connection - and
    concurrent sessions on one connection are exactly what the dedup and
    consumer tests need to be *independent*. A file plus WAL gives each
    session a real connection, the way PostgreSQL does in production.
    """
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path}/test.db",
        # Seconds to wait on SQLite's single-writer lock before giving up.
        connect_args={"timeout": 30},
    )
    async with engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(engine) -> async_sessionmaker:
    return async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


@pytest_asyncio.fixture
async def session(session_factory):
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def redis():
    client = FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        # Close the pool too, or the connection object is finalised later and
        # raises a ResourceWarning that `filterwarnings = error` turns fatal.
        await client.aclose()
        await client.connection_pool.disconnect()


class RecordingRateSource:
    """A `RateSource` that counts calls and can be told to fail."""

    def __init__(
        self,
        rates: dict[str, Decimal] | None = None,
        failures_before_success: int = 0,
        error: Exception | None = None,
    ) -> None:
        self.rates = rates or {"EUR": Decimal("1.0850"), "GBP": Decimal("1.2640")}
        self.calls: list[str] = []
        self._remaining_failures = failures_before_success
        self._error = error

    async def fetch(self, currency: str) -> Decimal:
        self.calls.append(currency)
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise self._error or _default_error(currency)
        try:
            return self.rates[currency]
        except KeyError:
            from app.exceptions import UnknownCurrencyError

            raise UnknownCurrencyError(f"no rate for {currency}") from None


def _default_error(currency: str) -> Exception:
    from app.exceptions import RateUnavailableError

    return RateUnavailableError(f"provider down for {currency}")


@pytest.fixture
def rate_source() -> RecordingRateSource:
    return RecordingRateSource()


@pytest.fixture
def rate_provider(redis, settings, rate_source: RateSource) -> ExchangeRateProvider:
    return ExchangeRateProvider(redis, settings, source=rate_source)


def make_event(
    event_id: str = "evt-1",
    user_id: str = "user-1",
    amount: str = "100.00",
    currency: str = "EUR",
    minutes_offset: int = 0,
) -> TransactionEvent:
    """Build a valid event; every field has a sensible default."""
    return TransactionEvent(
        id=event_id,
        user_id=user_id,
        amount=Decimal(amount),
        currency=currency,
        timestamp=BASE_TIME + timedelta(minutes=minutes_offset),
    )


@pytest.fixture
def event_factory():
    return make_event


async def wait_for(predicate, timeout: float = 3.0, interval: float = 0.02) -> bool:
    """Poll `predicate` until it is true or the timeout expires."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return True
        await asyncio.sleep(interval)
    return False
