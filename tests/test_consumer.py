"""End-to-end tests of the consumer loop against fakeredis + SQLite.

These cover the failure handling the task asks about: nothing is acked
before it is committed, transient failures stay pending for redelivery, and
poison messages end up in the dead-letter stream instead of blocking the
queue.
"""

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.consumer import EventConsumer
from app.exceptions import RateUnavailableError, StorageUnavailableError
from app.models import Transaction
from app.processor import EventProcessor, Outcome
from app.queue import EventQueue
from app.rates import ExchangeRateProvider
from tests.conftest import RecordingRateSource, make_event, wait_for


@pytest.fixture
def queue(redis, settings) -> EventQueue:
    return EventQueue(redis, settings)


@pytest.fixture
def processor(session_factory, rate_provider) -> EventProcessor:
    return EventProcessor(session_factory, rate_provider)


@pytest.fixture
def consumer(queue, processor, settings) -> EventConsumer:
    return EventConsumer(queue, processor, settings)


async def stored_count(session_factory) -> int:
    async with session_factory() as session:
        return int((await session.execute(select(func.count(Transaction.id)))).scalar_one())


async def run_until(consumer: EventConsumer, condition, timeout: float = 3.0) -> bool:
    """Run the consumer in the background until `condition` holds."""
    task = asyncio.create_task(consumer.run())
    try:
        return await wait_for(condition, timeout=timeout)
    finally:
        consumer.request_stop()
        await asyncio.wait_for(task, timeout=5.0)


class TestHappyPath:
    async def test_published_events_are_converted_and_stored(
        self, consumer, queue, session_factory
    ):
        await queue.ensure_group()
        await queue.publish_many(
            [make_event(f"evt-{i}", amount="10.00", minutes_offset=i) for i in range(5)]
        )

        assert await run_until(consumer, lambda: stored_count(session_factory))
        assert await wait_for(lambda: stored_count(session_factory), timeout=0.1)
        assert await stored_count(session_factory) == 5

        async with session_factory() as session:
            row = (await session.execute(select(Transaction).limit(1))).scalar_one()
        assert row.amount_usd == Decimal("10.8500")

    async def test_everything_is_acknowledged(self, consumer, queue, session_factory):
        await queue.ensure_group()
        await queue.publish_many([make_event(f"evt-{i}") for i in range(3)])

        async def all_stored():
            return await stored_count(session_factory) == 3

        assert await run_until(consumer, all_stored)
        # Nothing left in the pending list means nothing will be redelivered.
        claimed, _ = await queue.claim_stale(min_idle_ms=0, count=10)
        assert claimed == []

    async def test_redelivery_does_not_double_count(
        self, consumer, queue, session_factory
    ):
        await queue.ensure_group()
        event = make_event("evt-same")
        # The same event published three times, as an at-least-once queue does.
        await queue.publish_many([event, event, event])

        async def one_row():
            return await stored_count(session_factory) == 1

        assert await run_until(consumer, one_row)
        assert await stored_count(session_factory) == 1


class TestPoisonMessages:
    async def test_unparsable_payload_is_dead_lettered(
        self, consumer, queue, redis, settings
    ):
        await queue.ensure_group()
        await redis.xadd(settings.stream_name, {"payload": "{not json"})

        async def in_dlq():
            return await redis.xlen(settings.dlq_stream_name) == 1

        assert await run_until(consumer, in_dlq)

        _id, fields = (await redis.xrange(settings.dlq_stream_name))[0]
        assert "unparsable payload" in fields["reason"]

    async def test_unknown_currency_is_dead_lettered_not_retried(
        self, queue, redis, settings, session_factory, rate_source
    ):
        provider = ExchangeRateProvider(redis, settings, source=rate_source)
        consumer = EventConsumer(
            queue, EventProcessor(session_factory, provider), settings
        )
        await queue.ensure_group()
        await queue.publish(make_event("evt-xyz", currency="XYZ"))

        async def in_dlq():
            return await redis.xlen(settings.dlq_stream_name) == 1

        assert await run_until(consumer, in_dlq)
        # One lookup only - a permanent error must not burn retries.
        assert rate_source.calls == ["XYZ"]
        assert await stored_count(session_factory) == 0

    async def test_a_poison_message_does_not_block_the_queue(
        self, consumer, queue, redis, settings, session_factory
    ):
        await queue.ensure_group()
        await redis.xadd(settings.stream_name, {"payload": "{broken"})
        await queue.publish(make_event("evt-good"))

        async def good_one_stored():
            return await stored_count(session_factory) == 1

        assert await run_until(consumer, good_one_stored)
        assert await redis.xlen(settings.dlq_stream_name) == 1


class TestTransientFailures:
    async def test_event_survives_a_rate_outage(
        self, queue, redis, settings, session_factory
    ):
        """The provider fails twice, then recovers: nothing is lost."""
        source = RecordingRateSource(failures_before_success=2)
        provider = ExchangeRateProvider(redis, settings, source=source)
        consumer = EventConsumer(
            queue, EventProcessor(session_factory, provider), settings
        )
        await queue.ensure_group()
        await queue.publish(make_event("evt-1"))

        async def stored():
            return await stored_count(session_factory) == 1

        assert await run_until(consumer, stored)
        assert len(source.calls) == 3  # two failures, one success

    async def test_event_stays_pending_when_retries_are_exhausted(
        self, queue, redis, settings, session_factory
    ):
        """Not acked means not lost: the message is still claimable."""
        always_down = RecordingRateSource(failures_before_success=10_000)
        provider = ExchangeRateProvider(redis, settings, source=always_down)
        consumer = EventConsumer(
            queue, EventProcessor(session_factory, provider), settings
        )
        await queue.ensure_group()
        await queue.publish(make_event("evt-1"))

        async def attempts_exhausted():
            return len(always_down.calls) >= settings.max_attempts

        assert await run_until(consumer, attempts_exhausted)
        assert await stored_count(session_factory) == 0

        claimed, _ = await queue.claim_stale(min_idle_ms=0, count=10)
        assert len(claimed) == 1
        assert "evt-1" in claimed[0].payload

    async def test_database_outage_leaves_the_event_pending(
        self, queue, redis, settings, session_factory, rate_provider
    ):
        class BrokenProcessor(EventProcessor):
            async def process(self, event):
                raise StorageUnavailableError("database is down")

        consumer = EventConsumer(
            queue, BrokenProcessor(session_factory, rate_provider), settings
        )
        await queue.ensure_group()
        await queue.publish(make_event("evt-1"))
        await asyncio.sleep(0)

        task = asyncio.create_task(consumer.run())
        await asyncio.sleep(0.4)
        consumer.request_stop()
        await asyncio.wait_for(task, timeout=5.0)

        claimed, _ = await queue.claim_stale(min_idle_ms=0, count=10)
        assert len(claimed) == 1

    async def test_reclaimed_message_is_processed_by_another_consumer(
        self, queue, redis, settings, session_factory, rate_provider
    ):
        """A message a dead worker never acked is picked up and completed."""
        await queue.ensure_group()
        await queue.publish(make_event("evt-orphan"))

        # Consumer A reads it and "dies" without acking.
        dead = EventQueue(redis, settings)
        read = await dead.read(count=10)
        assert len(read) == 1

        consumer = EventConsumer(
            queue, EventProcessor(session_factory, rate_provider), settings
        )

        async def stored():
            return await stored_count(session_factory) == 1

        assert await run_until(consumer, stored)


class TestProcessorErrorTranslation:
    async def test_rate_errors_propagate_as_transient(
        self, session_factory, redis, settings
    ):
        provider = ExchangeRateProvider(
            redis, settings, source=RecordingRateSource(failures_before_success=1)
        )
        processor = EventProcessor(session_factory, provider)
        with pytest.raises(RateUnavailableError):
            await processor.process(make_event("evt-1"))

    async def test_broken_engine_becomes_storage_unavailable(self, rate_provider):
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        # A database that does not exist: connecting raises, and the processor
        # must classify that as retryable rather than crash the worker.
        engine = create_async_engine("sqlite+aiosqlite:////nonexistent/dir/db.sqlite")
        processor = EventProcessor(
            async_sessionmaker(bind=engine, expire_on_commit=False), rate_provider
        )
        with pytest.raises(StorageUnavailableError):
            await processor.process(make_event("evt-1"))
        await engine.dispose()

    async def test_outcome_values_match_the_metric_labels(self):
        assert Outcome.STORED.value == "stored"
        assert Outcome.DUPLICATE.value == "duplicate"
