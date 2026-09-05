"""Tests for the Redis Streams wrapper, driven by `fakeredis`."""

import pytest

from app.queue import EventQueue, QueuedMessage
from tests.conftest import make_event


@pytest.fixture
def queue(redis, settings) -> EventQueue:
    return EventQueue(redis, settings)


class TestPublishAndRead:
    async def test_published_event_round_trips(self, queue):
        await queue.ensure_group()
        event = make_event("evt-1", amount="12.34", currency="GBP")
        await queue.publish(event)

        messages = await queue.read(count=10)
        assert len(messages) == 1
        from app.schemas import TransactionEvent

        assert TransactionEvent.model_validate_json(messages[0].payload) == event

    async def test_ensure_group_is_idempotent(self, queue):
        await queue.ensure_group()
        await queue.ensure_group()  # BUSYGROUP is swallowed

    async def test_group_created_at_zero_sees_earlier_events(self, queue):
        """The API may publish before any worker has ever started."""
        await queue.publish(make_event("evt-early"))
        await queue.ensure_group()
        assert len(await queue.read(count=10)) == 1

    async def test_batch_publish_preserves_order(self, queue):
        await queue.ensure_group()
        events = [make_event(f"evt-{i}") for i in range(5)]
        ids = await queue.publish_many(events)
        assert len(ids) == 5

        messages = await queue.read(count=10)
        assert [m.message_id for m in messages] == ids

    async def test_read_returns_empty_when_idle(self, queue):
        await queue.ensure_group()
        assert await queue.read(count=10) == []

    async def test_acked_messages_are_not_redelivered(self, queue):
        await queue.ensure_group()
        await queue.publish(make_event("evt-1"))
        [message] = await queue.read(count=10)
        assert await queue.ack(message.message_id) == 1
        assert await queue.read(count=10) == []

    async def test_ack_of_nothing_is_a_no_op(self, queue):
        await queue.ensure_group()
        assert await queue.ack() == 0


class TestReclaim:
    async def test_unacked_message_is_reclaimed(self, queue):
        """The at-least-once guarantee: an unacked entry comes back."""
        await queue.ensure_group()
        await queue.publish(make_event("evt-1"))
        [first] = await queue.read(count=10)
        # Deliberately do not ack - simulate a consumer that died.

        claimed, _deleted = await queue.claim_stale(min_idle_ms=0, count=10)
        assert [m.message_id for m in claimed] == [first.message_id]

    async def test_acked_message_is_not_reclaimed(self, queue):
        await queue.ensure_group()
        await queue.publish(make_event("evt-1"))
        [message] = await queue.read(count=10)
        await queue.ack(message.message_id)

        claimed, _deleted = await queue.claim_stale(min_idle_ms=0, count=10)
        assert claimed == []


class TestDeadLetter:
    async def test_dead_letter_moves_and_acks(self, queue, redis, settings):
        await queue.ensure_group()
        await queue.publish(make_event("evt-poison"))
        [message] = await queue.read(count=10)

        await queue.dead_letter(message, reason="unknown_currency")

        # Parked in the DLQ with the diagnostic context...
        entries = await redis.xrange(settings.dlq_stream_name)
        assert len(entries) == 1
        _dlq_id, fields = entries[0]
        assert fields["reason"] == "unknown_currency"
        assert fields["original_id"] == message.message_id
        assert "evt-poison" in fields["payload"]

        # ...and no longer pending on the main stream.
        claimed, _ = await queue.claim_stale(min_idle_ms=0, count=10)
        assert claimed == []


class TestStats:
    async def test_reports_stream_and_dlq_length(self, queue):
        await queue.ensure_group()
        await queue.publish_many([make_event(f"evt-{i}") for i in range(3)])

        stats = await queue.stats()
        assert stats.length == 3
        assert stats.dlq_length == 0

        await queue.read(count=10)
        [message] = (await queue.claim_stale(min_idle_ms=0, count=1))[0][:1]
        await queue.dead_letter(message, reason="test")
        assert (await queue.stats()).dlq_length == 1

    async def test_lag_counts_unread_entries(self, queue):
        await queue.ensure_group()
        await queue.publish_many([make_event(f"evt-{i}") for i in range(4)])
        # Only the direction is asserted: fakeredis' `entries-read` bookkeeping
        # is off by one against real Redis, and the metric is a health signal,
        # not an exact count.
        assert (await queue.stats()).lag > 0

        await queue.read(count=10)
        assert (await queue.stats()).lag == 0


def test_queued_message_defaults_to_first_delivery():
    message = QueuedMessage(message_id="1-0", payload="{}")
    assert message.delivery_count == 1
