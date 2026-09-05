"""Unit tests for deduplication (task requirement).

Dedup is enforced by the primary key on `transactions.id` plus
`ON CONFLICT DO NOTHING`, so these tests drive a real database (SQLite in
process, PostgreSQL in production - both support the clause).
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.models import Transaction
from app.processor import EventProcessor, Outcome
from app.repository import TransactionRepository
from tests.conftest import make_event


def _values(event_id: str = "evt-1", user_id: str = "user-1", amount: str = "100.00"):
    return {
        "id": event_id,
        "user_id": user_id,
        "amount": Decimal(amount),
        "currency": "EUR",
        "amount_usd": Decimal(amount) * Decimal("1.0850"),
        "rate": Decimal("1.0850"),
        "timestamp": datetime(2026, 9, 1, 12, 0, tzinfo=UTC),
        "processed_at": datetime.now(UTC),
    }


async def _count(session_factory) -> int:
    async with session_factory() as session:
        return int((await session.execute(select(func.count(Transaction.id)))).scalar_one())


class TestRepositoryDedup:
    """The single statement that does the deduplicating."""

    async def test_first_insert_wins(self, session):
        repo = TransactionRepository(session)
        assert await repo.insert_if_absent(_values()) is True
        await session.commit()

    async def test_second_insert_of_same_id_is_a_no_op(self, session):
        repo = TransactionRepository(session)
        assert await repo.insert_if_absent(_values()) is True
        await session.commit()
        assert await repo.insert_if_absent(_values()) is False
        await session.commit()

    async def test_different_ids_are_both_stored(self, session, session_factory):
        repo = TransactionRepository(session)
        assert await repo.insert_if_absent(_values("evt-1")) is True
        assert await repo.insert_if_absent(_values("evt-2")) is True
        await session.commit()
        assert await _count(session_factory) == 2

    async def test_duplicate_does_not_overwrite_the_stored_row(self, session):
        """First write wins: a redelivery must not mutate stored history."""
        repo = TransactionRepository(session)
        await repo.insert_if_absent(_values(amount="100.00"))
        await session.commit()

        # Same id, different amount - e.g. a producer bug or a replayed edit.
        await repo.insert_if_absent(_values(amount="999.00"))
        await session.commit()

        stored = (await session.execute(select(Transaction))).scalar_one()
        assert stored.amount == Decimal("100.0000")


class TestProcessorDedup:
    """Dedup as the consumer sees it: an outcome per event."""

    @pytest.fixture
    def processor(self, session_factory, rate_provider) -> EventProcessor:
        return EventProcessor(session_factory, rate_provider)

    async def test_new_event_is_stored(self, processor):
        assert await processor.process(make_event("evt-1")) is Outcome.STORED

    async def test_redelivered_event_is_reported_as_duplicate(self, processor):
        event = make_event("evt-1")
        assert await processor.process(event) is Outcome.STORED
        assert await processor.process(event) is Outcome.DUPLICATE
        assert await processor.process(event) is Outcome.DUPLICATE

    async def test_duplicate_leaves_exactly_one_row(self, processor, session_factory):
        event = make_event("evt-1")
        for _ in range(5):
            await processor.process(event)
        assert await _count(session_factory) == 1

    async def test_duplicate_does_not_inflate_the_user_total(
        self, processor, session_factory
    ):
        """The point of dedup: replays must not double-count money."""
        await processor.process(make_event("evt-1", amount="100.00"))
        await processor.process(make_event("evt-1", amount="100.00"))
        await processor.process(make_event("evt-2", amount="50.00"))

        async with session_factory() as session:
            summary = await TransactionRepository(session).get_summary("user-1")
        assert summary.transaction_count == 2
        # (100 + 50) * 1.085
        assert summary.total_usd == Decimal("162.7500")

    async def test_concurrent_deliveries_of_one_event_store_one_row(
        self, processor, session_factory
    ):
        """Two workers racing on the same redelivered event.

        Only one of them may report STORED; the database decides, not us.
        """
        event = make_event("evt-race")
        outcomes = await asyncio.gather(*(processor.process(event) for _ in range(8)))

        assert outcomes.count(Outcome.STORED) == 1
        assert outcomes.count(Outcome.DUPLICATE) == 7
        assert await _count(session_factory) == 1

    async def test_distinct_events_are_all_stored(self, processor, session_factory):
        events = [make_event(f"evt-{i}", minutes_offset=i) for i in range(20)]
        outcomes = await asyncio.gather(*(processor.process(e) for e in events))
        assert all(o is Outcome.STORED for o in outcomes)
        assert await _count(session_factory) == 20

    async def test_stored_row_carries_the_converted_amount(
        self, processor, session_factory
    ):
        await processor.process(make_event("evt-1", amount="100.00", currency="EUR"))
        async with session_factory() as session:
            row = (await session.execute(select(Transaction))).scalar_one()
        assert row.currency == "EUR"
        assert row.rate == Decimal("1.0850000000")
        assert row.amount_usd == Decimal("108.5000")


@pytest.mark.parametrize("amount", ["999999.1234", "0.0001", "-4321.5678"])
async def test_amounts_round_trip_through_the_database(session_factory, amount):
    """Guards the SQLite Decimal-via-float warning we silence in pyproject."""
    values = _values(amount=amount)
    async with session_factory() as session:
        await TransactionRepository(session).insert_if_absent(values)
        await session.commit()
    async with session_factory() as session:
        row = (await session.execute(select(Transaction))).scalar_one()
    assert row.amount == Decimal(amount)
