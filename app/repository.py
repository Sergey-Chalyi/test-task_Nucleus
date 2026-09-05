"""All SQL the service issues, in one place."""

from datetime import datetime
from decimal import Decimal
from typing import Any, NamedTuple

from sqlalchemy import Select, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Transaction
from app.money import quantize_usd


class SummaryRow(NamedTuple):
    """Result of the per-user aggregate query."""

    total_usd: Decimal
    transaction_count: int


def _insert_ignore_duplicates(dialect: str, values: dict[str, Any]):
    """Build an `INSERT ... ON CONFLICT (id) DO NOTHING` for this dialect.

    Both PostgreSQL (production) and SQLite (unit tests) support the clause
    but expose it through their own dialect-specific construct, hence the
    dispatch. Any other dialect falls back to a plain INSERT, which raises
    an IntegrityError on a duplicate - still correct, just less pleasant.
    """
    if dialect == "postgresql":
        return pg_insert(Transaction).values(**values).on_conflict_do_nothing(
            index_elements=["id"]
        )
    if dialect == "sqlite":
        return sqlite_insert(Transaction).values(**values).on_conflict_do_nothing(
            index_elements=["id"]
        )
    return insert(Transaction).values(**values)


class TransactionRepository:
    """Data access for the `transactions` table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, values: dict[str, Any]) -> bool:
        """Insert a transaction, ignoring it if the id is already stored.

        Returns True when this call wrote the row and False when the id was
        already present, i.e. the event is a duplicate. The uniqueness check
        and the write are the *same* statement, so two workers racing on the
        same event cannot both report a successful insert.
        """
        dialect = self._session.bind.dialect.name  # type: ignore[union-attr]
        result = await self._session.execute(_insert_ignore_duplicates(dialect, values))
        # rowcount is 1 when the row went in, 0 when the conflict clause
        # swallowed it.
        return result.rowcount == 1

    async def get_summary(self, user_id: str) -> SummaryRow:
        """Total USD and transaction count for one user."""
        stmt = select(
            func.coalesce(func.sum(Transaction.amount_usd), 0),
            func.count(Transaction.id),
        ).where(Transaction.user_id == user_id)
        total, count = (await self._session.execute(stmt)).one()
        # COALESCE over a NUMERIC(24,8) column yields things like 0E-8;
        # normalise so the API always answers with the same scale.
        return SummaryRow(
            total_usd=quantize_usd(Decimal(total)), transaction_count=int(count)
        )

    def _range_filter(
        self,
        stmt: Select,
        user_id: str,
        date_from: datetime | None,
        date_to: datetime | None,
    ) -> Select:
        stmt = stmt.where(Transaction.user_id == user_id)
        if date_from is not None:
            stmt = stmt.where(Transaction.timestamp >= date_from)
        if date_to is not None:
            # Inclusive upper bound: `to=...T23:59:59Z` behaves as a user expects.
            stmt = stmt.where(Transaction.timestamp <= date_to)
        return stmt

    async def count_transactions(
        self, user_id: str, date_from: datetime | None, date_to: datetime | None
    ) -> int:
        stmt = self._range_filter(
            select(func.count(Transaction.id)), user_id, date_from, date_to
        )
        return int((await self._session.execute(stmt)).scalar_one())

    async def list_transactions(
        self,
        user_id: str,
        date_from: datetime | None,
        date_to: datetime | None,
        limit: int,
        offset: int,
    ) -> list[Transaction]:
        """One page of a user's transactions, newest first.

        `id` is the tie-breaker so rows with identical timestamps keep a
        stable order across pages.
        """
        stmt = self._range_filter(select(Transaction), user_id, date_from, date_to)
        stmt = (
            stmt.order_by(Transaction.timestamp.desc(), Transaction.id.desc())
            .limit(limit)
            .offset(offset)
        )
        return list((await self._session.execute(stmt)).scalars().all())
