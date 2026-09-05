"""SQLAlchemy ORM models."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Index, Numeric, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for every table in the service."""


class Transaction(Base):
    """A processed transaction.

    The primary key is the *event* id supplied by the producer. That single
    constraint is what makes the consumer idempotent: a redelivered event
    collides with the row written by the first delivery and is skipped.
    """

    __tablename__ = "transactions"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(24, 4), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    amount_usd: Mapped[Decimal] = mapped_column(Numeric(24, 4), nullable=False)
    # Rates need more precision than amounts: JPY is 0.0064 USD.
    rate: Mapped[Decimal] = mapped_column(Numeric(24, 10), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        # Serves both `GET /users/{id}/summary` and the time-ranged listing.
        Index("ix_transactions_user_timestamp", "user_id", "timestamp"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<Transaction {self.id} {self.user_id} {self.amount_usd} USD>"
