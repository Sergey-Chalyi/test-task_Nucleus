"""Pydantic models for the HTTP layer (request bodies and responses)."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Amounts are money: always Decimal, never float. Four decimal places is the
# scale the whole system uses - the API accepts it, the columns store it and
# the responses return it - so nothing is ever silently rounded on the way in.
MoneyAmount = Annotated[Decimal, Field(max_digits=20, decimal_places=4)]


class TransactionEvent(BaseModel):
    """An incoming transaction event, exactly as described in the task."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "id": "evt-0001",
                "user_id": "user-42",
                "amount": "125.50",
                "currency": "EUR",
                "timestamp": "2026-09-05T10:15:00Z",
            }
        },
    )

    id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=128)
    amount: MoneyAmount
    currency: str = Field(min_length=3, max_length=3)
    timestamp: datetime

    @field_validator("currency")
    @classmethod
    def _normalise_currency(cls, value: str) -> str:
        """ISO-4217 codes are upper case; accept lower case from clients."""
        code = value.strip().upper()
        if not code.isalpha():
            raise ValueError("currency must be a 3-letter ISO-4217 code")
        return code

    @field_validator("amount")
    @classmethod
    def _finite_amount(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("amount must be a finite number")
        return value

    @field_validator("timestamp")
    @classmethod
    def _to_utc(cls, value: datetime) -> datetime:
        """Store everything in UTC; treat a naive timestamp as UTC."""
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class EventAccepted(BaseModel):
    """Response of a successful enqueue."""

    id: str
    status: str = "queued"
    message_id: str = Field(description="Redis stream entry id of the queued event")


class BatchAccepted(BaseModel):
    """Response of a successful batch enqueue."""

    accepted: int
    status: str = "queued"
    message_ids: list[str]


class UserSummary(BaseModel):
    """Aggregated view over every stored transaction of one user."""

    user_id: str
    total_usd: Decimal
    transaction_count: int


class TransactionOut(BaseModel):
    """One stored transaction as returned by the list endpoint."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    amount: Decimal
    currency: str
    amount_usd: Decimal
    rate: Decimal
    timestamp: datetime


class TransactionPage(BaseModel):
    """Offset-paginated slice of a user's transactions."""

    items: list[TransactionOut]
    total: int
    limit: int
    offset: int
    has_more: bool


class HealthResponse(BaseModel):
    """Readiness of the process and its two dependencies."""

    status: str
    database: str
    redis: str
