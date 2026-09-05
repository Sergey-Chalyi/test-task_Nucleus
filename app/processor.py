"""The consumer's core: deduplicate, convert to USD, store."""

import enum
import logging
from datetime import UTC, datetime

from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.exceptions import StorageUnavailableError
from app.money import convert_to_usd
from app.rates import ExchangeRateProvider
from app.repository import TransactionRepository
from app.schemas import TransactionEvent

logger = logging.getLogger(__name__)


class Outcome(str, enum.Enum):
    """What happened to one event."""

    STORED = "stored"
    DUPLICATE = "duplicate"


class EventProcessor:
    """Turns one :class:`TransactionEvent` into at most one stored row.

    Deduplication is the database's job: the event id is the primary key and
    the insert carries `ON CONFLICT DO NOTHING`, so a redelivered event is a
    no-op no matter how many workers race on it. There is deliberately *no*
    "have I seen this id?" check in Redis in front of it - a crash between
    marking the id in Redis and committing to Postgres would silently drop
    the event, which is exactly the failure mode the task asks us to avoid.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        rate_provider: ExchangeRateProvider,
    ) -> None:
        self._session_factory = session_factory
        self._rates = rate_provider

    async def process(self, event: TransactionEvent) -> Outcome:
        """Process a single event.

        Raises :class:`~app.exceptions.TransientError` when a dependency is
        unavailable (caller retries) and
        :class:`~app.exceptions.PermanentError` when the event itself is
        unprocessable (caller dead-letters).
        """
        # 1. Convert. Raises RateUnavailableError (retry) or
        #    UnknownCurrencyError (dead-letter).
        rate = await self._rates.get_rate(event.currency)
        amount_usd = convert_to_usd(event.amount, rate)

        # 2. Deduplicate + store in one statement.
        values = {
            "id": event.id,
            "user_id": event.user_id,
            "amount": event.amount,
            "currency": event.currency,
            "amount_usd": amount_usd,
            "rate": rate,
            "timestamp": event.timestamp,
            "processed_at": datetime.now(UTC),
        }
        inserted = await self._store(values)

        if inserted:
            logger.debug("stored %s: %s %s -> %s USD", event.id, event.amount,
                         event.currency, amount_usd)
            return Outcome.STORED

        logger.debug("duplicate %s ignored", event.id)
        return Outcome.DUPLICATE

    async def _store(self, values: dict) -> bool:
        """Write the row, translating DB failures into transient errors."""
        try:
            async with self._session_factory() as session:
                repo = TransactionRepository(session)
                inserted = await repo.insert_if_absent(values)
                await session.commit()
                return inserted
        except IntegrityError:
            # Only reachable on a dialect without ON CONFLICT support: the
            # unique violation *is* the duplicate signal.
            return False
        except (OperationalError, DBAPIError) as exc:
            # Connection dropped, pool exhausted, database restarting, ...
            raise StorageUnavailableError(f"database write failed: {exc}") from exc
        except OSError as exc:
            raise StorageUnavailableError(f"database unreachable: {exc}") from exc
