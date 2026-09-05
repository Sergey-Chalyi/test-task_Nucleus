"""Currency conversion: the exchange-rate lookup and the pure math around it.

`ExchangeRateProvider` sits in front of a *downstream* rate source. The
source is deliberately pluggable so the whole thing can be unit-tested (and
made to fail on demand) without a network.
"""

import asyncio
import logging
import random
from decimal import Decimal, DecimalException
from typing import Protocol

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import Settings
from app.exceptions import RateUnavailableError, UnknownCurrencyError
from app.metrics import RATE_LOOKUPS

logger = logging.getLogger(__name__)

BASE_CURRENCY = "USD"

# Units of USD per 1 unit of the key currency. A real deployment would read
# these from an FX API; the shape of the call (async, fallible) is the same.
STATIC_RATES: dict[str, Decimal] = {
    "USD": Decimal("1"),
    "EUR": Decimal("1.0850"),
    "GBP": Decimal("1.2640"),
    "JPY": Decimal("0.0064"),
    "CHF": Decimal("1.1180"),
    "CAD": Decimal("0.7350"),
    "AUD": Decimal("0.6620"),
    "SEK": Decimal("0.0945"),
    "PLN": Decimal("0.2510"),
    "UAH": Decimal("0.0242"),
}


class RateSource(Protocol):
    """The downstream dependency: given a currency, return USD per unit."""

    async def fetch(self, currency: str) -> Decimal: ...


class StaticRateSource:
    """In-process stand-in for a real FX API.

    `failure_rate` and `latency` exist so the retry/backoff path can be
    exercised locally (`RATES_FAILURE_RATE=0.3 docker compose up`).
    """

    def __init__(self, failure_rate: float = 0.0, latency: float = 0.0) -> None:
        self._failure_rate = failure_rate
        self._latency = latency

    async def fetch(self, currency: str) -> Decimal:
        if self._latency:
            await asyncio.sleep(self._latency)
        if self._failure_rate and random.random() < self._failure_rate:
            raise RateUnavailableError(
                f"simulated rate-provider outage for {currency}"
            )
        try:
            return STATIC_RATES[currency]
        except KeyError:
            raise UnknownCurrencyError(f"no exchange rate for {currency}") from None


class ExchangeRateProvider:
    """Cache-aside wrapper around a :class:`RateSource`.

    Redis is a *cache*, not the source of truth: if it is down we log and go
    straight to the source rather than failing the event.
    """

    def __init__(
        self,
        redis: Redis | None,
        settings: Settings,
        source: RateSource | None = None,
    ) -> None:
        self._redis = redis
        self._ttl = settings.rates_cache_ttl
        self._source = source or StaticRateSource(
            failure_rate=settings.rates_failure_rate,
            latency=settings.rates_latency,
        )

    @staticmethod
    def _cache_key(currency: str) -> str:
        return f"rate:{currency}"

    async def get_rate(self, currency: str) -> Decimal:
        """Return USD per 1 unit of `currency`.

        Raises :class:`UnknownCurrencyError` (permanent) for a currency we do
        not know, and :class:`RateUnavailableError` (transient) when the
        downstream lookup fails.
        """
        currency = currency.upper()
        if currency == BASE_CURRENCY:
            # No lookup and no cache round-trip for the base currency.
            RATE_LOOKUPS.labels(result="cache_hit").inc()
            return Decimal("1")

        cached = await self._read_cache(currency)
        if cached is not None:
            RATE_LOOKUPS.labels(result="cache_hit").inc()
            return cached

        try:
            rate = await self._source.fetch(currency)
        except UnknownCurrencyError:
            RATE_LOOKUPS.labels(result="failed").inc()
            raise
        except RateUnavailableError:
            RATE_LOOKUPS.labels(result="failed").inc()
            raise
        except Exception as exc:  # any other downstream error is transient
            RATE_LOOKUPS.labels(result="failed").inc()
            raise RateUnavailableError(f"rate lookup failed for {currency}") from exc

        RATE_LOOKUPS.labels(result="fetched").inc()
        await self._write_cache(currency, rate)
        return rate

    async def _read_cache(self, currency: str) -> Decimal | None:
        if self._redis is None:
            return None
        try:
            raw = await self._redis.get(self._cache_key(currency))
        except RedisError as exc:
            logger.warning("rate cache read failed for %s: %s", currency, exc)
            return None
        if raw is None:
            return None
        try:
            return Decimal(raw if isinstance(raw, str) else raw.decode())
        except (DecimalException, ValueError):
            logger.warning("corrupt cached rate for %s, ignoring", currency)
            return None

    async def _write_cache(self, currency: str, rate: Decimal) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.set(self._cache_key(currency), str(rate), ex=self._ttl)
        except RedisError as exc:
            logger.warning("rate cache write failed for %s: %s", currency, exc)
