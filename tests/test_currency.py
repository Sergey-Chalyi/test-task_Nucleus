"""Unit tests for currency conversion (task requirement)."""

from decimal import Decimal

import pytest

from app.exceptions import RateUnavailableError, UnknownCurrencyError
from app.money import convert_to_usd
from app.rates import STATIC_RATES, ExchangeRateProvider, StaticRateSource
from tests.conftest import RecordingRateSource


class TestConvertToUsd:
    """The pure conversion function."""

    def test_converts_with_rate(self):
        assert convert_to_usd(Decimal("100.00"), Decimal("1.0850")) == Decimal("108.5000")

    def test_usd_rate_is_identity(self):
        assert convert_to_usd(Decimal("42.4242"), Decimal("1")) == Decimal("42.4242")

    def test_result_is_quantised_to_four_places(self):
        # 0.0064 USD/JPY * 12345 = 79.008 exactly -> padded, not rounded.
        result = convert_to_usd(Decimal("12345"), Decimal("0.0064"))
        assert result == Decimal("79.0080")
        assert result.as_tuple().exponent == -4

    @pytest.mark.parametrize(
        ("amount", "rate", "expected"),
        [
            # 1.23455 rounds half *up*, not half-to-even.
            (Decimal("1.23455"), Decimal("1"), Decimal("1.2346")),
            (Decimal("1.23465"), Decimal("1"), Decimal("1.2347")),
            # Banker's rounding would give 1.2346 for the first case.
            (Decimal("0.00005"), Decimal("1"), Decimal("0.0001")),
            (Decimal("0.00004"), Decimal("1"), Decimal("0.0000")),
        ],
    )
    def test_rounds_half_up(self, amount, rate, expected):
        assert convert_to_usd(amount, rate) == expected

    def test_handles_negative_amounts(self):
        """Refunds and chargebacks are negative; they must convert too."""
        assert convert_to_usd(Decimal("-50.00"), Decimal("1.2640")) == Decimal("-63.2000")

    def test_handles_zero(self):
        assert convert_to_usd(Decimal("0"), Decimal("1.0850")) == Decimal("0.0000")

    def test_keeps_precision_on_large_amounts(self):
        result = convert_to_usd(Decimal("1000000.55"), Decimal("1.085"))
        assert result == Decimal("1085000.5968")

    def test_rejects_float_amount(self):
        """Floats are how money bugs get in; the function refuses them."""
        with pytest.raises(TypeError):
            convert_to_usd(100.0, Decimal("1.085"))  # type: ignore[arg-type]

    def test_rejects_float_rate(self):
        with pytest.raises(TypeError):
            convert_to_usd(Decimal("100"), 1.085)  # type: ignore[arg-type]

    @pytest.mark.parametrize("rate", [Decimal("0"), Decimal("-1.5")])
    def test_rejects_non_positive_rate(self, rate):
        with pytest.raises(ValueError, match="positive"):
            convert_to_usd(Decimal("100"), rate)

    @pytest.mark.parametrize("bad", [Decimal("NaN"), Decimal("Infinity")])
    def test_rejects_non_finite(self, bad):
        with pytest.raises(ValueError, match="finite"):
            convert_to_usd(bad, Decimal("1"))
        with pytest.raises(ValueError, match="finite"):
            convert_to_usd(Decimal("1"), bad)


class TestExchangeRateProvider:
    """The cache-aside lookup in front of the downstream rate source."""

    async def test_usd_needs_no_lookup(self, rate_provider, rate_source):
        assert await rate_provider.get_rate("USD") == Decimal("1")
        assert rate_source.calls == []

    async def test_fetches_then_caches(self, rate_provider, rate_source):
        first = await rate_provider.get_rate("EUR")
        second = await rate_provider.get_rate("EUR")
        assert first == second == Decimal("1.0850")
        # The second call was served from Redis, not the downstream.
        assert rate_source.calls == ["EUR"]

    async def test_currency_code_is_case_insensitive(self, rate_provider):
        assert await rate_provider.get_rate("eur") == Decimal("1.0850")

    async def test_unknown_currency_is_permanent(self, rate_provider):
        with pytest.raises(UnknownCurrencyError):
            await rate_provider.get_rate("XYZ")

    async def test_downstream_failure_is_transient(self, redis, settings):
        source = RecordingRateSource(failures_before_success=1)
        provider = ExchangeRateProvider(redis, settings, source=source)
        with pytest.raises(RateUnavailableError):
            await provider.get_rate("EUR")
        # Nothing poisoned the cache: the next attempt succeeds.
        assert await provider.get_rate("EUR") == Decimal("1.0850")

    async def test_unexpected_downstream_error_becomes_transient(self, redis, settings):
        source = RecordingRateSource(
            failures_before_success=1, error=TimeoutError("connect timeout")
        )
        provider = ExchangeRateProvider(redis, settings, source=source)
        with pytest.raises(RateUnavailableError):
            await provider.get_rate("EUR")

    async def test_works_without_a_cache(self, settings, rate_source):
        """Redis is a cache, not a dependency: no Redis still converts."""
        provider = ExchangeRateProvider(None, settings, source=rate_source)
        assert await provider.get_rate("EUR") == Decimal("1.0850")
        assert await provider.get_rate("EUR") == Decimal("1.0850")
        # Without a cache every call hits the source - correct, just slower.
        assert rate_source.calls == ["EUR", "EUR"]

    async def test_cache_read_failure_falls_through_to_source(self, settings, rate_source):
        from redis.exceptions import ConnectionError as RedisConnectionError

        class BrokenRedis:
            async def get(self, key):
                raise RedisConnectionError("cache down")

            async def set(self, key, value, ex=None):
                raise RedisConnectionError("cache down")

        provider = ExchangeRateProvider(BrokenRedis(), settings, source=rate_source)
        assert await provider.get_rate("EUR") == Decimal("1.0850")

    async def test_corrupt_cache_entry_is_ignored(self, redis, settings, rate_source):
        await redis.set("rate:EUR", "not-a-number")
        provider = ExchangeRateProvider(redis, settings, source=rate_source)
        assert await provider.get_rate("EUR") == Decimal("1.0850")


class TestStaticRateSource:
    """The stand-in downstream used by the running service."""

    async def test_returns_known_rate(self):
        assert await StaticRateSource().fetch("EUR") == STATIC_RATES["EUR"]

    async def test_unknown_currency_raises_permanent(self):
        with pytest.raises(UnknownCurrencyError):
            await StaticRateSource().fetch("XYZ")

    async def test_failure_injection_always_fails_at_rate_one(self):
        with pytest.raises(RateUnavailableError):
            await StaticRateSource(failure_rate=1.0).fetch("EUR")

    def test_every_static_rate_is_a_positive_decimal(self):
        for code, rate in STATIC_RATES.items():
            assert isinstance(rate, Decimal), code
            assert rate > 0, code
            assert len(code) == 3 and code.isupper()
