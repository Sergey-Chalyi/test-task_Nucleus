"""Unit tests for the backoff policy and the retry wrapper."""

import pytest

from app.exceptions import PermanentError, RateUnavailableError, StorageUnavailableError
from app.retry import backoff_delay, retry_transient


class TestBackoffDelay:
    def test_ceiling_doubles_per_attempt(self, monkeypatch):
        # random.uniform(0, ceiling) - pin it to the ceiling to see the ramp.
        monkeypatch.setattr("app.retry.random.uniform", lambda _lo, hi: hi)
        assert backoff_delay(1, base_delay=0.5, max_delay=100) == 0.5
        assert backoff_delay(2, base_delay=0.5, max_delay=100) == 1.0
        assert backoff_delay(3, base_delay=0.5, max_delay=100) == 2.0
        assert backoff_delay(7, base_delay=0.5, max_delay=100) == 32.0

    def test_ceiling_is_capped(self, monkeypatch):
        monkeypatch.setattr("app.retry.random.uniform", lambda _lo, hi: hi)
        assert backoff_delay(20, base_delay=0.5, max_delay=5.0) == 5.0

    def test_delay_is_jittered_within_the_ceiling(self):
        delays = {backoff_delay(4, base_delay=0.5, max_delay=100) for _ in range(200)}
        assert all(0 <= d <= 4.0 for d in delays)
        # Full jitter, not a constant: 200 draws must not collapse to one value.
        assert len(delays) > 1

    def test_rejects_attempt_zero(self):
        with pytest.raises(ValueError):
            backoff_delay(0, base_delay=1, max_delay=10)


class TestRetryTransient:
    @staticmethod
    async def _no_sleep(_seconds: float) -> None:
        return None

    async def test_returns_immediately_on_success(self):
        calls = []

        async def op():
            calls.append(1)
            return "done"

        result = await retry_transient(
            op, max_attempts=3, base_delay=0, max_delay=0, sleep=self._no_sleep
        )
        assert result == "done"
        assert len(calls) == 1

    async def test_retries_until_the_dependency_recovers(self):
        calls = []

        async def op():
            calls.append(1)
            if len(calls) < 3:
                raise RateUnavailableError("provider down")
            return "recovered"

        result = await retry_transient(
            op, max_attempts=5, base_delay=0, max_delay=0, sleep=self._no_sleep
        )
        assert result == "recovered"
        assert len(calls) == 3

    async def test_reraises_after_exhausting_attempts(self):
        calls = []

        async def op():
            calls.append(1)
            raise StorageUnavailableError("db down")

        with pytest.raises(StorageUnavailableError):
            await retry_transient(
                op, max_attempts=4, base_delay=0, max_delay=0, sleep=self._no_sleep
            )
        assert len(calls) == 4

    async def test_permanent_errors_are_not_retried(self):
        calls = []

        async def op():
            calls.append(1)
            raise PermanentError("bad event")

        with pytest.raises(PermanentError):
            await retry_transient(
                op, max_attempts=5, base_delay=0, max_delay=0, sleep=self._no_sleep
            )
        assert len(calls) == 1

    async def test_unexpected_errors_are_not_retried(self):
        """Only TransientError is retryable; a bug should surface at once."""
        calls = []

        async def op():
            calls.append(1)
            raise ZeroDivisionError("bug")

        with pytest.raises(ZeroDivisionError):
            await retry_transient(
                op, max_attempts=5, base_delay=0, max_delay=0, sleep=self._no_sleep
            )
        assert len(calls) == 1

    async def test_on_retry_hook_fires_once_per_retry(self):
        retries = []

        async def op():
            raise RateUnavailableError("down")

        with pytest.raises(RateUnavailableError):
            await retry_transient(
                op,
                max_attempts=3,
                base_delay=0,
                max_delay=0,
                on_retry=lambda: retries.append(1),
                sleep=self._no_sleep,
            )
        # 3 attempts -> 2 waits between them.
        assert len(retries) == 2

    async def test_sleeps_between_attempts(self):
        slept = []

        async def op():
            raise RateUnavailableError("down")

        async def record(seconds):
            slept.append(seconds)

        with pytest.raises(RateUnavailableError):
            await retry_transient(
                op, max_attempts=3, base_delay=0.5, max_delay=10, sleep=record
            )
        assert len(slept) == 2
        assert all(s >= 0 for s in slept)

    async def test_rejects_zero_attempts(self):
        async def op():
            return None

        with pytest.raises(ValueError):
            await retry_transient(op, max_attempts=0, base_delay=0, max_delay=0)
