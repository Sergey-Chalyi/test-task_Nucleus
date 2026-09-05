"""Exponential backoff with full jitter."""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from app.exceptions import TransientError

logger = logging.getLogger(__name__)

T = TypeVar("T")


def backoff_delay(attempt: int, base_delay: float, max_delay: float) -> float:
    """Delay before retry number `attempt` (1-based), with full jitter.

    Full jitter (`uniform(0, base * 2**(attempt-1))`) rather than a fixed
    ramp: when a downstream comes back after an outage, every worker would
    otherwise retry in lockstep and knock it over again.
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    ceiling = min(max_delay, base_delay * (2 ** (attempt - 1)))
    return random.uniform(0, ceiling)


async def retry_transient(
    operation: Callable[[], Awaitable[T]],
    *,
    max_attempts: int,
    base_delay: float,
    max_delay: float,
    description: str = "operation",
    on_retry: Callable[[], None] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Run `operation`, retrying only :class:`TransientError` failures.

    Permanent errors propagate on the first attempt - retrying them just
    burns the downstream. After `max_attempts` the last transient error is
    re-raised so the caller can decide what to do (here: leave the message
    unacknowledged for redelivery).
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    last_error: TransientError | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await operation()
        except TransientError as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            delay = backoff_delay(attempt, base_delay, max_delay)
            logger.warning(
                "%s failed (attempt %d/%d): %s - retrying in %.3fs",
                description,
                attempt,
                max_attempts,
                exc,
                delay,
            )
            if on_retry is not None:
                on_retry()
            await sleep(delay)

    assert last_error is not None  # only reachable through the except branch
    raise last_error
