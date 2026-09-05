"""Process-wide Redis connection pool."""

from redis.asyncio import Redis

from app.config import get_settings

_redis: Redis | None = None


def get_redis() -> Redis:
    """Return the shared Redis client, creating the pool on first use.

    `decode_responses=True` keeps the rest of the code working with `str`
    instead of `bytes`; the payloads we store are JSON, never binary.
    """
    global _redis
    if _redis is None:
        settings = get_settings()
        _redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            health_check_interval=30,
        )
    return _redis


async def close_redis() -> None:
    """Close the shared client (called on shutdown)."""
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
