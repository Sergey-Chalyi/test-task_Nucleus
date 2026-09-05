"""Worker entrypoint: `python -m app.worker`.

Runs the queue consumer plus a small Prometheus endpoint. Kept in a
separate process from the API so ingest latency is never affected by
processing throughput, and so the two can be scaled independently.
"""

import asyncio
import logging
import signal

from prometheus_client import start_http_server
from sqlalchemy import text

from app.config import get_settings
from app.consumer import EventConsumer
from app.db import dispose_engine, get_session_factory, init_db, session_scope
from app.logging_config import configure_logging
from app.processor import EventProcessor
from app.queue import EventQueue
from app.rates import ExchangeRateProvider
from app.redis_client import close_redis, get_redis

logger = logging.getLogger(__name__)

STARTUP_MAX_ATTEMPTS = 30
STARTUP_RETRY_DELAY = 2.0


async def wait_for_dependencies() -> None:
    """Block until Postgres and Redis answer, or give up after ~1 minute.

    Compose starts containers in parallel, so the worker routinely wins the
    race against the databases. Retrying here is simpler than a wait script.
    """
    redis = get_redis()
    for attempt in range(1, STARTUP_MAX_ATTEMPTS + 1):
        try:
            await redis.ping()
            async with session_scope() as session:
                await session.execute(text("SELECT 1"))
            logger.info("dependencies ready")
            return
        except Exception as exc:
            logger.warning(
                "dependencies not ready (attempt %d/%d): %s",
                attempt,
                STARTUP_MAX_ATTEMPTS,
                exc,
            )
            await asyncio.sleep(STARTUP_RETRY_DELAY)
    raise RuntimeError("dependencies did not become ready in time")


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("worker %s booting", settings.consumer_name)

    await wait_for_dependencies()
    await init_db()

    # Each process serves its own metrics; Prometheus sums across them.
    start_http_server(settings.worker_metrics_port)
    logger.info("metrics on :%d/metrics", settings.worker_metrics_port)

    consumer = EventConsumer(
        queue=EventQueue(get_redis(), settings),
        processor=EventProcessor(
            session_factory=get_session_factory(),
            rate_provider=ExchangeRateProvider(get_redis(), settings),
        ),
        settings=settings,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Graceful shutdown: finish the batch in flight, ack what succeeded,
        # and leave anything unfinished in the PEL for the next consumer.
        loop.add_signal_handler(sig, consumer.request_stop)

    try:
        await consumer.run()
    finally:
        await close_redis()
        await dispose_engine()


if __name__ == "__main__":
    asyncio.run(main())
