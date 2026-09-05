"""FastAPI application: the producer half of the service."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.api import events, system, users
from app.config import get_settings
from app.db import dispose_engine, init_db
from app.logging_config import configure_logging
from app.metrics import EVENTS_REJECTED
from app.queue import EventQueue
from app.redis_client import close_redis, get_redis

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Prepare shared resources on startup, release them on shutdown."""
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("starting API")

    await init_db()
    # Create the consumer group here too, so events published before the
    # worker's first start are still delivered to it rather than skipped.
    await EventQueue(get_redis(), settings).ensure_group()

    yield

    logger.info("shutting down API")
    await close_redis()
    await dispose_engine()


def create_app() -> FastAPI:
    """Application factory - keeps tests free to build their own instance."""
    app = FastAPI(
        title="Transaction Event Service",
        description=(
            "Accepts transaction events, queues them in Redis Streams, "
            "converts amounts to USD in an async worker and serves per-user "
            "aggregates."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    app.include_router(events.router)
    app.include_router(users.router)
    app.include_router(system.router)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        """Never leak a stack trace to a client; always log it server-side."""
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        EVENTS_REJECTED.labels(reason="internal_error").inc()
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "internal server error"},
        )

    @app.get("/", tags=["system"], summary="Service banner")
    async def root() -> dict[str, str]:
        return {
            "service": "transaction-event-service",
            "version": app.version,
            "docs": "/docs",
        }

    return app


app = create_app()
