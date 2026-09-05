"""Operational endpoints: liveness, readiness, metrics, queue introspection."""

import logging

from fastapi import APIRouter, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text

from app.db import session_scope
from app.dependencies import QueueDep, RedisDep
from app.metrics import DLQ_LENGTH, QUEUE_LAG, QUEUE_LENGTH, QUEUE_PENDING
from app.schemas import HealthResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["system"])


@router.get("/health", response_model=HealthResponse, summary="Readiness probe")
async def health(redis: RedisDep, response: Response) -> HealthResponse:
    """Report whether Postgres and Redis are both reachable.

    Returns 503 when either is down so an orchestrator stops routing traffic
    here, but still reports *which* dependency failed in the body.
    """
    database = "ok"
    try:
        async with session_scope() as session:
            await session.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning("health: database check failed: %s", exc)
        database = "unavailable"

    redis_state = "ok"
    try:
        await redis.ping()
    except Exception as exc:
        logger.warning("health: redis check failed: %s", exc)
        redis_state = "unavailable"

    healthy = database == "ok" and redis_state == "ok"
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return HealthResponse(
        status="ok" if healthy else "degraded",
        database=database,
        redis=redis_state,
    )


@router.get("/live", summary="Liveness probe")
async def live() -> dict[str, str]:
    """Cheap probe that only says the process is running."""
    return {"status": "ok"}


@router.get("/metrics", summary="Prometheus metrics for the API process")
async def metrics(queue: QueueDep) -> Response:
    """Expose the API process's counters in Prometheus text format.

    Queue gauges are refreshed here too so the numbers are meaningful even
    if the worker is down.
    """
    try:
        stats = await queue.stats()
        QUEUE_LENGTH.set(stats.length)
        QUEUE_PENDING.set(stats.pending)
        QUEUE_LAG.set(stats.lag)
        DLQ_LENGTH.set(stats.dlq_length)
    except Exception as exc:
        logger.warning("could not refresh queue gauges: %s", exc)

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/queue/stats", summary="Human-readable queue depth and lag")
async def queue_stats(queue: QueueDep) -> dict[str, int]:
    """Same numbers as `/metrics`, as JSON, for quick manual checks."""
    stats = await queue.stats()
    return {
        "stream_length": stats.length,
        "pending": stats.pending,
        "lag": stats.lag,
        "dlq_length": stats.dlq_length,
    }
