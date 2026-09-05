"""Ingest endpoints: accept transaction events and hand them to the queue."""

import logging

from fastapi import APIRouter, Body, HTTPException, status
from redis.exceptions import RedisError

from app.dependencies import QueueDep
from app.metrics import EVENTS_RECEIVED, EVENTS_REJECTED
from app.schemas import BatchAccepted, EventAccepted, TransactionEvent

logger = logging.getLogger(__name__)

router = APIRouter(tags=["events"])

MAX_BATCH_SIZE = 500


@router.post(
    "/events",
    response_model=EventAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Receive one transaction event",
)
async def receive_event(queue: QueueDep, event: TransactionEvent) -> EventAccepted:
    """Validate the event and append it to the queue.

    Returns 202, not 201: at this point the event is durable in Redis but
    has not been converted or stored yet. Reporting 201 would be a lie.
    """
    try:
        message_id = await queue.publish(event)
    except RedisError as exc:
        EVENTS_REJECTED.labels(reason="queue_unavailable").inc()
        logger.error("failed to enqueue event %s: %s", event.id, exc)
        # 503 tells the producer to retry; we would rather be rejected loudly
        # than accept an event we cannot store.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="event queue unavailable, retry later",
        ) from exc

    EVENTS_RECEIVED.inc()
    return EventAccepted(id=event.id, message_id=message_id)


@router.post(
    "/events/batch",
    response_model=BatchAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Receive many transaction events in one call",
)
async def receive_events(
    queue: QueueDep,
    events: list[TransactionEvent] = Body(..., min_length=1),
) -> BatchAccepted:
    """Enqueue a batch in a single Redis pipeline.

    At 1k events/sec the per-request overhead dominates, so producers that
    can buffer should use this endpoint instead of hammering `/events`.
    """
    if len(events) > MAX_BATCH_SIZE:
        EVENTS_REJECTED.labels(reason="batch_too_large").inc()
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"batch may contain at most {MAX_BATCH_SIZE} events",
        )

    try:
        message_ids = await queue.publish_many(events)
    except RedisError as exc:
        EVENTS_REJECTED.labels(reason="queue_unavailable").inc()
        logger.error("failed to enqueue batch of %d: %s", len(events), exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="event queue unavailable, retry later",
        ) from exc

    EVENTS_RECEIVED.inc(len(events))
    return BatchAccepted(accepted=len(message_ids), message_ids=message_ids)
