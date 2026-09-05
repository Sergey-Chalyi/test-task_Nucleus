"""Redis Streams queue: publishing, consuming, acking, dead-lettering.

Why a Redis Stream and not a plain list: a stream keeps a *consumer group*
with a pending-entries list (PEL). A message that was delivered but never
acknowledged stays in the PEL, so a consumer that crashes mid-event does
not take the event with it — another consumer reclaims it with XAUTOCLAIM.
That is what makes the pipeline at-least-once.
"""

import logging
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import Settings
from app.schemas import TransactionEvent

logger = logging.getLogger(__name__)

PAYLOAD_FIELD = "payload"


@dataclass(frozen=True, slots=True)
class QueuedMessage:
    """One entry read from the stream."""

    message_id: str
    payload: str
    # How many times this entry has been delivered to a consumer (>=1).
    delivery_count: int = 1


@dataclass(frozen=True, slots=True)
class QueueStats:
    """Snapshot of queue health, published as metrics."""

    length: int
    pending: int
    lag: int
    dlq_length: int


class EventQueue:
    """Thin, explicit wrapper over the Redis Stream commands we use."""

    def __init__(self, redis: Redis, settings: Settings) -> None:
        self._redis = redis
        self._stream = settings.stream_name
        self._dlq = settings.dlq_stream_name
        self._group = settings.consumer_group
        self._consumer = settings.consumer_name
        self._maxlen = settings.stream_max_len

    # --- producer -------------------------------------------------------

    async def publish(self, event: TransactionEvent) -> str:
        """Append one event to the stream and return its entry id.

        `maxlen` with `approximate=True` trims lazily at node boundaries: it
        bounds memory without paying for an exact trim on every write.
        """
        return await self._redis.xadd(
            self._stream,
            {PAYLOAD_FIELD: event.model_dump_json()},
            maxlen=self._maxlen,
            approximate=True,
        )

    async def publish_many(self, events: list[TransactionEvent]) -> list[str]:
        """Append many events in a single round-trip via a pipeline."""
        pipe = self._redis.pipeline(transaction=False)
        for event in events:
            pipe.xadd(
                self._stream,
                {PAYLOAD_FIELD: event.model_dump_json()},
                maxlen=self._maxlen,
                approximate=True,
            )
        return await pipe.execute()

    # --- consumer group lifecycle ---------------------------------------

    async def ensure_group(self) -> None:
        """Create the consumer group (and the stream) if they do not exist.

        `id="0"` so a group created after events were already published
        still sees them; `mkstream=True` so the worker can start first.
        """
        try:
            await self._redis.xgroup_create(
                self._stream, self._group, id="0", mkstream=True
            )
            logger.info("created consumer group %s on %s", self._group, self._stream)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
            logger.debug("consumer group %s already exists", self._group)

    # --- consumer -------------------------------------------------------

    async def read(self, count: int, block_ms: int = 0) -> list[QueuedMessage]:
        """Read up to `count` never-delivered entries.

        The special id `>` means "entries never delivered to any consumer in
        this group"; already-pending entries are handled by `claim_stale`.

        `block_ms > 0` parks the call inside Redis until an entry arrives,
        which is what keeps an idle worker from spinning. `block_ms <= 0`
        omits BLOCK and returns immediately - the caller then paces itself.
        """
        kwargs = {
            "groupname": self._group,
            "consumername": self._consumer,
            "streams": {self._stream: ">"},
            "count": count,
        }
        if block_ms > 0:
            kwargs["block"] = block_ms
        response = await self._redis.xreadgroup(**kwargs)
        if not response:
            return []
        # Single stream requested, so a single (name, entries) pair comes back.
        _stream_name, entries = response[0]
        return [
            QueuedMessage(message_id=mid, payload=fields.get(PAYLOAD_FIELD, ""))
            for mid, fields in entries
        ]

    async def claim_stale(
        self, min_idle_ms: int, count: int
    ) -> tuple[list[QueuedMessage], list[str]]:
        """Take over messages another consumer left unacknowledged.

        Returns the reclaimed messages plus the ids of entries that vanished
        from the stream (trimmed away) and only need acking.
        """
        claimed, deleted = await self._autoclaim(min_idle_ms, count)
        if not claimed:
            return [], deleted

        counts = await self._delivery_counts([mid for mid, _ in claimed])
        messages = [
            QueuedMessage(
                message_id=mid,
                payload=fields.get(PAYLOAD_FIELD, ""),
                delivery_count=counts.get(mid, 1),
            )
            for mid, fields in claimed
        ]
        return messages, deleted

    async def _autoclaim(
        self, min_idle_ms: int, count: int
    ) -> tuple[list[tuple[str, dict]], list[str]]:
        result = await self._redis.xautoclaim(
            name=self._stream,
            groupname=self._group,
            consumername=self._consumer,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )
        # Redis >= 7 returns (next_cursor, entries, deleted_ids);
        # Redis 6 returns (next_cursor, entries).
        entries = result[1] if len(result) > 1 else []
        deleted = list(result[2]) if len(result) > 2 else []
        return [(mid, fields) for mid, fields in entries if fields is not None], deleted

    async def _delivery_counts(self, message_ids: list[str]) -> dict[str, int]:
        """Ask the PEL how often each message has been delivered so far.

        Scoped to this consumer: XAUTOCLAIM has just transferred ownership of
        every id in `message_ids` to us, so one XPENDING over our own entries
        covers them all without scanning the whole pending list.
        """
        if not message_ids:
            return {}
        pending = await self._redis.xpending_range(
            name=self._stream,
            groupname=self._group,
            min="-",
            max="+",
            count=max(len(message_ids), 1),
            consumername=self._consumer,
        )
        wanted = set(message_ids)
        return {
            entry["message_id"]: int(entry.get("times_delivered", 1))
            for entry in pending
            if entry["message_id"] in wanted
        }

    async def ack(self, *message_ids: str) -> int:
        """Acknowledge messages, removing them from the pending list."""
        if not message_ids:
            return 0
        return await self._redis.xack(self._stream, self._group, *message_ids)

    async def dead_letter(self, message: QueuedMessage, reason: str) -> None:
        """Park a poisonous message in the DLQ, then ack the original.

        The XADD and the XACK go out in one pipeline so we cannot ack an
        event that failed to reach the DLQ.
        """
        pipe = self._redis.pipeline(transaction=True)
        pipe.xadd(
            self._dlq,
            {
                PAYLOAD_FIELD: message.payload,
                "reason": reason,
                "original_id": message.message_id,
                "delivery_count": str(message.delivery_count),
            },
            maxlen=self._maxlen,
            approximate=True,
        )
        pipe.xack(self._stream, self._group, message.message_id)
        await pipe.execute()
        logger.error(
            "dead-lettered message %s after %d deliveries: %s",
            message.message_id,
            message.delivery_count,
            reason,
        )

    # --- observability ---------------------------------------------------

    async def stats(self) -> QueueStats:
        """Collect stream length, in-flight count, lag and DLQ size."""
        length = await self._redis.xlen(self._stream)
        dlq_length = await self._redis.xlen(self._dlq)

        pending = 0
        lag = 0
        for group in await self._redis.xinfo_groups(self._stream):
            if group.get("name") != self._group:
                continue
            pending = int(group.get("pending") or 0)
            # `lag` (Redis >= 7.0) is the number of entries the group has not
            # read yet. It is None when Redis cannot compute it after a trim.
            raw_lag = group.get("lag")
            lag = int(raw_lag) if raw_lag is not None else 0
        return QueueStats(
            length=length, pending=pending, lag=lag, dlq_length=dlq_length
        )
