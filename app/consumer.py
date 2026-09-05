"""The queue consumer: read, process with retries, ack or dead-letter.

Delivery semantics: **at-least-once**. A message is acknowledged only after
the row is committed, so a crash between the two replays the event. The
replay is harmless because :class:`~app.processor.EventProcessor` is
idempotent on the event id - the observable end state is exactly-once
*storage* built out of at-least-once *delivery*.
"""

import asyncio
import contextlib
import logging
import time

from pydantic import ValidationError

from app.config import Settings
from app.exceptions import InvalidEventError, PermanentError, TransientError
from app.metrics import (
    DLQ_LENGTH,
    EVENTS_DEAD_LETTERED,
    EVENTS_FAILED,
    EVENTS_PROCESSED,
    EVENTS_RETRIED,
    PROCESSING_SECONDS,
    QUEUE_LAG,
    QUEUE_LENGTH,
    QUEUE_PENDING,
)
from app.processor import EventProcessor
from app.queue import EventQueue, QueuedMessage
from app.retry import retry_transient
from app.schemas import TransactionEvent

logger = logging.getLogger(__name__)


class EventConsumer:
    """Runs three cooperating loops until asked to stop."""

    def __init__(
        self,
        queue: EventQueue,
        processor: EventProcessor,
        settings: Settings,
    ) -> None:
        self._queue = queue
        self._processor = processor
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.concurrency)
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        """Ask every loop to finish its current batch and return."""
        self._stop.set()

    async def run(self) -> None:
        """Start the consume / reclaim / metrics loops and wait for them."""
        await self._queue.ensure_group()
        logger.info(
            "consumer %s started (batch=%d concurrency=%d)",
            self._settings.consumer_name,
            self._settings.batch_size,
            self._settings.concurrency,
        )
        await asyncio.gather(
            self._consume_loop(),
            self._reclaim_loop(),
            self._metrics_loop(),
        )
        logger.info("consumer stopped")

    # --- loops ------------------------------------------------------------

    async def _consume_loop(self) -> None:
        """Pull entries that no consumer has seen yet."""
        while not self._stop.is_set():
            try:
                messages = await self._queue.read(
                    count=self._settings.batch_size,
                    block_ms=self._settings.block_ms,
                )
            except Exception:
                # Redis is down. Nothing is lost - unacked entries stay in the
                # PEL and unread ones stay in the stream - so back off and
                # retry. (Cancellation is a BaseException and still escapes,
                # which is what makes shutdown work.)
                logger.exception("read from queue failed, backing off")
                await self._sleep(1.0)
                continue

            if messages:
                await self._handle_batch(messages)
            elif self._settings.block_ms <= 0:
                # Non-blocking mode: pace the loop so it does not spin hot.
                await self._sleep(self._settings.idle_poll_interval)

    async def _reclaim_loop(self) -> None:
        """Recover messages a dead or stuck consumer never acknowledged."""
        while not self._stop.is_set():
            await self._sleep(self._settings.reclaim_interval)
            if self._stop.is_set():
                break
            try:
                messages, deleted = await self._queue.claim_stale(
                    min_idle_ms=self._settings.reclaim_idle_ms,
                    count=self._settings.batch_size,
                )
                if deleted:
                    # Entries trimmed out of the stream while still pending;
                    # ack them so the PEL does not grow forever.
                    await self._queue.ack(*deleted)
            except Exception:
                logger.exception("reclaim failed, will try again")
                continue

            if not messages:
                continue

            logger.info("reclaimed %d stale message(s)", len(messages))
            retryable = []
            for message in messages:
                if message.delivery_count > self._settings.max_deliveries:
                    await self._dead_letter(
                        message,
                        f"exceeded {self._settings.max_deliveries} delivery attempts",
                    )
                else:
                    retryable.append(message)
            if retryable:
                await self._handle_batch(retryable)

    async def _metrics_loop(self) -> None:
        """Refresh queue-health gauges on a fixed interval."""
        while not self._stop.is_set():
            try:
                stats = await self._queue.stats()
                QUEUE_LENGTH.set(stats.length)
                QUEUE_PENDING.set(stats.pending)
                QUEUE_LAG.set(stats.lag)
                DLQ_LENGTH.set(stats.dlq_length)
            except Exception as exc:
                logger.warning("queue stats unavailable: %s", exc)
            await self._sleep(self._settings.metrics_refresh_interval)

    # --- message handling -------------------------------------------------

    async def _handle_batch(self, messages: list[QueuedMessage]) -> None:
        """Process a batch concurrently, then acknowledge what succeeded.

        The ack is deferred to one XACK for the whole batch: at 1k events/sec
        a round-trip per message is the difference between Redis being free
        and Redis being the bottleneck. Every id in the list has already had
        its row committed, so batching the ack changes nothing about the
        at-least-once guarantee.
        """
        results = await asyncio.gather(*(self._handle_one(m) for m in messages))
        done = [message_id for message_id in results if message_id is not None]
        if done:
            await self._ack(*done)

    async def _handle_one(self, message: QueuedMessage) -> str | None:
        """Process one message; return its id if it is ready to be acked."""
        async with self._semaphore:
            started = time.perf_counter()
            try:
                event = self._parse(message)
                outcome = await retry_transient(
                    lambda: self._processor.process(event),
                    max_attempts=self._settings.max_attempts,
                    base_delay=self._settings.retry_base_delay,
                    max_delay=self._settings.retry_max_delay,
                    description=f"processing event {event.id}",
                    on_retry=EVENTS_RETRIED.inc,
                )
            except PermanentError as exc:
                # This event will never succeed: park it and move on.
                # `_dead_letter` acks it itself, so nothing to ack here.
                EVENTS_FAILED.labels(reason=exc.reason).inc()
                await self._dead_letter(message, str(exc))
                return None
            except TransientError as exc:
                # Out of in-process attempts. Do NOT ack: the entry stays in
                # the pending list and the reclaim loop retries it later.
                EVENTS_FAILED.labels(reason=exc.reason).inc()
                logger.error(
                    "giving up on %s for now, leaving it pending: %s",
                    message.message_id,
                    exc,
                )
                return None
            except Exception:  # unexpected bug: keep the event, alert loudly
                EVENTS_FAILED.labels(reason="unknown").inc()
                logger.exception("unexpected error on %s", message.message_id)
                return None
            finally:
                PROCESSING_SECONDS.observe(time.perf_counter() - started)

            EVENTS_PROCESSED.labels(result=outcome.value).inc()
            return message.message_id

    def _parse(self, message: QueuedMessage) -> TransactionEvent:
        """Decode the queued JSON payload back into an event."""
        try:
            return TransactionEvent.model_validate_json(message.payload)
        except ValidationError as exc:
            raise InvalidEventError(f"unparsable payload: {exc}") from exc

    async def _ack(self, *message_ids: str) -> None:
        try:
            await self._queue.ack(*message_ids)
        except Exception:
            # The rows are already committed; a failed ack only means those
            # events get redelivered and deduplicated. Log and carry on.
            logger.exception("ack failed for %d message(s) (will be redelivered)",
                             len(message_ids))

    async def _dead_letter(self, message: QueuedMessage, reason: str) -> None:
        try:
            await self._queue.dead_letter(message, reason)
            EVENTS_DEAD_LETTERED.inc()
        except Exception:
            logger.exception("could not dead-letter %s, leaving it pending",
                             message.message_id)

    async def _sleep(self, seconds: float) -> None:
        """Sleep, but wake up immediately when a stop is requested.

        Waiting on the stop event instead of `asyncio.sleep` is what makes
        shutdown feel instant rather than taking a whole poll interval.
        """
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)


__all__ = ["EventConsumer"]
