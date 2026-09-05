"""Prometheus metrics shared by the API process and the worker process.

Both processes import this module and expose their own scrape endpoint
(`/metrics` on the API, a standalone HTTP server on the worker), so the
counters below are per-process and are summed by Prometheus at query time.
"""

from prometheus_client import Counter, Gauge, Histogram

# --- Producer side (HTTP API) -------------------------------------------
EVENTS_RECEIVED = Counter(
    "events_received_total",
    "Transaction events accepted by the HTTP API and pushed to the queue.",
)

EVENTS_REJECTED = Counter(
    "events_rejected_total",
    "Transaction events rejected before reaching the queue.",
    ["reason"],
)

# --- Consumer side (worker) ---------------------------------------------
EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Events taken off the queue and acknowledged, by outcome.",
    ["result"],  # stored | duplicate
)

EVENTS_FAILED = Counter(
    "events_failed_total",
    "Processing attempts that raised, by failure class.",
    ["reason"],  # rate_unavailable | database_unavailable | invalid | unknown
)

EVENTS_RETRIED = Counter(
    "events_retried_total",
    "Individual retry attempts made after a transient failure.",
)

EVENTS_DEAD_LETTERED = Counter(
    "events_dead_lettered_total",
    "Events moved to the dead-letter stream after exhausting all deliveries.",
)

PROCESSING_SECONDS = Histogram(
    "event_processing_seconds",
    "Wall-clock time to process one event, including retries.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# --- Queue health --------------------------------------------------------
QUEUE_LAG = Gauge(
    "queue_lag_messages",
    "Entries added to the stream that the consumer group has not read yet.",
)

QUEUE_PENDING = Gauge(
    "queue_pending_messages",
    "Messages delivered to a consumer but not yet acknowledged (in flight).",
)

QUEUE_LENGTH = Gauge(
    "queue_length_messages",
    "Total entries currently retained in the Redis stream.",
)

DLQ_LENGTH = Gauge(
    "queue_dlq_length_messages",
    "Total entries currently retained in the dead-letter stream.",
)

# --- Downstream dependency ----------------------------------------------
RATE_LOOKUPS = Counter(
    "rate_lookups_total",
    "Exchange-rate lookups, by outcome.",
    ["result"],  # cache_hit | fetched | failed
)
