"""Application settings, loaded from environment variables."""

import socket
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration in one place.

    Every field can be overridden with an environment variable of the same
    (case-insensitive) name, e.g. ``REDIS_URL=redis://redis:6379/0``.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Infrastructure -------------------------------------------------
    database_url: str = "postgresql+asyncpg://events:events@localhost:5432/events"
    redis_url: str = "redis://localhost:6379/0"

    # Connections the pool keeps open. Leave unset and it follows
    # `concurrency`: a pool smaller than the number of tasks that want a
    # connection turns every checkout into a queue and costs ~3x throughput.
    db_pool_size: int | None = None
    db_max_overflow: int = 10

    # --- Queue ----------------------------------------------------------
    stream_name: str = "transactions"
    dlq_stream_name: str = "transactions.dlq"
    consumer_group: str = "processors"
    # Defaults to the container hostname, which is unique per replica, so
    # `docker compose up --scale worker=4` needs no extra configuration.
    consumer_name: str = Field(default_factory=socket.gethostname)
    # Cap the stream so a stuck consumer cannot fill up Redis memory.
    stream_max_len: int = 1_000_000

    # --- Consumer tuning ------------------------------------------------
    # How many messages one XREADGROUP call may return.
    batch_size: int = 100
    # How long XREADGROUP blocks when the stream is empty (milliseconds).
    # Zero disables the blocking read; the loop then polls every
    # `idle_poll_interval` seconds instead.
    block_ms: int = 2_000
    idle_poll_interval: float = 0.05
    # Messages processed concurrently inside one batch.
    concurrency: int = 32

    # --- Retry / backoff ------------------------------------------------
    # In-process attempts before the message is left unacked for redelivery.
    max_attempts: int = 3
    retry_base_delay: float = 0.2
    retry_max_delay: float = 5.0
    # A pending message idle for this long is reclaimed by another consumer.
    reclaim_idle_ms: int = 30_000
    reclaim_interval: float = 5.0
    # Total deliveries (across consumers) before the message goes to the DLQ.
    max_deliveries: int = 5

    # --- Exchange rates -------------------------------------------------
    rates_cache_ttl: int = 300
    # Probability (0..1) that a rate lookup fails. Used to exercise the retry
    # path locally; keep at 0.0 for real runs.
    rates_failure_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    rates_latency: float = 0.0

    # --- HTTP API -------------------------------------------------------
    api_page_size: int = 50
    api_max_page_size: int = 500

    # --- Observability --------------------------------------------------
    worker_metrics_port: int = 9100
    metrics_refresh_interval: float = 5.0
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings singleton."""
    return Settings()
