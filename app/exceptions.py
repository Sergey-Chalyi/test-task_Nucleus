"""Error taxonomy that drives the retry policy.

The consumer only ever asks one question about a failure: *is it worth
trying again?* Everything transient (a rate provider that timed out, a
database that is restarting) inherits from :class:`TransientError` and is
retried with backoff. Everything permanent (an event we will never be able
to process, no matter how often we try) inherits from
:class:`PermanentError` and goes straight to the dead-letter queue.
"""


class EventProcessingError(Exception):
    """Base class for every failure raised while processing an event."""

    reason = "unknown"


class TransientError(EventProcessingError):
    """A failure that may succeed on a later attempt. Retry it."""


class PermanentError(EventProcessingError):
    """A failure that will never succeed. Do not retry, dead-letter it."""


class RateUnavailableError(TransientError):
    """The exchange-rate lookup failed or timed out."""

    reason = "rate_unavailable"


class StorageUnavailableError(TransientError):
    """The database could not be reached or the write failed transiently."""

    reason = "database_unavailable"


class UnknownCurrencyError(PermanentError):
    """The event carries a currency we have no rate for."""

    reason = "unknown_currency"


class InvalidEventError(PermanentError):
    """The queued payload could not be parsed back into an event."""

    reason = "invalid_event"
