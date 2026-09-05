# Transaction Event Service

An async event-processing service: it accepts transaction events over HTTP, queues them in
**Redis Streams**, converts each amount to USD in a separate worker process, stores the result
in **PostgreSQL** exactly once, and serves per-user aggregates.

```
                  ┌──────────────┐                  ┌──────────────┐
  POST /events    │              │  XADD            │              │
 ───────────────▶ │  FastAPI     │ ───────────────▶ │ Redis Stream │
                  │  (producer)  │                  │ transactions │
                  └──────┬───────┘                  └──────┬───────┘
                         │                                 │ XREADGROUP
   GET /users/{id}/...   │                                 │ (consumer group)
 ◀───────────────────────┤                                 ▼
                         │                          ┌──────────────┐   rate lookup
                         │                          │   Worker     │ ─────────────▶ FX source
                         │                          │  (consumer)  │ ◀───────────── (cached in Redis)
                         │                          └──────┬───────┘
                         │                                 │ INSERT ... ON CONFLICT DO NOTHING
                         ▼                                 ▼
                  ┌───────────────────────────────────────────────┐
                  │              PostgreSQL: transactions         │
                  └───────────────────────────────────────────────┘
                                          │ on permanent failure
                                          ▼
                                 Redis Stream: transactions.dlq
```

---

## Quick start

```bash
docker compose up --build          # postgres, redis, api, worker
curl localhost:8000/health         # {"status":"ok","database":"ok","redis":"ok"}
```

The API is on <http://localhost:8000>, interactive docs on <http://localhost:8000/docs>.

If ports 5432/6379/8000 are already taken on your machine, override the host side:

```bash
POSTGRES_HOST_PORT=55432 REDIS_HOST_PORT=56379 API_HOST_PORT=8080 docker compose up --build
```

Send an event and read it back:

```bash
curl -X POST localhost:8000/events -H 'content-type: application/json' -d '{
  "id": "evt-001",
  "user_id": "user-42",
  "amount": "125.50",
  "currency": "EUR",
  "timestamp": "2026-09-05T10:15:00Z"
}'
# {"id":"evt-001","status":"queued","message_id":"1788591498512-0"}

curl localhost:8000/users/user-42/summary
# {"user_id":"user-42","total_usd":"136.1675","transaction_count":1}
```

Send the *same* event again and the count stays at 1 — that is the deduplication working.

### Running the tests

The test suite needs no Docker: SQLite stands in for PostgreSQL and `fakeredis` for Redis.

```bash
make install     # python3 -m venv .venv && pip install -r requirements-dev.txt
make test        # 118 tests, ~2s
make lint        # ruff
```

### Generating load

```bash
.venv/bin/python scripts/load_test.py --rate 1000 --seconds 10
```

It publishes at the requested rate (replaying 5% of ids so dedup has work to do), then waits
until the queue has drained and prints how long the consumer took to catch up.

---

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/events` | Accept one transaction event. Returns **202** — the event is durable in Redis, not yet stored. |
| `POST` | `/events/batch` | Accept up to 500 events in one Redis pipeline. |
| `GET` | `/users/{user_id}/summary` | Total USD and transaction count. |
| `GET` | `/users/{user_id}/transactions` | Paginated history; `from`, `to`, `limit`, `offset`. |
| `GET` | `/health` | Readiness: reports PostgreSQL and Redis, **503** if either is down. |
| `GET` | `/live` | Liveness: is the process up. |
| `GET` | `/metrics` | Prometheus metrics for the API process. |
| `GET` | `/queue/stats` | Queue depth, lag, in-flight and DLQ size as JSON. |

**Event shape.** `id` (the idempotency key), `user_id`, `amount` (decimal string, at most 4
decimal places), `currency` (ISO-4217, case-insensitive), `timestamp` (ISO-8601; a naive value
is read as UTC).

**Listing.**

```bash
curl "localhost:8000/users/user-42/transactions?from=2026-09-01T00:00:00Z&limit=50&offset=0"
```

`from`/`to` are inclusive bounds on `timestamp`, results are newest first, and the response
carries `total` and `has_more` so a client can page without guessing. An unknown user is not a
404 — it is a user with zero transactions.

**Money.** Every amount is a `Decimal` end to end: validated at 4 decimal places, stored in
`NUMERIC(24,4)`, serialised as a JSON *string*. No float ever touches a monetary value.
Conversion rounds `ROUND_HALF_UP`, because that is what someone checking a total by hand
expects. Four decimals rather than two so that summing millions of rows does not drift by the
rounding error of each one.

---

## Why Redis Streams

The queue needed four things: durability, redelivery of anything a crashed consumer never
acknowledged, horizontal scale-out across workers, and an operator who can see how far behind
the consumer is. Redis Streams gives all four, and Redis is already in the stack as the
exchange-rate cache — one dependency instead of two.

Concretely, a **consumer group** keeps a *pending entries list* (PEL). `XREADGROUP` moves an
entry into the PEL and it stays there until `XACK`. If the worker dies mid-event, the entry is
still in the PEL, and another worker takes it over with `XAUTOCLAIM` after an idle timeout.
`XINFO GROUPS` reports the group's `lag` directly, which becomes the queue-depth metric for
free.

What I turned down, and why:

| Option | Why not |
| --- | --- |
| Redis list (`LPUSH`/`BRPOP`) | `BRPOP` deletes the item at read time. A worker that dies one line later takes the event with it, which is precisely the failure mode the task asks about. |
| Kafka | The right answer for replay, retention and partition-level ordering at scale — and far too much operational surface for a service that runs `docker compose up`. It is where I would go at 10x (see below). |
| RabbitMQ | A good fit, and its per-message nacks beat Redis's PEL. But it is a second piece of infrastructure to run, and I already needed Redis for the rate cache. |
| Postgres as a queue (`SKIP LOCKED`) | Zero extra infrastructure and genuinely fine at 100/s. Rejected because ingest would then depend on the same database the consumer is writing to — one slow database and the HTTP endpoint starts failing. |

---

## Delivery semantics: at-least-once, with an idempotent consumer

**The queue is at-least-once. Storage is effectively exactly-once.**

Exactly-once *delivery* is not achievable across a network — the acknowledgement itself can be
lost. What is achievable is at-least-once delivery plus a consumer whose second execution has
no additional effect, and that is what this service does:

1. The worker never acknowledges a message before the row is committed. A crash in between
   leaves the entry in the PEL, so the event is redelivered rather than lost.
2. `transactions.id` **is** the producer's event id, and the write is a single
   `INSERT ... ON CONFLICT (id) DO NOTHING`. A redelivery collides with the row the first
   delivery wrote and becomes a no-op. The statement returns `rowcount` 1 for a real insert and
   0 for a duplicate, so the worker can report which happened.

Because the uniqueness check and the write are the *same* statement, two workers racing on the
same redelivered event cannot both insert — the database arbitrates, not application logic.
[`tests/test_dedup.py`](tests/test_dedup.py) fires eight concurrent deliveries of one event and
asserts exactly one `STORED` and one row.

**What I deliberately did not do:** put a `SETNX seen:{id}` check in Redis in front of the
database. It would be faster, and it would silently lose events — a crash between marking the
id in Redis and committing to PostgreSQL leaves an event that is marked as seen and was never
stored. The database constraint is the only place where "have I seen this?" and "is it stored?"
are the same question.

**First write wins.** If the same id arrives with a different amount, the stored row is not
updated. Transaction history should not mutate under a replay; a genuine correction should
arrive as its own event.

---

## Handling a failing downstream

Failures are split into two classes, and that classification is the whole retry policy
([`app/exceptions.py`](app/exceptions.py)):

* **`TransientError`** — the rate lookup timed out, the database is restarting. Worth retrying.
* **`PermanentError`** — an unknown currency, a payload that no longer parses. Retrying only
  burns the downstream.

A transient failure is retried in-process up to `MAX_ATTEMPTS` times with **exponential backoff
and full jitter** (`uniform(0, base · 2^(n-1))`, capped at `RETRY_MAX_DELAY`). Full jitter
rather than a fixed ramp because when a downstream recovers from an outage, every worker
otherwise retries in lockstep and knocks it straight back over.

If the attempts run out, the worker **does not acknowledge the message**. The entry stays in the
PEL, and the reclaim loop (`XAUTOCLAIM` every `RECLAIM_INTERVAL` seconds over entries idle for
`RECLAIM_IDLE_MS`) hands it to a worker again later. That is the outer retry loop, and it is
what makes a multi-minute outage survivable rather than just a multi-second one.

A permanent failure — or a message that has been delivered more than `MAX_DELIVERIES` times —
is moved to the `transactions.dlq` stream together with the reason and the delivery count, and
only then acknowledged. The `XADD` to the DLQ and the `XACK` go out in one pipeline, so an event
can never be acknowledged without having landed somewhere.

Observed behaviour with `RATES_FAILURE_RATE=1.0` (a total rate-provider outage), 15 events:

```
during the outage:  stored=0   pending=15  dlq=0   retries=30  rate_lookups{failed}=45
provider recovers:  stored=15  pending=0   dlq=0     (recovered by the reclaim loop, ~15s)
```

Nothing was lost, nothing was stored twice, and no operator had to intervene.

The API side has the matching property: if Redis is unreachable, `POST /events` returns **503**
rather than pretending to have accepted the event. Losing an event loudly at the edge, where the
producer can retry, beats losing it quietly.

---

## Metrics

Prometheus text format on `/metrics` (API) and on the worker's port 9100. Both processes export
the same metric names; Prometheus sums them across replicas.

| Metric | What it tells you |
| --- | --- |
| `events_received_total` | Events accepted by the API and queued. |
| `events_processed_total{result="stored"\|"duplicate"}` | Consumer throughput, and how much of it is redelivery. |
| `events_failed_total{reason}` | Failures by class: `rate_unavailable`, `database_unavailable`, `unknown_currency`, `invalid_event`. |
| `events_retried_total` | Retry attempts — the early warning that a downstream is degrading. |
| `events_dead_lettered_total` | Events parked in the DLQ. Should be zero; alert if it is not. |
| `event_processing_seconds` | Histogram of per-event latency including retries. |
| **`queue_lag_messages`** | **Entries published but not yet read by the group — the number to alert on.** |
| `queue_pending_messages` | Delivered but not yet acknowledged (in flight or stuck). |
| `queue_length_messages`, `queue_dlq_length_messages` | Stream sizes. |
| `rate_lookups_total{result}` | `cache_hit` / `fetched` / `failed` — cache effectiveness and downstream health. |

`GET /queue/stats` returns the queue numbers as JSON for a quick look without a Prometheus.

```bash
curl localhost:8000/queue/stats
# {"stream_length":10003,"pending":0,"lag":0,"dlq_length":0}

curl "http://$(docker compose port worker 9100)/metrics"
```

---

## Measured throughput

Docker Desktop on macOS, one API container, PostgreSQL 16 and Redis 7, on a laptop:

| Setup | Sustained processing rate |
| --- | --- |
| 1 worker | ~1,350 events/sec (21 ms per event at 32-way concurrency) |
| 3 workers (`--scale worker=3`) | ~3,300 events/sec — 15,800 events drained in 4.5 s |

The task's target is ~100/sec with bursts to ~1,000/sec, so a single worker has roughly 13x
headroom on the sustained rate and absorbs the burst without the queue growing. Ingest is not
the constraint: the API accepted 15,800 events in 0.7 s with zero errors while the workers were
still catching up — which is exactly what the queue is there for.

That last run also included 800 exact replays; 15,000 distinct rows were stored, which is the
dedup guarantee holding under concurrency across three independent workers.

Two things mattered for that number, and both are worth knowing about:

* **The database pool must be at least as large as the consumer's concurrency.** With
  `CONCURRENCY=32` against a 10-connection pool, throughput was 387/sec; matching them gave
  ~1,000/sec in the same test. `db_pool_size` therefore defaults to `concurrency` rather than a
  constant, so the two cannot drift apart by accident.
* **One `XACK` per batch, not per message.** Acknowledging each of 100 messages separately makes
  Redis the bottleneck long before PostgreSQL is. Every id in the batch has already had its row
  committed, so batching the ack costs nothing in safety.

---

## Trade-offs

**The one I would flag first: `GET /users/{id}/summary` recomputes its aggregate on every
request.** It is `SUM(amount_usd), COUNT(*)` over the user's rows, served by the
`(user_id, timestamp)` index. For a user with a few thousand transactions that is a sub-millisecond
index scan and the numbers are always exactly consistent with the stored rows — no cache to
invalidate, no counter that can drift away from the data it summarises, no second write in the
consumer's hot path. For a user with ten million rows it is a sequential scan and it will be
slow. I took the simple, always-correct version because correctness of money is worth more here
than the latency of an endpoint nobody has complained about yet, and because a running
aggregate is a straightforward thing to add later (below) but a very annoying thing to *remove*
once you find it disagreeing with the ledger.

Others, briefly:

* **Schema via `create_all`, not Alembic.** One table, and `docker compose up` stays a single
  step. A real deployment needs migrations; this is the first thing I would add.
* **`offset`-based pagination.** Simple and correct, and it gives clients a `total`. Deep offsets
  scan and discard rows, so at large histories this becomes keyset pagination on
  `(timestamp, id)`.
* **The FX source is a static in-process table** ([`app/rates.py`](app/rates.py)) with injectable
  latency and failures. The *shape* of the call — async, fallible, cached in Redis with a TTL —
  is what a real provider needs, so swapping it is one class.
* **Rates are cached for 5 minutes and the cached rate is used for conversion.** An event is
  converted at the rate the worker knew when it processed it, which means a replayed event and a
  fresh one can convert at slightly different rates. For per-transaction reporting that is fine;
  for anything that must reconcile against a ledger, the rate would have to be pinned to the
  event's timestamp instead. The rate actually used is stored on every row, so the conversion is
  always auditable after the fact.
* **A batch request is all-or-nothing on validation.** One malformed event rejects the whole
  batch with a 422 that names the offending index. Partial acceptance would need a per-item
  result object; rejecting loudly is easier for a producer to get right.

---

## What I would change at 10x load (~1k/sec sustained, ~10k/sec bursts)

Roughly in the order I would do it:

1. **Batch the inserts.** The consumer commits one transaction per event. Writing a batch as a
   single multi-row `INSERT ... ON CONFLICT DO NOTHING RETURNING id` turns 100 commits into one:
   measured on this stack, single-row inserts run ~7,800/sec and 100-row batches ~70,000/sec.
   The returned ids say which rows were new, so dedup accounting survives intact, and a batch
   that fails simply is not acknowledged — the at-least-once guarantee is unchanged. A failed
   batch falls back to per-event processing so one poison row cannot stall its ninety-nine
   neighbours.
2. **Maintain a running per-user aggregate** so `/summary` becomes a primary-key lookup. Same
   transaction as the insert (`INSERT ... ON CONFLICT (user_id) DO UPDATE SET total = total +
   excluded.total`), which keeps it exactly consistent with the rows and keeps dedup meaningful —
   a duplicate that inserts nothing must also add nothing. Contention on hot users is the thing
   to watch; the escape hatch is to write per-user deltas and fold them periodically.
3. **Partition `transactions` by month** and add a retention policy. The table is append-only and
   every query is time-ranged, so partition pruning does real work, and dropping a partition
   beats deleting rows. Switch the listing endpoint to keyset pagination at the same time.
4. **Split the stream across partitions** — `transactions:{0..N}`, keyed by `hash(user_id)` — once
   one consumer group stops keeping up. Same-user events then stay ordered within a partition,
   which matters if the running aggregate arrives.
5. **Move the rate lookup out of the per-event path.** A background refresher writes the rate
   table into Redis on a schedule and the consumer only ever reads a warm cache, so a provider
   outage stops being something the event path has to retry around at all.
6. **Put pgbouncer in front of PostgreSQL** in transaction-pooling mode, so worker count stops
   being bounded by `max_connections`.
7. **At that point, reconsider Kafka.** Days of retention, replay from an offset, partition-level
   ordering and consumer-lag tooling that operators already know. The rewrite is mostly the queue
   module — the processor, the dedup and the schema are unchanged, which is why they live behind
   [`app/queue.py`](app/queue.py).

Operationally I would also add: Alembic migrations, `/metrics` scraped into Prometheus with
alerts on `queue_lag_messages` and `events_dead_lettered_total`, a DLQ replay command, and
structured JSON logs with the event id as a correlation field.

---

## Configuration

Everything is an environment variable; see [`app/config.py`](app/config.py) and
[`.env.example`](.env.example).

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql+asyncpg://events:events@localhost:5432/events` | PostgreSQL DSN. |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis DSN, used for the queue and the rate cache. |
| `DB_POOL_SIZE` | `concurrency` | Pooled connections per process. Keep it ≥ `CONCURRENCY`. |
| `DB_MAX_OVERFLOW` | `10` | Extra connections above the pool under burst. |
| `BATCH_SIZE` | `100` | Messages per `XREADGROUP`. |
| `CONCURRENCY` | `32` | Events processed in parallel by one worker. |
| `MAX_ATTEMPTS` | `3` | In-process attempts before the message is left pending. |
| `RETRY_BASE_DELAY` / `RETRY_MAX_DELAY` | `0.2` / `5.0` | Backoff floor and ceiling, in seconds. |
| `RECLAIM_IDLE_MS` | `30000` | How long a pending message may sit before another worker takes it. |
| `MAX_DELIVERIES` | `5` | Deliveries before a message is dead-lettered. |
| `RATES_CACHE_TTL` | `300` | Rate cache TTL in seconds. |
| `RATES_FAILURE_RATE` | `0.0` | Share of rate lookups that fail. Set to `0.5` to watch the retry path. |
| `CONSUMER_NAME` | container hostname | Identity within the consumer group; unique per replica. |
| `LOG_LEVEL` | `INFO` | Root log level. |

Scale the consumer horizontally — each replica gets its own name in the group automatically:

```bash
docker compose up --scale worker=3
```

One thing to keep an eye on when you do: connections are per process, so
`replicas x (DB_POOL_SIZE + DB_MAX_OVERFLOW) + api` has to fit inside PostgreSQL's
`max_connections`. Three workers at the defaults want 126 of them, which is why the compose file
raises the server's limit to 300. Past a handful of workers the answer stops being a bigger
number and becomes pgbouncer in transaction-pooling mode.

---

## Layout

```
app/
  main.py         FastAPI app factory, lifespan, error handler
  worker.py       Worker entrypoint: waits for deps, starts the consumer and its metrics server
  config.py       All settings, one place
  schemas.py      Request/response models and input validation
  models.py       The transactions table (id = the producer's event id)
  db.py           Async engine, session factory, schema bootstrap
  redis_client.py Shared Redis pool
  queue.py        Redis Streams: publish, read, ack, reclaim, dead-letter, stats
  consumer.py     The consume / reclaim / metrics loops and their failure handling
  processor.py    Deduplicate, convert, store — the core of one event
  money.py        Decimal conversion and rounding (pure)
  rates.py        Rate lookup: cache-aside over a pluggable, fallible source
  repository.py   Every SQL statement the service issues
  retry.py        Exponential backoff with full jitter
  exceptions.py   Transient vs permanent — the retry policy in one file
  metrics.py      Prometheus counters, gauges and histograms
  api/            events.py, users.py, system.py
tests/            118 tests: dedup, conversion, retry, queue, consumer, HTTP
scripts/
  load_test.py    Synthetic traffic generator with a drain timer
```

### Where to look first

* Deduplication — [`app/processor.py`](app/processor.py) and
  [`app/repository.py`](app/repository.py) (`insert_if_absent`).
* Not losing events — [`app/consumer.py`](app/consumer.py) (`_handle_one`, `_reclaim_loop`).
* Currency conversion — [`app/money.py`](app/money.py) and [`app/rates.py`](app/rates.py).
