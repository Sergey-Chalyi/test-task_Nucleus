"""HTTP layer tests, driven in-process through httpx's ASGI transport."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import quote

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.dependencies import (
    get_queue,
    get_redis_dep,
    get_repository,
    get_settings_dep,
)
from app.main import create_app
from app.processor import EventProcessor
from app.queue import EventQueue
from app.repository import TransactionRepository
from tests.conftest import BASE_TIME, make_event


@pytest_asyncio.fixture
async def client(session_factory, redis, settings, rate_provider):
    """An app wired to the in-memory database and fake Redis.

    The lifespan hook is intentionally not run: it would create tables in the
    *real* database configured by the environment.
    """
    app = create_app()

    async def override_repository():
        async with session_factory() as session:
            yield TransactionRepository(session)

    app.dependency_overrides[get_settings_dep] = lambda: settings
    app.dependency_overrides[get_redis_dep] = lambda: redis
    app.dependency_overrides[get_queue] = lambda: EventQueue(redis, settings)
    app.dependency_overrides[get_repository] = override_repository

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture
async def seeded(session_factory, rate_provider):
    """Store a handful of transactions for two users."""
    processor = EventProcessor(session_factory, rate_provider)
    for i in range(5):
        await processor.process(
            make_event(f"evt-{i}", user_id="user-1", amount="10.00", minutes_offset=i)
        )
    await processor.process(
        make_event("evt-other", user_id="user-2", amount="1000.00")
    )
    return processor


VALID_EVENT = {
    "id": "evt-1",
    "user_id": "user-1",
    "amount": "125.50",
    "currency": "EUR",
    "timestamp": "2026-09-05T10:15:00Z",
}


class TestReceiveEvent:
    async def test_accepts_a_valid_event(self, client, redis, settings):
        response = await client.post("/events", json=VALID_EVENT)
        assert response.status_code == 202
        body = response.json()
        assert body["id"] == "evt-1"
        assert body["status"] == "queued"
        # 202, and the event really is on the queue.
        assert await redis.xlen(settings.stream_name) == 1

    async def test_lowercase_currency_is_normalised(self, client, redis, settings):
        await client.post("/events", json={**VALID_EVENT, "currency": "eur"})
        _id, fields = (await redis.xrange(settings.stream_name))[0]
        assert '"currency":"EUR"' in fields["payload"]

    async def test_naive_timestamp_is_treated_as_utc(self, client, redis, settings):
        await client.post(
            "/events", json={**VALID_EVENT, "timestamp": "2026-09-05T10:15:00"}
        )
        _id, fields = (await redis.xrange(settings.stream_name))[0]
        assert "10:15:00Z" in fields["payload"] or "+00:00" in fields["payload"]

    @pytest.mark.parametrize(
        "override",
        [
            {"id": ""},
            {"user_id": ""},
            {"currency": "EU"},
            {"currency": "EURO"},
            {"currency": "12$"},
            {"amount": "not-a-number"},
            {"timestamp": "yesterday"},
        ],
    )
    async def test_rejects_invalid_events(self, client, override, redis, settings):
        response = await client.post("/events", json={**VALID_EVENT, **override})
        assert response.status_code == 422
        assert await redis.xlen(settings.stream_name) == 0

    async def test_rejects_missing_fields(self, client):
        assert (await client.post("/events", json={"id": "x"})).status_code == 422

    async def test_rejects_unknown_fields(self, client):
        response = await client.post("/events", json={**VALID_EVENT, "fee": "1.00"})
        assert response.status_code == 422

    async def test_queue_outage_returns_503(self, client, settings):
        from redis.exceptions import ConnectionError as RedisConnectionError

        class BrokenQueue(EventQueue):
            async def publish(self, event):
                raise RedisConnectionError("redis is down")

        client._transport.app.dependency_overrides[get_queue] = lambda: BrokenQueue(
            None, settings
        )
        response = await client.post("/events", json=VALID_EVENT)
        assert response.status_code == 503


class TestReceiveBatch:
    async def test_accepts_a_batch(self, client, redis, settings):
        payload = [{**VALID_EVENT, "id": f"evt-{i}"} for i in range(10)]
        response = await client.post("/events/batch", json=payload)
        assert response.status_code == 202
        assert response.json()["accepted"] == 10
        assert await redis.xlen(settings.stream_name) == 10

    async def test_rejects_an_empty_batch(self, client):
        assert (await client.post("/events/batch", json=[])).status_code == 422

    async def test_rejects_an_oversized_batch(self, client):
        from app.api.events import MAX_BATCH_SIZE

        payload = [{**VALID_EVENT, "id": f"e{i}"} for i in range(MAX_BATCH_SIZE + 1)]
        assert (await client.post("/events/batch", json=payload)).status_code == 413

    async def test_one_bad_event_rejects_the_whole_batch(self, client, redis, settings):
        payload = [VALID_EVENT, {**VALID_EVENT, "id": "evt-2", "currency": "X"}]
        assert (await client.post("/events/batch", json=payload)).status_code == 422
        assert await redis.xlen(settings.stream_name) == 0


class TestSummary:
    async def test_totals_and_counts(self, client, seeded):
        response = await client.get("/users/user-1/summary")
        assert response.status_code == 200
        body = response.json()
        assert body["user_id"] == "user-1"
        assert body["transaction_count"] == 5
        # 5 x 10.00 EUR at 1.085
        assert Decimal(body["total_usd"]) == Decimal("54.2500")

    async def test_users_are_isolated(self, client, seeded):
        body = (await client.get("/users/user-2/summary")).json()
        assert body["transaction_count"] == 1
        assert Decimal(body["total_usd"]) == Decimal("1085.0000")

    async def test_unknown_user_is_an_empty_summary_not_a_404(self, client, seeded):
        response = await client.get("/users/nobody/summary")
        assert response.status_code == 200
        assert response.json() == {
            "user_id": "nobody",
            "total_usd": "0.0000",
            "transaction_count": 0,
        }


class TestTransactionList:
    async def test_returns_newest_first(self, client, seeded):
        body = (await client.get("/users/user-1/transactions")).json()
        assert body["total"] == 5
        assert len(body["items"]) == 5
        timestamps = [item["timestamp"] for item in body["items"]]
        assert timestamps == sorted(timestamps, reverse=True)

    async def test_includes_the_converted_amount(self, client, seeded):
        item = (await client.get("/users/user-1/transactions")).json()["items"][0]
        assert item["currency"] == "EUR"
        assert Decimal(item["amount"]) == Decimal("10")
        assert Decimal(item["amount_usd"]) == Decimal("10.85")

    async def test_paginates(self, client, seeded):
        first = (await client.get("/users/user-1/transactions?limit=2")).json()
        assert len(first["items"]) == 2
        assert first["total"] == 5
        assert first["has_more"] is True

        last = (await client.get("/users/user-1/transactions?limit=2&offset=4")).json()
        assert len(last["items"]) == 1
        assert last["has_more"] is False

    async def test_pages_do_not_overlap(self, client, seeded):
        seen = []
        for offset in (0, 2, 4):
            page = (
                await client.get(f"/users/user-1/transactions?limit=2&offset={offset}")
            ).json()
            seen.extend(item["id"] for item in page["items"])
        assert len(seen) == len(set(seen)) == 5

    async def test_page_size_is_capped(self, client, seeded, settings):
        body = (await client.get("/users/user-1/transactions?limit=100000")).json()
        assert body["limit"] == settings.api_max_page_size

    async def test_filters_by_from(self, client, seeded):
        cutoff = quote((BASE_TIME + timedelta(minutes=3)).isoformat())
        body = (await client.get(f"/users/user-1/transactions?from={cutoff}")).json()
        assert body["total"] == 2  # offsets 3 and 4

    async def test_filters_by_to(self, client, seeded):
        cutoff = quote((BASE_TIME + timedelta(minutes=1)).isoformat())
        body = (await client.get(f"/users/user-1/transactions?to={cutoff}")).json()
        assert body["total"] == 2  # offsets 0 and 1, upper bound inclusive

    async def test_filters_by_range(self, client, seeded):
        start = quote((BASE_TIME + timedelta(minutes=1)).isoformat())
        end = quote((BASE_TIME + timedelta(minutes=3)).isoformat())
        body = (
            await client.get(f"/users/user-1/transactions?from={start}&to={end}")
        ).json()
        assert body["total"] == 3

    async def test_inverted_range_is_a_400(self, client, seeded):
        start = quote((BASE_TIME + timedelta(minutes=3)).isoformat())
        end = quote(BASE_TIME.isoformat())
        response = await client.get(
            f"/users/user-1/transactions?from={start}&to={end}"
        )
        assert response.status_code == 400

    async def test_empty_range_returns_an_empty_page(self, client, seeded):
        future = quote(datetime(2030, 1, 1, tzinfo=UTC).isoformat())
        body = (await client.get(f"/users/user-1/transactions?from={future}")).json()
        assert body["items"] == []
        assert body["total"] == 0
        assert body["has_more"] is False

    async def test_rejects_a_negative_offset(self, client, seeded):
        response = await client.get("/users/user-1/transactions?offset=-1")
        assert response.status_code == 422


class TestSystemEndpoints:
    async def test_live(self, client):
        assert (await client.get("/live")).json() == {"status": "ok"}

    async def test_root_banner(self, client):
        assert (await client.get("/")).json()["service"] == "transaction-event-service"

    async def test_metrics_are_prometheus_text(self, client):
        await client.post("/events", json=VALID_EVENT)
        response = await client.get("/metrics")
        assert response.status_code == 200
        assert "events_received_total" in response.text
        assert "queue_length_messages" in response.text

    async def test_queue_stats(self, client):
        await client.post("/events", json=VALID_EVENT)
        body = (await client.get("/queue/stats")).json()
        assert body["stream_length"] == 1
        assert body["dlq_length"] == 0

    async def test_health_reports_a_broken_database(self, client, monkeypatch):
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def broken_scope():
            raise OSError("connection refused")
            yield  # pragma: no cover

        monkeypatch.setattr("app.api.system.session_scope", broken_scope)
        response = await client.get("/health")
        assert response.status_code == 503
        assert response.json() == {
            "status": "degraded",
            "database": "unavailable",
            "redis": "ok",
        }

    async def test_openapi_schema_is_served(self, client):
        schema = (await client.get("/openapi.json")).json()
        assert "/users/{user_id}/summary" in schema["paths"]
        assert "/events" in schema["paths"]
