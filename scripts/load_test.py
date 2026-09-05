#!/usr/bin/env python3
"""Fire synthetic traffic at a running API and report end-to-end throughput.

    python scripts/load_test.py --rate 1000 --seconds 10

Publishes events at roughly `--rate` per second, then waits until the queue
has drained and prints how long the consumer took to catch up.
"""

import argparse
import asyncio
import random
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "SEK", "PLN", "UAH"]


def build_event(user_pool: int, duplicate_of: str | None = None) -> dict:
    """One random event, or an exact replay of an earlier one."""
    event_id = duplicate_of or str(uuid.uuid4())
    return {
        "id": event_id,
        "user_id": f"user-{random.randrange(user_pool)}",
        "amount": f"{random.uniform(1, 5000):.2f}",
        "currency": random.choice(CURRENCIES),
        "timestamp": (
            datetime.now(UTC) - timedelta(seconds=random.randrange(86400))
        ).isoformat(),
    }


async def publish(
    client: httpx.AsyncClient, rate: int, seconds: int, batch: int,
    users: int, duplicate_ratio: float,
) -> tuple[int, int]:
    """Publish for `seconds` at ~`rate` events/sec. Returns (sent, failed)."""
    sent = failed = 0
    seen: list[str] = []
    batches_per_second = max(rate // batch, 1)
    interval = 1.0 / batches_per_second

    deadline = time.perf_counter() + seconds
    next_send = time.perf_counter()
    while time.perf_counter() < deadline:
        payload = []
        for _ in range(batch):
            # Replay a share of earlier ids so dedup has something to do.
            replay = seen and random.random() < duplicate_ratio
            event = build_event(users, duplicate_of=random.choice(seen) if replay else None)
            if not replay:
                seen.append(event["id"])
            payload.append(event)

        try:
            response = await client.post("/events/batch", json=payload, timeout=10.0)
            if response.status_code == 202:
                sent += len(payload)
            else:
                failed += len(payload)
                print(f"  ! {response.status_code}: {response.text[:120]}")
        except httpx.HTTPError as exc:
            failed += len(payload)
            print(f"  ! {exc}")

        next_send += interval
        delay = next_send - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)

    return sent, failed


async def wait_for_drain(client: httpx.AsyncClient, timeout: float) -> float | None:
    """Poll `/queue/stats` until nothing is queued or in flight."""
    start = time.perf_counter()
    while time.perf_counter() - start < timeout:
        stats = (await client.get("/queue/stats")).json()
        if stats["lag"] == 0 and stats["pending"] == 0:
            return time.perf_counter() - start
        await asyncio.sleep(0.25)
    return None


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--rate", type=int, default=1000, help="events per second")
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--batch", type=int, default=100, help="events per request")
    parser.add_argument("--users", type=int, default=100, help="size of the user pool")
    parser.add_argument(
        "--duplicate-ratio", type=float, default=0.05,
        help="share of events that replay an earlier id",
    )
    parser.add_argument("--drain-timeout", type=float, default=120.0)
    args = parser.parse_args()

    async with httpx.AsyncClient(base_url=args.url) as client:
        health = await client.get("/health")
        print(f"health: {health.json()}")

        print(f"publishing ~{args.rate}/s for {args.seconds}s ...")
        started = time.perf_counter()
        sent, failed = await publish(
            client, args.rate, args.seconds, args.batch, args.users,
            args.duplicate_ratio,
        )
        elapsed = time.perf_counter() - started
        print(f"sent {sent} events in {elapsed:.1f}s ({sent / elapsed:.0f}/s), "
              f"{failed} failed")

        print("waiting for the consumer to drain the queue ...")
        drained = await wait_for_drain(client, args.drain_timeout)
        if drained is None:
            print(f"! still draining after {args.drain_timeout}s: "
                  f"{(await client.get('/queue/stats')).json()}")
        else:
            print(f"drained in {drained:.1f}s")

        print(f"queue: {(await client.get('/queue/stats')).json()}")
        summary = await client.get("/users/user-0/summary")
        print(f"sample summary: {summary.json()}")


if __name__ == "__main__":
    asyncio.run(main())
