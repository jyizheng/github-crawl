from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from github_crawl.config import RateLimitInfo
from github_crawl.rate_limiter import RateLimiter


def test_rate_limiter_acquire_consumes_estimated_budget():
    limiter = RateLimiter()
    now = datetime.now(timezone.utc)

    async def scenario() -> int | None:
        await limiter.record(RateLimitInfo(cost=30, remaining=40, reset_at=now))
        await limiter.acquire()
        return await limiter.remaining()

    remaining = asyncio.run(scenario())
    assert remaining == 24


def test_rate_limiter_waits_when_budget_exhausted(monkeypatch):
    limiter = RateLimiter(minimum_sleep=0.0)
    reset_at = datetime.now(timezone.utc) + timedelta(seconds=5)

    slept = False

    async def fake_sleep(duration: float) -> None:  # pragma: no cover - patched behaviour
        nonlocal slept
        slept = True

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def scenario() -> int | None:
        await limiter.record(RateLimitInfo(cost=1, remaining=0, reset_at=reset_at))
        await limiter.acquire()
        return await limiter.remaining()

    remaining = asyncio.run(scenario())

    assert slept is True
    assert remaining is None


def test_rate_limiter_reset_clears_state():
    limiter = RateLimiter()
    now = datetime.now(timezone.utc)

    async def scenario() -> int | None:
        await limiter.record(RateLimitInfo(cost=1, remaining=5, reset_at=now))
        await limiter.reset()
        return await limiter.remaining()

    remaining = asyncio.run(scenario())

    assert remaining is None
