"""Helpers for coordinating GitHub GraphQL rate limits."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from math import ceil

from .config import RateLimitInfo, UTC

LOGGER = logging.getLogger(__name__)


class RateLimiter:
    """Async rate limit coordinator using GitHub's rate limit responses."""

    def __init__(self, *, minimum_sleep: float = 0.05) -> None:
        self._lock = asyncio.Lock()
        self._info: RateLimitInfo | None = None
        self._estimated_cost: float = 1.0
        self._minimum_sleep = max(minimum_sleep, 0.0)

    async def acquire(self) -> None:
        """Wait until enough budget is available for the next GraphQL call."""

        while True:
            async with self._lock:
                info = self._info
                if info is None:
                    return
                estimated_cost = max(1, ceil(self._estimated_cost))
                if info.remaining >= estimated_cost:
                    info.remaining -= estimated_cost
                    return
                remaining = info.remaining
                reset_at = info.reset_at

            delay = (reset_at - datetime.now(tz=UTC)).total_seconds()
            delay = max(delay, self._minimum_sleep)
            LOGGER.warning(
                "GitHub rate limit low (%s remaining); sleeping %.2fs until reset",
                remaining,
                delay,
            )
            await asyncio.sleep(delay)
            async with self._lock:
                if self._info is info:
                    self._info = None

    async def record(self, info: RateLimitInfo) -> None:
        """Update the limiter with the latest rate limit payload."""

        async with self._lock:
            # Store a fresh copy to avoid mutating the caller's data.
            self._info = RateLimitInfo(
                cost=info.cost,
                remaining=info.remaining,
                reset_at=info.reset_at,
            )
            if info.cost > 0:
                self._estimated_cost = max(1.0, (self._estimated_cost * 0.5) + (info.cost * 0.5))

    async def reset(self) -> None:
        """Clear cached rate limit information after a failed request."""

        async with self._lock:
            self._info = None

    async def remaining(self) -> int | None:
        """Return the last known remaining budget, if any."""

        async with self._lock:
            return self._info.remaining if self._info else None


__all__ = ["RateLimiter"]
