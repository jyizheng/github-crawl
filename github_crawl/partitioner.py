"""Logic for partitioning GitHub repositories into crawlable ranges."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Sequence

from .config import UTC
from .github_client import GitHubGraphQLClient
from .graphql_queries import REPOSITORY_COUNT_QUERY


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class TimeRange:
    """Represents a half-open interval [start, end) in UTC."""

    start: datetime
    end: datetime

    def split(self) -> tuple["TimeRange", "TimeRange"]:
        """Split the range into two equally sized halves."""

        delta = self.end - self.start
        midpoint = self.start + delta / 2
        if midpoint <= self.start or midpoint >= self.end:
            raise ValueError("TimeRange is too small to split further")
        first = TimeRange(self.start, midpoint)
        second = TimeRange(midpoint, self.end)
        return first, second

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    @property
    def can_split(self) -> bool:
        return self.duration >= timedelta(seconds=2)

    def to_search_query(self) -> str:
        start = self.start.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = self.end.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"created:>={start} created:<{end} is:public sort:created-asc"


@dataclass(slots=True)
class RangePlan:
    time_range: TimeRange
    requested_results: int
    available_results: int


class RangePlanner:
    """Creates search ranges that respect GitHub's search result limits."""

    def __init__(self, client: GitHubGraphQLClient, search_limit: int) -> None:
        self._client = client
        self._search_limit = search_limit

    async def plan(self, initial_range: TimeRange, total_needed: int) -> list[RangePlan]:
        """Plan ranges that together yield ``total_needed`` repositories."""

        planned: list[RangePlan] = []
        stack: list[tuple[TimeRange, int | None]] = [(initial_range, None)]
        remaining = total_needed

        while stack and remaining > 0:
            current, known_count = stack.pop()
            count = known_count if known_count is not None else await self._count(current)
            if count == 0:
                continue
            if count > self._search_limit:
                if not current.can_split:
                    LOGGER.warning(
                        "Search result count %s exceeds limit %s for unsplittable range %s - %s; clamping to limit.",
                        count,
                        self._search_limit,
                        current.start.isoformat(),
                        current.end.isoformat(),
                    )
                    count = self._search_limit
                else:
                    older, newer = current.split()
                    older_count = await self._count(older)
                    newer_count = await self._count(newer)
                    max_available = min(count, self._search_limit)
                    if older_count + newer_count < max_available:
                        count = max_available
                    else:
                        stack.append((older, older_count))
                        stack.append((newer, newer_count))
                        continue
            take = min(count, remaining)
            planned.append(RangePlan(time_range=current, requested_results=take, available_results=count))
            remaining -= take

        return planned

    async def _count(self, time_range: TimeRange) -> int:
        query = time_range.to_search_query()
        result = await self._client.execute(REPOSITORY_COUNT_QUERY, {"query": query})
        return int(result.data["search"]["repositoryCount"])


def flatten_ranges(plans: Sequence[RangePlan]) -> Iterable[TimeRange]:
    for plan in plans:
        yield plan.time_range


__all__ = ["RangePlanner", "RangePlan", "TimeRange", "flatten_ranges"]
