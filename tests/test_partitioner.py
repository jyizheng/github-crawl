from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from github_crawl.config import UTC
from github_crawl.partitioner import RangePlanner, TimeRange


class FakeClient:
    def __init__(self, counts: dict[tuple[str, str], int]) -> None:
        self._counts = counts

    async def execute(self, query: str, variables: dict[str, str]):
        start, end = _extract_range(variables["query"])
        count = self._counts.get((start, end), 0)
        data = {"search": {"repositoryCount": count}}
        return SimpleNamespace(data=data, rate_limit=None)


def _extract_range(query: str) -> tuple[str, str]:
    start = end = ""
    for token in query.split():
        if token.startswith("created:>="):
            start = token.split(":>=", 1)[1]
        elif token.startswith("created:<"):
            end = token.split(":<", 1)[1]
    return start, end


def test_range_planner_splits_until_limit():
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 5, tzinfo=UTC)
    initial_range = TimeRange(start, end)

    halves = initial_range.split()
    quarters = [segment.split() for segment in halves]

    counts: dict[tuple[str, str], int] = {
        (initial_range.start.strftime("%Y-%m-%dT%H:%M:%SZ"), initial_range.end.strftime("%Y-%m-%dT%H:%M:%SZ")): 5000,
    }
    for pair in halves:
        counts[(pair.start.strftime("%Y-%m-%dT%H:%M:%SZ"), pair.end.strftime("%Y-%m-%dT%H:%M:%SZ"))] = 2000
    for pair in quarters:
        for segment in pair:
            counts[(segment.start.strftime("%Y-%m-%dT%H:%M:%SZ"), segment.end.strftime("%Y-%m-%dT%H:%M:%SZ"))] = 600

    client = FakeClient(counts)
    planner = RangePlanner(client, search_limit=1000)
    plans = asyncio.run(planner.plan(initial_range, total_needed=2000))

    assert sum(plan.requested_results for plan in plans) == 2000
    assert all(plan.available_results <= 1000 for plan in plans)


def test_range_planner_respects_total_needed():
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    initial_range = TimeRange(start, end)

    key = (start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ"))
    client = FakeClient({key: 800})
    planner = RangePlanner(client, search_limit=1000)
    plans = asyncio.run(planner.plan(initial_range, total_needed=500))


def test_range_planner_clamps_unsplittable_range(caplog):
    start = datetime(2025, 10, 2, 5, 54, 1, 358998, tzinfo=UTC)
    end = datetime(2025, 10, 2, 5, 54, 2, 402525, tzinfo=UTC)
    initial_range = TimeRange(start, end)

    key = (start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ"))
    client = FakeClient({key: 274650407})
    planner = RangePlanner(client, search_limit=1000)

    with caplog.at_level("WARNING"):
        plans = asyncio.run(planner.plan(initial_range, total_needed=10))

    assert len(plans) == 1
    assert plans[0].requested_results == 10
    assert plans[0].available_results == 1000
    assert "clamping to limit" in caplog.text


def test_time_range_split_in_half():
    start = datetime(2023, 12, 31, tzinfo=UTC)
    end = datetime(2024, 1, 2, tzinfo=UTC)
    time_range = TimeRange(start, end)
    first, second = time_range.split()

    assert first.start == start
    assert first.end == second.start
    assert second.end == end
    assert first.duration == second.duration
