"""High level orchestration for crawling repositories."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import AppConfig, RateLimitInfo
from .db import Database
from .github_client import GitHubGraphQLClient
from .graphql_queries import REPOSITORY_SEARCH_QUERY
from .models import RepositoryRecord
from .rate_limiter import RateLimiter
from .partitioner import RangePlan, RangePlanner, TimeRange

LOGGER = logging.getLogger(__name__)
UTC = timezone.utc


@dataclass(slots=True)
class CrawlResult:
    repositories_written: int
    rate_limit_remaining: int | None
    finished_at: datetime


class GitHubCrawler:
    """Fetches repositories from GitHub and stores them in Postgres."""

    def __init__(self, config: AppConfig, client: GitHubGraphQLClient, database: Database) -> None:
        self._config = config
        self._client = client
        self._database = database
        self._semaphore = asyncio.Semaphore(config.github.max_concurrency)
        self._seen_ids: set[str] = set()
        self._seen_lock = asyncio.Lock()
        self._rate_limiter = RateLimiter()
        self._latest_rate_limit: RateLimitInfo | None = None

    async def crawl(self) -> CrawlResult:
        await self._database.connect()

        now = datetime.now(tz=UTC)
        initial_range = TimeRange(self._config.crawl.range_start, now)
        planner = RangePlanner(self._client, self._config.crawl.search_result_limit)
        LOGGER.info(
            "Planning ranges for %s repositories between %s and %s",
            self._config.crawl.target_repository_count,
            initial_range.start.isoformat(),
            initial_range.end.isoformat(),
        )
        plans = await planner.plan(initial_range, self._config.crawl.target_repository_count)
        LOGGER.info("Prepared %s ranges", len(plans))

        queue: asyncio.Queue[RepositoryRecord | None] = asyncio.Queue()
        producers = [
            asyncio.create_task(self._produce(plan, queue))
            for plan in plans
        ]
        consumer = asyncio.create_task(self._consume(queue))

        await asyncio.gather(*producers)
        await queue.put(None)
        written = await consumer
        LOGGER.info("Crawl finished with %s repositories persisted", written)
        return CrawlResult(
            repositories_written=written,
            rate_limit_remaining=(self._latest_rate_limit.remaining if self._latest_rate_limit else None),
            finished_at=datetime.now(tz=UTC),
        )

    async def _produce(self, plan: RangePlan, queue: asyncio.Queue[RepositoryRecord | None]) -> None:
        remaining = plan.requested_results
        cursor: str | None = None
        fetched_count = 0
        LOGGER.debug(
            "Fetching up to %s repositories for range %s - %s",
            plan.requested_results,
            plan.time_range.start,
            plan.time_range.end,
        )
        while remaining > 0:
            page_size = min(self._config.github.page_size, remaining)
            data = await self._fetch_page(plan.time_range, page_size, cursor)
            search = data["search"]
            page_info = search["pageInfo"]
            cursor = page_info.get("endCursor")
            nodes = search.get("nodes") or []
            if not nodes:
                break
            fetched_at = datetime.now(tz=UTC)
            for node in nodes:
                if remaining <= 0:
                    break
                if not isinstance(node, dict):
                    continue
                record = RepositoryRecord.from_graphql(node, fetched_at=fetched_at)
                if await self._mark_seen(record.node_id):
                    await queue.put(record)
                    remaining -= 1
                    fetched_count += 1
                else:
                    LOGGER.debug("Skipping duplicate repository %s", record.node_id)
            if not page_info.get("hasNextPage"):
                break
        LOGGER.debug(
            "Range %s - %s produced %s repositories", plan.time_range.start, plan.time_range.end, fetched_count
        )

    async def _fetch_page(self, time_range: TimeRange, page_size: int, cursor: str | None) -> dict[str, Any]:
        variables = {
            "query": time_range.to_search_query(),
            "first": page_size,
            "after": cursor,
        }
        await self._rate_limiter.acquire()
        try:
            async with self._semaphore:
                response = await self._client.execute(REPOSITORY_SEARCH_QUERY, variables)
        except Exception:
            await self._rate_limiter.reset()
            raise
        if response.rate_limit:
            await self._rate_limiter.record(response.rate_limit)
            self._latest_rate_limit = response.rate_limit
        return response.data

    async def _consume(self, queue: asyncio.Queue[RepositoryRecord | None]) -> int:
        buffer: list[RepositoryRecord] = []
        written = 0
        while True:
            item = await queue.get()
            if item is None:
                break
            buffer.append(item)
            if len(buffer) >= self._config.database.batch_size:
                await self._database.upsert_repositories(buffer)
                written += len(buffer)
                buffer.clear()
        if buffer:
            await self._database.upsert_repositories(buffer)
            written += len(buffer)
        return written

    async def _mark_seen(self, node_id: str) -> bool:
        async with self._seen_lock:
            if node_id in self._seen_ids:
                return False
            self._seen_ids.add(node_id)
            return True


__all__ = ["GitHubCrawler", "CrawlResult"]
