"""Application configuration helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, PositiveInt


UTC = timezone.utc


class GitHubSettings(BaseModel):
    """Configuration options for the GitHub GraphQL API."""

    token: str | None = Field(default=None, description="Personal access token or GitHub Actions token.")
    graphql_url: str = Field(default="https://api.github.com/graphql")
    max_concurrency: PositiveInt = Field(default=12, description="Maximum concurrent GraphQL requests.")
    page_size: PositiveInt = Field(default=100, le=100, description="Number of repositories fetched per GraphQL request.")
    max_retries: PositiveInt = Field(default=6, description="Maximum number of retries per GraphQL request.")
    initial_backoff: float = Field(default=1.0, ge=0.1, description="Initial exponential backoff in seconds.")
    max_backoff: float = Field(default=30.0, ge=1.0, description="Maximum delay for exponential backoff in seconds.")
    request_timeout: float = Field(default=40.0, ge=1.0, description="Timeout for a single HTTP request in seconds.")


class DatabaseSettings(BaseModel):
    """Configuration for connecting to Postgres."""

    dsn: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/github_crawl",
        description="Database connection string.",
    )
    statement_timeout: float = Field(default=60.0, ge=1.0, description="Statement timeout in seconds.")
    batch_size: PositiveInt = Field(default=500, description="Number of rows inserted per batch.")


class CrawlSettings(BaseModel):
    """Tunable parameters for the crawling algorithm."""

    target_repository_count: PositiveInt = Field(
        default=100_000, description="Number of repositories to collect."
    )
    search_result_limit: PositiveInt = Field(
        default=1_000,
        description="Maximum number of repositories accessible for a single GraphQL search query.",
    )
    range_start: datetime = Field(
        default=datetime(2008, 1, 1, tzinfo=UTC),
        description="Lower bound for the creation timestamp partitioner.",
    )


class AppConfig(BaseModel):
    """Root configuration container."""

    github: GitHubSettings = Field(default_factory=GitHubSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    crawl: CrawlSettings = Field(default_factory=CrawlSettings)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None, overrides: dict[str, Any] | None = None) -> "AppConfig":
        """Construct a configuration object from environment variables."""

        env = env or os.environ
        overrides = overrides or {}

        github = GitHubSettings(
            token=overrides.get("github_token") or env.get("GITHUB_TOKEN") or env.get("GH_TOKEN"),
            graphql_url=overrides.get("github_graphql_url") or env.get("GITHUB_GRAPHQL_URL") or "https://api.github.com/graphql",
            max_concurrency=int(overrides.get("github_max_concurrency") or env.get("GITHUB_MAX_CONCURRENCY", 12)),
            page_size=int(overrides.get("github_page_size") or env.get("GITHUB_PAGE_SIZE", 100)),
            max_retries=int(overrides.get("github_max_retries") or env.get("GITHUB_MAX_RETRIES", 6)),
            initial_backoff=float(overrides.get("github_initial_backoff") or env.get("GITHUB_INITIAL_BACKOFF", 1.0)),
            max_backoff=float(overrides.get("github_max_backoff") or env.get("GITHUB_MAX_BACKOFF", 30.0)),
            request_timeout=float(overrides.get("github_request_timeout") or env.get("GITHUB_REQUEST_TIMEOUT", 40.0)),
        )

        database = DatabaseSettings(
            dsn=overrides.get("database_dsn") or env.get("DATABASE_DSN") or env.get("POSTGRES_DSN") or env.get("DATABASE_URL") or "postgresql://postgres:postgres@localhost:5432/github_crawl",
            statement_timeout=float(overrides.get("database_statement_timeout") or env.get("DATABASE_STATEMENT_TIMEOUT", 60.0)),
            batch_size=int(overrides.get("database_batch_size") or env.get("DATABASE_BATCH_SIZE", 500)),
        )

        crawl = CrawlSettings(
            target_repository_count=int(
                overrides.get("target_repository_count")
                or env.get("TARGET_REPOSITORY_COUNT")
                or 100_000
            ),
            search_result_limit=int(
                overrides.get("search_result_limit")
                or env.get("SEARCH_RESULT_LIMIT")
                or 1_000
            ),
            range_start=overrides.get("range_start")
            or _parse_datetime(env.get("CREATED_RANGE_START"))
            or datetime(2008, 1, 1, tzinfo=UTC),
        )

        return cls(github=github, database=database, crawl=crawl)


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).astimezone(UTC)
    except ValueError as exc:  # pragma: no cover - defensive clause
        raise ValueError(f"Invalid datetime format: {value}") from exc


@dataclass(slots=True)
class RateLimitInfo:
    """Snapshot of GitHub's rate limit state."""

    cost: int
    remaining: int
    reset_at: datetime


__all__ = [
    "AppConfig",
    "GitHubSettings",
    "DatabaseSettings",
    "CrawlSettings",
    "RateLimitInfo",
    "UTC",
]
