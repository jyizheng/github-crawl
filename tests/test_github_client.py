"""Tests for the GitHub GraphQL client."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
import pytest

from github_crawl.config import GitHubSettings
from github_crawl.github_client import GitHubGraphQLClient, GraphQLClientError


def test_execute_retries_on_secondary_rate_limit():
    """Ensure a secondary rate limit response is retried using Retry-After."""

    call_count = 0

    async def runner() -> None:
        nonlocal call_count

        def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - synchronous handler
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(
                    403,
                    json={
                        "message": "You have exceeded a secondary rate limit. Please wait.",
                        "documentation_url": "https://docs.github.com/rest/overview/resources-in-the-rest-api#rate-limiting",
                    },
                    headers={"Retry-After": "0"},
                )
            return httpx.Response(
                200,
                json={
                    "data": {
                        "rateLimit": {
                            "cost": 1,
                            "remaining": 4999,
                            "resetAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        },
                        "viewer": {"login": "octocat"},
                    }
                },
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as async_client:
            settings = GitHubSettings(
                token=None,
                max_retries=3,
                initial_backoff=0.1,
                max_backoff=1.0,
                request_timeout=5.0,
            )
            client = GitHubGraphQLClient(settings, async_client)
            response = await client.execute("query { viewer { login } }")

        assert response.data["viewer"]["login"] == "octocat"

    asyncio.run(runner())
    assert call_count == 2


def test_execute_raises_for_non_retryable_message():
    """A message without rate limit content should raise an error immediately."""

    async def runner() -> None:
        def handler(_: httpx.Request) -> httpx.Response:  # pragma: no cover - synchronous handler
            return httpx.Response(403, json={"message": "Bad credentials"})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as async_client:
            settings = GitHubSettings(
                token=None,
                max_retries=2,
                initial_backoff=0.1,
                max_backoff=1.0,
                request_timeout=5.0,
            )
            client = GitHubGraphQLClient(settings, async_client)
            with pytest.raises(GraphQLClientError) as exc:
                await client.execute("query { viewer { login } }")

        assert "Bad credentials" in str(exc.value)

    asyncio.run(runner())
