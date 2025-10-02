"""HTTP client for interacting with GitHub's GraphQL API."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterable

import httpx

from .config import GitHubSettings, RateLimitInfo

LOGGER = logging.getLogger(__name__)


class GraphQLClientError(RuntimeError):
    """Raised when a GraphQL request fails permanently."""


@dataclass(slots=True)
class GraphQLResponse:
    data: dict[str, Any]
    rate_limit: RateLimitInfo | None


class GitHubGraphQLClient:
    """Light-weight GraphQL client with retry and rate-limit support."""

    def __init__(self, settings: GitHubSettings, client: httpx.AsyncClient | None = None) -> None:
        self._settings = settings
        self._endpoint = settings.graphql_url.rstrip("/")
        headers = {
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "github-crawl-bot",
        }
        if settings.token:
            headers["Authorization"] = f"bearer {settings.token}"
        self._client = client or httpx.AsyncClient(
            headers=headers,
            timeout=settings.request_timeout,
        )
        self._owns_client = client is None

    async def __aenter__(self) -> "GitHubGraphQLClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def execute(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLResponse:
        """Execute a GraphQL query with retries and exponential backoff."""

        backoff = self._settings.initial_backoff
        attempt = 0

        while True:
            attempt += 1
            try:
                response = await self._client.post(
                    self._endpoint,
                    json={"query": query, "variables": variables or {}},
                )
            except httpx.RequestError as exc:
                LOGGER.warning("GraphQL request error: %s", exc)
                if attempt >= self._settings.max_retries:
                    raise GraphQLClientError("Maximum retries exceeded") from exc
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._settings.max_backoff)
                continue

            if response.status_code in {502, 503, 504}:
                LOGGER.info("GitHub transient HTTP %s", response.status_code)
                if attempt >= self._settings.max_retries:
                    raise GraphQLClientError(
                        f"GitHub GraphQL service unavailable after {self._settings.max_retries} attempts"
                    )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._settings.max_backoff)
                continue

            payload = response.json()

            message = payload.get("message")
            if message and response.status_code == 403:
                message_text = str(message)
                lowered = message_text.lower()
                if "rate limit" in lowered and attempt < self._settings.max_retries:
                    delay = _retry_after_seconds(response) or backoff
                    LOGGER.warning("GitHub GraphQL rate limited: %s", message_text)
                    await asyncio.sleep(min(delay, self._settings.max_backoff))
                    backoff = min(max(backoff * 2, delay), self._settings.max_backoff)
                    continue
                raise GraphQLClientError(message_text)

            errors = payload.get("errors")
            if errors:
                if _is_retryable(errors) and attempt < self._settings.max_retries:
                    delay = _retry_delay(errors) or backoff
                    LOGGER.info("Retrying GraphQL call after error: %s", errors)
                    await asyncio.sleep(min(delay, self._settings.max_backoff))
                    backoff = min(backoff * 2, self._settings.max_backoff)
                    continue
                raise GraphQLClientError(str(errors))

            data = payload.get("data")
            if data is None:
                raise GraphQLClientError("Response payload missing 'data'")

            rate_limit = None
            if rate := data.get("rateLimit"):
                rate_limit = RateLimitInfo(
                    cost=rate.get("cost", 0),
                    remaining=rate.get("remaining", 0),
                    reset_at=_parse_datetime(rate.get("resetAt")),
                )
            return GraphQLResponse(data=data, rate_limit=rate_limit)


def _is_retryable(errors: Iterable[dict[str, Any]]) -> bool:
    for error in errors:
        error_type = error.get("type") or ""
        message = (error.get("message") or "").lower()
        if error_type in {"RATE_LIMITED", "ABUSE_DETECTED"}:
            return True
        if "timeout" in message or "try again" in message or "temporary" in message:
            return True
    return False


def _retry_delay(errors: Iterable[dict[str, Any]]) -> float | None:
    for error in errors:
        if "retryAfter" in error:
            try:
                return float(error["retryAfter"])
            except (TypeError, ValueError):
                continue
    return None


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        pass

    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive parsing
        return None
    if retry_at is None:  # pragma: no cover - defensive clause
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    delta = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return max(delta, 0.0)


def _parse_datetime(value: str | None) -> datetime:
    if not value:
        raise GraphQLClientError("Rate limit missing resetAt timestamp")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


__all__ = ["GitHubGraphQLClient", "GraphQLClientError", "GraphQLResponse"]
