"""Domain models used by the crawler."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


UTC = timezone.utc


@dataclass(slots=True)
class RepositoryRecord:
    """Normalized representation of a GitHub repository."""

    node_id: str
    database_id: int | None
    owner_login: str
    owner_type: str
    name: str
    full_name: str
    description: str | None
    primary_language: str | None
    stargazer_count: int
    fork_count: int
    open_issue_count: int
    watcher_count: int
    is_private: bool
    is_fork: bool
    is_archived: bool
    created_at: datetime
    updated_at: datetime
    pushed_at: datetime | None
    fetched_at: datetime

    @classmethod
    def from_graphql(cls, payload: dict[str, Any], fetched_at: datetime) -> "RepositoryRecord":
        """Convert a GraphQL node into a :class:`RepositoryRecord`."""

        owner = payload.get("owner") or {}
        watchers = payload.get("watchers") or {}
        issues = payload.get("issues") or {}
        primary_language = payload.get("primaryLanguage") or {}

        return cls(
            node_id=payload["id"],
            database_id=payload.get("databaseId"),
            owner_login=owner.get("login", ""),
            owner_type=owner.get("__typename", "Unknown"),
            name=payload.get("name", ""),
            full_name=payload.get("nameWithOwner", ""),
            description=payload.get("description"),
            primary_language=primary_language.get("name"),
            stargazer_count=payload.get("stargazerCount", 0),
            fork_count=payload.get("forkCount", 0),
            open_issue_count=issues.get("totalCount", 0),
            watcher_count=watchers.get("totalCount", 0),
            is_private=payload.get("isPrivate", False),
            is_fork=payload.get("isFork", False),
            is_archived=payload.get("isArchived", False),
            created_at=_parse_datetime(payload.get("createdAt")),
            updated_at=_parse_datetime(payload.get("updatedAt")),
            pushed_at=_parse_datetime(payload.get("pushedAt")) if payload.get("pushedAt") else None,
            fetched_at=fetched_at.astimezone(UTC),
        )


def _parse_datetime(value: str | None) -> datetime:
    if value is None:
        raise ValueError("datetime value is required")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(UTC)


__all__ = ["RepositoryRecord"]
