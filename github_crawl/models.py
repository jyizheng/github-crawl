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
    name: str
    full_name: str
    stargazer_count: int
    fork_count: int
    fetched_at: datetime

    @classmethod
    def from_graphql(cls, payload: dict[str, Any], fetched_at: datetime) -> "RepositoryRecord":
        """Convert a GraphQL node into a :class:`RepositoryRecord`."""

        owner = payload.get("owner") or {}

        return cls(
            node_id=payload["id"],
            database_id=payload.get("databaseId"),
            owner_login=owner.get("login", ""),
            name=payload.get("name", ""),
            full_name=payload.get("nameWithOwner", ""),
            stargazer_count=payload.get("stargazerCount", 0),
            fork_count=payload.get("forkCount", 0),
            fetched_at=fetched_at.astimezone(UTC),
        )


__all__ = ["RepositoryRecord"]
