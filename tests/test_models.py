from __future__ import annotations

from datetime import datetime, timezone

from github_crawl.models import RepositoryRecord


def test_repository_record_from_graphql_parses_fields():
    payload = {
        "id": "MDEwOlJlcG9zaXRvcnkx",
        "databaseId": 1,
        "name": "demo",
        "nameWithOwner": "acme/demo",
        "description": "example",
        "stargazerCount": 42,
        "forkCount": 3,
        "isPrivate": False,
        "isFork": False,
        "isArchived": False,
        "createdAt": "2024-01-01T00:00:00Z",
        "updatedAt": "2024-01-02T00:00:00Z",
        "pushedAt": "2024-01-03T00:00:00Z",
        "owner": {"login": "acme", "__typename": "Organization"},
        "watchers": {"totalCount": 4},
        "issues": {"totalCount": 5},
        "primaryLanguage": {"name": "Python"},
    }
    fetched_at = datetime(2024, 1, 10, tzinfo=timezone.utc)

    record = RepositoryRecord.from_graphql(payload, fetched_at)

    assert record.node_id == "MDEwOlJlcG9zaXRvcnkx"
    assert record.database_id == 1
    assert record.owner_login == "acme"
    assert record.owner_type == "Organization"
    assert record.stargazer_count == 42
    assert record.fork_count == 3
    assert record.open_issue_count == 5
    assert record.watcher_count == 4
    assert record.primary_language == "Python"
    assert record.created_at.tzinfo == timezone.utc
    assert record.fetched_at == fetched_at
