from __future__ import annotations

from datetime import datetime, timezone

from github_crawl.models import RepositoryRecord


def test_repository_record_from_graphql_parses_fields():
    payload = {
        "id": "MDEwOlJlcG9zaXRvcnkx",
        "databaseId": 1,
        "name": "demo",
        "nameWithOwner": "acme/demo",
        "stargazerCount": 42,
        "forkCount": 3,
        "owner": {"login": "acme"},
    }
    fetched_at = datetime(2024, 1, 10, tzinfo=timezone.utc)

    record = RepositoryRecord.from_graphql(payload, fetched_at)

    assert record.node_id == "MDEwOlJlcG9zaXRvcnkx"
    assert record.database_id == 1
    assert record.owner_login == "acme"
    assert record.stargazer_count == 42
    assert record.fork_count == 3
    assert record.fetched_at == fetched_at
