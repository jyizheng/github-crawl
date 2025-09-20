"""Persistence layer for the crawler."""

from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator, Iterable, Sequence

import asyncpg

from .config import DatabaseSettings
from .models import RepositoryRecord

SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "schema.sql"


class Database:
    """Async helper for writing repository data into Postgres."""

    def __init__(self, settings: DatabaseSettings) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            dsn=self._settings.dsn,
            init=self._init_connection,
            command_timeout=self._settings.statement_timeout,
        )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

    async def create_schema(self) -> None:
        pool = self._ensure_pool()
        statements = _load_sql_statements(SCHEMA_PATH)
        async with pool.acquire() as conn:
            for statement in statements:
                await conn.execute(statement)

    async def upsert_repositories(self, records: Sequence[RepositoryRecord]) -> None:
        if not records:
            return
        pool = self._ensure_pool()
        insert_sql = """
            INSERT INTO github_repositories (
                node_id,
                database_id,
                owner_login,
                owner_type,
                name,
                full_name,
                description,
                primary_language,
                stargazer_count,
                fork_count,
                open_issue_count,
                watcher_count,
                is_private,
                is_fork,
                is_archived,
                created_at,
                updated_at,
                pushed_at,
                fetched_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19
            )
            ON CONFLICT (node_id) DO UPDATE SET
                database_id = EXCLUDED.database_id,
                owner_login = EXCLUDED.owner_login,
                owner_type = EXCLUDED.owner_type,
                name = EXCLUDED.name,
                full_name = EXCLUDED.full_name,
                description = EXCLUDED.description,
                primary_language = EXCLUDED.primary_language,
                stargazer_count = EXCLUDED.stargazer_count,
                fork_count = EXCLUDED.fork_count,
                open_issue_count = EXCLUDED.open_issue_count,
                watcher_count = EXCLUDED.watcher_count,
                is_private = EXCLUDED.is_private,
                is_fork = EXCLUDED.is_fork,
                is_archived = EXCLUDED.is_archived,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                pushed_at = EXCLUDED.pushed_at,
                fetched_at = EXCLUDED.fetched_at
        """

        snapshot_sql = """
            INSERT INTO github_repository_snapshots (
                repository_node_id,
                fetched_at,
                stargazer_count,
                fork_count,
                open_issue_count,
                watcher_count
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (repository_node_id, fetched_at) DO NOTHING
        """

        async with pool.acquire() as conn:
            async with conn.transaction():
                for chunk in _chunks(records, self._settings.batch_size):
                    repo_rows = [
                        (
                            record.node_id,
                            record.database_id,
                            record.owner_login,
                            record.owner_type,
                            record.name,
                            record.full_name,
                            record.description,
                            record.primary_language,
                            record.stargazer_count,
                            record.fork_count,
                            record.open_issue_count,
                            record.watcher_count,
                            record.is_private,
                            record.is_fork,
                            record.is_archived,
                            record.created_at,
                            record.updated_at,
                            record.pushed_at,
                            record.fetched_at,
                        )
                        for record in chunk
                    ]
                    await conn.executemany(insert_sql, repo_rows)

                    snapshot_rows = [
                        (
                            record.node_id,
                            record.fetched_at,
                            record.stargazer_count,
                            record.fork_count,
                            record.open_issue_count,
                            record.watcher_count,
                        )
                        for record in chunk
                    ]
                    await conn.executemany(snapshot_sql, snapshot_rows)

    async def stream_repositories(self) -> AsyncIterator[RepositoryRecord]:
        pool = self._ensure_pool()
        query = """
            SELECT
                node_id,
                database_id,
                owner_login,
                owner_type,
                name,
                full_name,
                description,
                primary_language,
                stargazer_count,
                fork_count,
                open_issue_count,
                watcher_count,
                is_private,
                is_fork,
                is_archived,
                created_at,
                updated_at,
                pushed_at,
                fetched_at
            FROM github_repositories
            ORDER BY stargazer_count DESC, node_id
        """
        async with pool.acquire() as conn:
            async with conn.transaction():
                async for row in conn.cursor(query):
                    yield RepositoryRecord(
                        node_id=row["node_id"],
                        database_id=row["database_id"],
                        owner_login=row["owner_login"],
                        owner_type=row["owner_type"],
                        name=row["name"],
                        full_name=row["full_name"],
                        description=row["description"],
                        primary_language=row["primary_language"],
                        stargazer_count=row["stargazer_count"],
                        fork_count=row["fork_count"],
                        open_issue_count=row["open_issue_count"],
                        watcher_count=row["watcher_count"],
                        is_private=row["is_private"],
                        is_fork=row["is_fork"],
                        is_archived=row["is_archived"],
                        created_at=row["created_at"],
                        updated_at=row["updated_at"],
                        pushed_at=row["pushed_at"],
                        fetched_at=row["fetched_at"],
                    )

    def _ensure_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database pool has not been initialized")
        return self._pool

    async def _init_connection(self, conn: asyncpg.Connection) -> None:
        await conn.execute("SET TIME ZONE 'UTC'")
        await conn.execute(f"SET statement_timeout = {int(self._settings.statement_timeout * 1000)}")


def _chunks(items: Sequence[RepositoryRecord], size: int) -> Iterable[Sequence[RepositoryRecord]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _load_sql_statements(path: Path) -> list[str]:
    script = path.read_text(encoding="utf-8")
    statements: list[str] = []
    for part in script.split(";"):
        statement = part.strip()
        if statement:
            statements.append(statement)
    return statements


__all__ = ["Database"]
