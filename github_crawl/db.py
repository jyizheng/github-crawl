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
                name,
                full_name,
                stargazer_count,
                fork_count,
                fetched_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (node_id) DO UPDATE SET
                database_id = EXCLUDED.database_id,
                owner_login = EXCLUDED.owner_login,
                name = EXCLUDED.name,
                full_name = EXCLUDED.full_name,
                stargazer_count = EXCLUDED.stargazer_count,
                fork_count = EXCLUDED.fork_count,
                fetched_at = EXCLUDED.fetched_at
        """

        snapshot_sql = """
            INSERT INTO github_repository_snapshots (
                repository_node_id,
                fetched_at,
                stargazer_count,
                fork_count
            ) VALUES ($1, $2, $3, $4)
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
                            record.name,
                            record.full_name,
                            record.stargazer_count,
                            record.fork_count,
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
                name,
                full_name,
                stargazer_count,
                fork_count,
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
                        name=row["name"],
                        full_name=row["full_name"],
                        stargazer_count=row["stargazer_count"],
                        fork_count=row["fork_count"],
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
