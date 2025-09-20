"""Command line interface for the GitHub crawler."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

import typer

from .config import AppConfig
from .crawler import GitHubCrawler
from .db import Database
from .github_client import GitHubGraphQLClient

app = typer.Typer(add_completion=False)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


@app.command("init-db")
def init_db(
    dsn: Optional[str] = typer.Option(None, help="Postgres DSN to use"),
    log_level: str = typer.Option("INFO", help="Logging level"),
) -> None:
    """Create database schema."""

    configure_logging(log_level)
    overrides = {"database_dsn": dsn} if dsn else {}
    config = AppConfig.from_env(overrides=overrides)

    async def runner() -> None:
        async with Database(config.database) as database:
            await database.create_schema()

    asyncio.run(runner())


@app.command("crawl-stars")
def crawl_stars(
    count: Optional[int] = typer.Option(None, help="Number of repositories to crawl"),
    dsn: Optional[str] = typer.Option(None, help="Postgres DSN"),
    github_token: Optional[str] = typer.Option(None, envvar="GITHUB_TOKEN", help="GitHub token"),
    log_level: str = typer.Option("INFO", help="Logging level"),
) -> None:
    """Fetch repositories and write them to Postgres."""

    configure_logging(log_level)
    overrides = {}
    if count:
        overrides["target_repository_count"] = count
    if dsn:
        overrides["database_dsn"] = dsn
    if github_token:
        overrides["github_token"] = github_token

    config = AppConfig.from_env(overrides=overrides)
    if not config.github.token:
        raise typer.BadParameter("A GitHub token is required")

    async def runner() -> None:
        async with GitHubGraphQLClient(config.github) as client:
            async with Database(config.database) as database:
                crawler = GitHubCrawler(config, client, database)
                result = await crawler.crawl()
                typer.echo(
                    f"Persisted {result.repositories_written} repositories. Remaining rate limit: "
                    f"{result.rate_limit_remaining}"
                )

    asyncio.run(runner())


@app.command("dump")
def dump(
    output: Path = typer.Option(..., exists=False, dir_okay=False, help="Destination file"),
    dsn: Optional[str] = typer.Option(None, help="Postgres DSN"),
    format: str = typer.Option("csv", help="Export format"),
    log_level: str = typer.Option("INFO", help="Logging level"),
) -> None:
    """Dump repository data into a file."""

    configure_logging(log_level)
    overrides = {"database_dsn": dsn} if dsn else {}
    config = AppConfig.from_env(overrides=overrides)

    if format.lower() != "csv":
        raise typer.BadParameter("Only CSV format is supported currently")

    async def runner() -> None:
        async with Database(config.database) as database:
            await _write_csv(database, output)

    asyncio.run(runner())


async def _write_csv(database: Database, path: Path) -> None:
    import csv

    fieldnames = [
        "node_id",
        "database_id",
        "owner_login",
        "owner_type",
        "name",
        "full_name",
        "description",
        "primary_language",
        "stargazer_count",
        "fork_count",
        "open_issue_count",
        "watcher_count",
        "is_private",
        "is_fork",
        "is_archived",
        "created_at",
        "updated_at",
        "pushed_at",
        "fetched_at",
    ]

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        async for record in database.stream_repositories():
            writer.writerow(
                {
                    "node_id": record.node_id,
                    "database_id": record.database_id,
                    "owner_login": record.owner_login,
                    "owner_type": record.owner_type,
                    "name": record.name,
                    "full_name": record.full_name,
                    "description": record.description or "",
                    "primary_language": record.primary_language or "",
                    "stargazer_count": record.stargazer_count,
                    "fork_count": record.fork_count,
                    "open_issue_count": record.open_issue_count,
                    "watcher_count": record.watcher_count,
                    "is_private": record.is_private,
                    "is_fork": record.is_fork,
                    "is_archived": record.is_archived,
                    "created_at": record.created_at.isoformat(),
                    "updated_at": record.updated_at.isoformat(),
                    "pushed_at": record.pushed_at.isoformat() if record.pushed_at else "",
                    "fetched_at": record.fetched_at.isoformat(),
                }
            )


__all__ = ["app"]
