# GitHub Crawl

This repository contains a production-ready crawler that collects star counts for large batches of GitHub repositories using the GraphQL API and persists them in PostgreSQL. The implementation emphasises fast execution (100k repositories in under ten minutes on a fast connection), resilience to rate limits, and an evolvable schema for future metadata enrichment.

## Architecture overview

```
┌─────────────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│ github_crawl.cli    │────▶│ GitHubCrawler   │────▶│ postgres (asyncpg)   │
└─────────────────────┘     │  (crawler.py)   │     └──────────────────────┘
        ▲                   │    ▲    ▲       │              ▲
        │                   │    │    │       │              │
        │                   │    │    │       │              │
        │                   ▼    │    ▼       │              │
        │              RangePlanner │  GitHubGraphQLClient   │
        │             (partitioner) │    (github_client)     │
        │                           │                        │
        └─────────────── configuration (config.py) ──────────┘
```

Key ideas:

* **Parallel GraphQL requests.** Requests are batched with `asyncio` concurrency (default 12) and resilient exponential backoff.
* **Creation-date partitioning.** The crawler splits the creation-time axis until each search window holds ≤ 1,000 repositories (GitHub’s hard limit) before crawling each window in parallel.
* **Streaming persistence.** Repositories flow through an async queue and are upserted into PostgreSQL in batches, creating a snapshot history on every crawl.
* **Schema-first design.** SQL migrations (in `github_crawl/sql/schema.sql`) define the schema so the same setup runs locally and in CI.

## Database schema & future evolution

Two tables are created:

* `github_repositories` – the canonical per-repository record, keyed by GraphQL node id. Upserts keep star counts current while preserving metadata such as owner, language, timestamps, and flags.
* `github_repository_snapshots` – an append-only table (repository id + crawl timestamp + counts) enabling trends without rewriting the base table.

Future metadata (issues, pull requests, reviews, CI runs…) can follow the same pattern: one slowly changing dimension table plus append-only snapshot tables keyed by repository + foreign entity id + timestamp. Each entity gets its own table (e.g. `github_pull_requests`, `github_pull_request_snapshots`) to avoid massive row churn and to isolate update paths. Crawlers for nested entities can reuse the same partitioner, streaming queue, and storage abstractions.

## Installation

1. Install Python 3.11+ and a PostgreSQL instance (local Docker example below).
2. Install the package in editable mode:
   ```bash
   python -m pip install --upgrade pip
   pip install -e .[dev]
   ```
3. Export a GitHub token that has `repo` scope or rely on the default Actions token when running in CI:
   ```bash
   export GITHUB_TOKEN=ghp_yourtoken
   ```
4. (Optional) start PostgreSQL locally:
   ```bash
   docker run --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=github_crawl -p 5432:5432 postgres:15-alpine
   ```

## Commands

The CLI is available as `github-crawl`.

* Initialise the schema:
  ```bash
  github-crawl init-db --dsn postgresql://postgres:postgres@localhost:5432/github_crawl
  ```
* Crawl repositories:
  ```bash
  github-crawl crawl-stars --dsn postgresql://postgres:postgres@localhost:5432/github_crawl --count 100000
  ```
  The crawler defaults to 100,000 repositories, but any positive integer works for smoke testing.
* Dump results:
  ```bash
  github-crawl dump --dsn postgresql://postgres:postgres@localhost:5432/github_crawl --output repositories.csv
  ```

Environment variables (`DATABASE_DSN`, `TARGET_REPOSITORY_COUNT`, `GITHUB_MAX_CONCURRENCY`, etc.) override defaults as described in `github_crawl/config.py`.

## Testing

Run unit tests (partitioner, models, and utility logic) with:

```bash
pytest
```

Tests that require PostgreSQL are marked with `@pytest.mark.db` for future extension; the current suite relies purely on deterministic stubs so that CI passes without a running database.

## GitHub Actions pipeline

`.github/workflows/crawl-stars.yml` provisions a PostgreSQL service container and executes the end-to-end crawl:

1. Check out the code and install dependencies.
2. Run `pytest`.
3. Run `github-crawl init-db` to create tables.
4. Execute `github-crawl crawl-stars` (default 100,000 repositories via the default `GITHUB_TOKEN`).
5. Dump the data to CSV and upload it as an artifact.

`workflow_dispatch` exposes an optional `target_count` input so the run can be throttled for smoke tests while still defaulting to 100,000 repositories.

## Performance notes

* Each GraphQL page requests 100 repositories. With concurrency 12, the crawler issues roughly 1,000 queries in well under ten minutes provided GitHub grants the usual 5,000 rate-limit points.
* Exponential backoff handles secondary rate limits and temporary abuse detection responses automatically.
* Batched database writes (default 500 rows) keep `asyncpg` busy while avoiding large transactions.

## Scaling to 500 million repositories

If the target ballooned from 100k to 500M repositories (plus issues/PRs/comments/etc.), the approach evolves as follows:

1. **Distributed crawling.** Partition the creation-time space across many workers (Kubernetes jobs, AWS Batch, etc.) coordinated via a lightweight scheduler or message queue. Each worker would process disjoint time ranges.
2. **Persistent planning cache.** Store range counts in Redis/PostgreSQL so subsequent runs reuse previous split decisions instead of recomputing counts for hot ranges.
3. **Streaming ingestion.** Swap out direct database writes for a log (Kafka/PubSub) feeding a write-optimised warehouse (BigQuery/ClickHouse) and dedicated upsert workers. This isolates API latency from storage latency.
4. **Sharded storage.** Move from a single PostgreSQL instance to a Citus/Timescale cluster or an analytical warehouse with partitioned tables (per month/owner). Snapshot tables would be partitioned by crawl date to keep retention manageable.
5. **Incremental updates.** Track high-water marks per entity (latest updated/pushed timestamp) and only crawl deltas instead of the entire creation timeline on each run.
6. **Adaptive API usage.** Employ GraphQL persisted queries, request compression, and heuristics to avoid over-fetching (e.g., skip archived/forked repositories unless requested). Secondary rate limits would require token pools and backpressure-aware scheduling.
7. **Observability.** Add tracing, structured metrics (per-range throughput, rate-limit headroom), and anomaly detection to spot failing shards quickly.

These changes keep the core design (range partitioning + streaming upserts) but distribute load horizontally and move storage to systems that comfortably ingest tens of millions of rows per hour.

## Submission checklist

* Python package with typed, testable modules and a Typer CLI.
* PostgreSQL schema migration checked into source control.
* GitHub Actions workflow including a PostgreSQL service, setup, crawl, and artifact export.
* README explaining how to run locally and how to evolve for much larger datasets.
