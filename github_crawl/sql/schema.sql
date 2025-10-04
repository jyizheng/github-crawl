CREATE TABLE IF NOT EXISTS github_repositories (
    node_id TEXT PRIMARY KEY,
    database_id BIGINT,
    owner_login TEXT NOT NULL,
    name TEXT NOT NULL,
    full_name TEXT NOT NULL,
    stargazer_count INTEGER NOT NULL,
    fork_count INTEGER NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS github_repository_snapshots (
    repository_node_id TEXT NOT NULL REFERENCES github_repositories(node_id) ON DELETE CASCADE,
    fetched_at TIMESTAMPTZ NOT NULL,
    stargazer_count INTEGER NOT NULL,
    fork_count INTEGER NOT NULL,
    PRIMARY KEY (repository_node_id, fetched_at)
);

CREATE INDEX IF NOT EXISTS idx_github_repositories_owner ON github_repositories(owner_login);
CREATE INDEX IF NOT EXISTS idx_github_repositories_stars ON github_repositories(stargazer_count DESC);
