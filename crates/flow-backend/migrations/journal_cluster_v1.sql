-- Independent local journals; PostgreSQL is authoritative only for membership and reports.
CREATE TABLE IF NOT EXISTS flow_journal_cluster_nodes (
    cluster_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    journal_id TEXT NOT NULL,
    incarnation_id TEXT NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    revision BIGINT NOT NULL DEFAULT 0,
    desired_state TEXT NOT NULL CHECK (desired_state IN ('enabled', 'removed')),
    observed_state TEXT NOT NULL CHECK (observed_state IN ('online', 'degraded', 'stopped')),
    rpc_url TEXT NOT NULL,
    http_url TEXT NOT NULL,
    build TEXT NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    durable_lsn BIGINT NOT NULL DEFAULT 0 CHECK (durable_lsn >= 0),
    applied_lsn BIGINT NOT NULL DEFAULT 0 CHECK (applied_lsn >= 0),
    projected_lsn BIGINT NOT NULL DEFAULT 0 CHECK (projected_lsn >= 0),
    master_epoch BIGINT NOT NULL DEFAULT 0 CHECK (master_epoch >= 0),
    snapshot JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (cluster_id, node_id),
    CHECK (applied_lsn <= durable_lsn AND projected_lsn <= durable_lsn)
);
CREATE TABLE IF NOT EXISTS flow_journal_cluster_journals (
    cluster_id TEXT NOT NULL,
    journal_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (cluster_id, journal_id),
    UNIQUE (cluster_id, node_id),
    FOREIGN KEY (cluster_id, node_id) REFERENCES flow_journal_cluster_nodes(cluster_id, node_id)
);
CREATE TABLE IF NOT EXISTS flow_journal_cluster_commands (
    cluster_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request JSONB NOT NULL,
    result JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cluster_id, request_id)
);
