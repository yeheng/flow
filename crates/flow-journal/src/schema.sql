PRAGMA user_version=4;
CREATE TABLE store_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    journal_id TEXT NOT NULL,
    committed_lsn TEXT NOT NULL,
    domain_mode INTEGER NOT NULL DEFAULT 0 CHECK(domain_mode IN (0,1))
);
CREATE TABLE transactions (
    lsn TEXT PRIMARY KEY,
    tx_id TEXT NOT NULL UNIQUE,
    event_count INTEGER NOT NULL CHECK(event_count>0),
    digest TEXT NOT NULL,
    bytes INTEGER NOT NULL CHECK(bytes>0)
) WITHOUT ROWID;
CREATE TABLE events (
    lsn TEXT NOT NULL REFERENCES transactions(lsn),
    event_index INTEGER NOT NULL,
    run_id TEXT,
    dispatch_id TEXT,
    run_event INTEGER NOT NULL,
    audit_event INTEGER NOT NULL,
    body TEXT NOT NULL CHECK(json_valid(body)),
    PRIMARY KEY(lsn,event_index)
) WITHOUT ROWID;
CREATE INDEX events_run ON events(run_id,lsn,event_index);
CREATE INDEX events_run_control ON events(run_id,lsn,event_index) WHERE run_event=1;
CREATE INDEX events_run_audit ON events(run_id,lsn,event_index) WHERE audit_event=1;
CREATE TABLE value_chunks (
    output_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    lsn TEXT NOT NULL,
    event_index INTEGER NOT NULL,
    bytes BLOB NOT NULL CHECK(length(bytes)>0 AND length(bytes)<=262144),
    digest TEXT NOT NULL,
    PRIMARY KEY(output_id,chunk_index),
    FOREIGN KEY(lsn,event_index) REFERENCES events(lsn,event_index)
) WITHOUT ROWID;
CREATE UNIQUE INDEX value_chunks_fact ON value_chunks(lsn,event_index);
-- Current rows keep the JSON body as the wire form, but every queried key part and
-- body field is an explicit column. Queries never parse keys or bodies to plan.
CREATE TABLE current_entities (
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    -- Typed key parts: run/workflow/scope, node/version/slot, dispatch/child.
    parent TEXT NOT NULL DEFAULT '',
    slot TEXT NOT NULL DEFAULT '',
    extra TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL CHECK(json_valid(body)),
    -- Body-derived values that product queries filter, sort or join on.
    workflow_id TEXT GENERATED ALWAYS AS (json_extract(body,'$.workflow_id')) VIRTUAL,
    status TEXT GENERATED ALWAYS AS (json_extract(body,'$.status')) VIRTUAL,
    source TEXT GENERATED ALWAYS AS (json_extract(body,'$.source')) VIRTUAL,
    name TEXT GENERATED ALWAYS AS (json_extract(body,'$.name')) VIRTUAL,
    dispatch_id TEXT GENERATED ALWAYS AS (json_extract(body,'$.dispatch_id')) VIRTUAL,
    digest TEXT NOT NULL,
    created_order TEXT NOT NULL,
    last_lsn TEXT NOT NULL REFERENCES transactions(lsn),
    PRIMARY KEY(kind,key)
) WITHOUT ROWID;
CREATE INDEX entity_created_order ON current_entities(kind,created_order DESC,key DESC);
CREATE INDEX entity_run_workflow ON current_entities(workflow_id,created_order DESC,key DESC) WHERE kind='run';
CREATE INDEX entity_run_status ON current_entities(status,created_order DESC,key DESC) WHERE kind='run';
CREATE INDEX entity_run_source ON current_entities(source,created_order DESC,key DESC) WHERE kind='run';
CREATE INDEX entity_name_order ON current_entities(name,key) WHERE kind IN ('workflow','template');
CREATE INDEX entity_schedule_workflow ON current_entities(workflow_id,created_order DESC,key DESC) WHERE kind='schedule';
CREATE INDEX entity_webhook_workflow ON current_entities(workflow_id,created_order DESC,key DESC) WHERE kind='webhook';
CREATE INDEX entity_run_nodes ON current_entities(parent,key) WHERE kind='run_node';
CREATE INDEX entity_attempt_nodes ON current_entities(parent,slot,key) WHERE kind='run_attempt';
CREATE INDEX entity_child_parent ON current_entities(parent,slot) WHERE kind='child';
CREATE INDEX entity_workflow_versions ON current_entities(parent,key) WHERE kind='workflow_version';
CREATE INDEX entity_active_runs ON current_entities(key) WHERE kind='run' AND status IN ('running','awaiting_resume');
