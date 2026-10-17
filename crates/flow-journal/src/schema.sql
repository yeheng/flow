PRAGMA user_version=1;
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
    digest TEXT NOT NULL
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
CREATE TABLE current_entities (
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    body TEXT NOT NULL CHECK(json_valid(body)),
    digest TEXT NOT NULL,
    created_order TEXT NOT NULL,
    last_lsn TEXT NOT NULL REFERENCES transactions(lsn),
    PRIMARY KEY(kind,key)
) WITHOUT ROWID;
CREATE INDEX entity_created_order ON current_entities(kind,created_order DESC,key DESC);
CREATE INDEX entity_workflow_order ON current_entities(kind,json_extract(body,'$.workflow_id'),created_order DESC,key DESC);
CREATE INDEX entity_status_order ON current_entities(kind,json_extract(body,'$.status'),created_order DESC,key DESC);
CREATE INDEX entity_source_order ON current_entities(kind,json_extract(body,'$.source'),created_order DESC,key DESC);
CREATE INDEX entity_name_order ON current_entities(kind,json_extract(body,'$.name'),key);
CREATE INDEX entity_run_nodes ON current_entities(json_extract(key,'$[0]')) WHERE kind='run_node';
CREATE INDEX entity_attempt_nodes ON current_entities(json_extract(key,'$[0]'),json_extract(key,'$[1]')) WHERE kind='run_attempt';
CREATE INDEX entity_child_parent ON current_entities(json_extract(key,'$[0]')) WHERE kind='child';
CREATE INDEX entity_workflow_versions ON current_entities(json_extract(key,'$[0]')) WHERE kind='workflow_version';
CREATE INDEX entity_active_runs ON current_entities(key) WHERE kind='run' AND json_extract(body,'$.status') IN ('running','awaiting_resume');
