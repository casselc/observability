-- The aggregator's side of the entity lanes: raw records, the merged
-- versions, and views in the shape of ../../sql/catalog.sql. {db} is the
-- catalog database. Statements are separated by a line ending in ';'.
--
-- Merge rule (so lanes can be replayed, arrive out of order, or come from two
-- controllers of the same cluster): per (cluster_key, level, key),
--   valid_from = min over records,
--   closed_at  = closed_at of the LATEST observation (ties: the close wins),
--                0 while open, so a wrongly closed version reopens when the
--                controller sees it again;
--   everything else is a function of the key (content-addressed), so any().

CREATE TABLE IF NOT EXISTS {db}.records
(
    level LowCardinality(String),
    key UInt64,
    entity UInt64,
    cluster_key UInt64,
    node_key UInt64 DEFAULT 0,
    ns_key UInt64 DEFAULT 0,
    wl_key UInt64 DEFAULT 0,
    pod_key UInt64 DEFAULT 0,
    kind LowCardinality(String) DEFAULT '',
    name String DEFAULT '',
    pod_uid String DEFAULT '',
    container LowCardinality(String) DEFAULT '',
    attrs Map(LowCardinality(String), String),
    valid_from DateTime64(3, 'UTC'),
    closed_at DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC'),
    event_at DateTime64(3, 'UTC'),
    writer LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (cluster_key, level, key, observed_at)
TTL toDateTime(ingested_at) + INTERVAL 7 DAY
SETTINGS non_replicated_deduplication_window = 100000;

CREATE TABLE IF NOT EXISTS {db}.versions
(
    cluster_key UInt64,
    level LowCardinality(String),
    key UInt64,
    entity SimpleAggregateFunction(any, UInt64),
    node_key SimpleAggregateFunction(any, UInt64),
    ns_key SimpleAggregateFunction(any, UInt64),
    wl_key SimpleAggregateFunction(any, UInt64),
    pod_key SimpleAggregateFunction(any, UInt64),
    kind SimpleAggregateFunction(any, String),
    name SimpleAggregateFunction(any, String),
    pod_uid SimpleAggregateFunction(any, String),
    container SimpleAggregateFunction(any, String),
    attrs SimpleAggregateFunction(any, Map(String, String)),
    valid_from SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    close_state AggregateFunction(argMax, DateTime64(3, 'UTC'), Tuple(DateTime64(3, 'UTC'), DateTime64(3, 'UTC'))),
    last_observed SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    first_event SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    first_ingested SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_ingested SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (cluster_key, level, key);

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.versions_mv TO {db}.versions AS
SELECT cluster_key, level, key,
       any(entity) AS entity, any(node_key) AS node_key, any(ns_key) AS ns_key, any(wl_key) AS wl_key, any(pod_key) AS pod_key,
       any(toString(kind)) AS kind, any(name) AS name, any(pod_uid) AS pod_uid, any(toString(container)) AS container, any(CAST(attrs, 'Map(String, String)')) AS attrs,
       min(valid_from) AS valid_from,
       argMaxState(closed_at, (observed_at, closed_at)) AS close_state,
       max(observed_at) AS last_observed,
       min(event_at) AS first_event,
       min(ingested_at) AS first_ingested,
       max(ingested_at) AS last_ingested
FROM {db}.records
GROUP BY cluster_key, level, key;

CREATE VIEW IF NOT EXISTS {db}.versions_final AS
SELECT cluster_key, level, key,
       any(entity) AS entity, any(node_key) AS node_key, any(ns_key) AS ns_key, any(wl_key) AS wl_key, any(pod_key) AS pod_key,
       any(kind) AS kind, any(name) AS name, any(pod_uid) AS pod_uid, any(container) AS container, any(attrs) AS attrs,
       min(valid_from) AS valid_from,
       argMaxMerge(close_state) AS closed_at,
       if(closed_at = toDateTime64(0, 3, 'UTC'), toDateTime64('2100-01-01 00:00:00', 3, 'UTC'), closed_at) AS valid_to,
       max(last_observed) AS last_observed,
       min(first_event) AS first_event,
       min(first_ingested) AS first_ingested,
       max(last_ingested) AS last_ingested
FROM {db}.versions
GROUP BY cluster_key, level, key;

-- catalog.sql's shapes (valid_to = 2100-01-01 while open)
CREATE VIEW IF NOT EXISTS {db}.clusters AS
SELECT entity AS cluster_key, attrs, valid_from, valid_to FROM {db}.versions_final WHERE level = 'cluster';
CREATE VIEW IF NOT EXISTS {db}.nodes AS
SELECT entity AS node_key, cluster_key, attrs, valid_from, valid_to FROM {db}.versions_final WHERE level = 'node';
CREATE VIEW IF NOT EXISTS {db}.namespaces AS
SELECT key AS ns_key, cluster_key, attrs, valid_from, valid_to FROM {db}.versions_final WHERE level = 'namespace';
CREATE VIEW IF NOT EXISTS {db}.workloads AS
SELECT key AS wl_key, cluster_key, ns_key, kind, name, attrs, valid_from, valid_to FROM {db}.versions_final WHERE level = 'workload';
CREATE VIEW IF NOT EXISTS {db}.pods AS
SELECT key AS pod_key, pod_uid, cluster_key, wl_key, node_key, ns_key, attrs, valid_from, valid_to FROM {db}.versions_final WHERE level = 'pod';
CREATE VIEW IF NOT EXISTS {db}.resources AS
SELECT key AS resource_id, attrs, cluster_key, pod_uid, container, valid_from, valid_to, 'controller' AS source,
       first_event, first_ingested, last_observed
FROM {db}.versions_final WHERE level = 'resource';
CREATE VIEW IF NOT EXISTS {db}.resources_current AS
SELECT * FROM {db}.resources WHERE valid_to > now64(3);

-- the aggregator's progress per lane (an optimization: records are idempotent)
CREATE TABLE IF NOT EXISTS {db}.lane_progress
(
    lane String,
    last_object String,
    objects UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY lane;

-- one row per ingested object, for lag and volume
CREATE TABLE IF NOT EXISTS {db}.ingest_log
(
    object String,
    cluster LowCardinality(String),
    kind LowCardinality(String),
    bytes UInt64,
    records UInt64,
    put_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3),
    closed_by_sync UInt64 DEFAULT 0
)
ENGINE = MergeTree
ORDER BY (cluster, ingested_at);
