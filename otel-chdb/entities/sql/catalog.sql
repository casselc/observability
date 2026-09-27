-- The entity catalog: what a multicluster controller with shared informers
-- knows about every cluster / node / namespace / workload / pod, versioned
-- (SCD2: one row per version, [valid_from, valid_to), valid_to = far future
-- while the version is current). {db} is the catalog database.
--
-- Every level stores only the resource attributes it contributes, under the
-- key names the edge's k8sattributes / resourcedetection processors emit, so
-- a container's covered attribute set is the mapConcat of its chain
-- (cluster, node, namespace, workload version, pod version, container).
-- ../README.md §Mechanism defines the covered set and resource_id.
-- Statements are separated by a line ending in ';'.

CREATE TABLE IF NOT EXISTS {db}.clusters
(
    cluster_key UInt64,              -- xxh3 of the cluster uid
    attrs Map(LowCardinality(String), String),
    valid_from DateTime64(3),
    valid_to DateTime64(3)
)
ENGINE = MergeTree ORDER BY cluster_key;

CREATE TABLE IF NOT EXISTS {db}.nodes
(
    node_key UInt64,                 -- xxh3 of the node uid
    cluster_key UInt64,
    attrs Map(LowCardinality(String), String),
    valid_from DateTime64(3),
    valid_to DateTime64(3)
)
ENGINE = MergeTree ORDER BY node_key;

CREATE TABLE IF NOT EXISTS {db}.namespaces
(
    ns_key UInt64,
    cluster_key UInt64,
    attrs Map(LowCardinality(String), String),
    valid_from DateTime64(3),
    valid_to DateTime64(3)
)
ENGINE = MergeTree ORDER BY ns_key;

-- One row per workload VERSION: a rollout (new pod template) is a new
-- version. containers: the template's containers that emit telemetry.
-- residual: what the workload's SDKs add that no informer can see
-- (telemetry.sdk.*, process.runtime.name, custom resource attributes);
-- the generator uses it to build rows, the catalog never serves it.
CREATE TABLE IF NOT EXISTS {db}.workloads
(
    wl_key UInt64,
    cluster_key UInt64,
    ns_key UInt64,
    kind LowCardinality(String),
    name String,
    attrs Map(LowCardinality(String), String),
    containers Array(Tuple(name String, image String, tag String)),
    residual Map(LowCardinality(String), String),
    weight Float32,                  -- relative traffic (generator only)
    valid_from DateTime64(3),
    valid_to DateTime64(3)
)
ENGINE = MergeTree ORDER BY wl_key;

-- One row per pod VERSION: a pod whose labels change mid-life gets a second
-- version with the same pod_uid.
CREATE TABLE IF NOT EXISTS {db}.pods
(
    pod_key UInt64,
    pod_uid String,
    cluster_key UInt64,
    wl_key UInt64,
    node_key UInt64,
    ns_key UInt64,
    attrs Map(LowCardinality(String), String),
    valid_from DateTime64(3),
    valid_to DateTime64(3),
    pod_start DateTime64(3),
    pod_end DateTime64(3)
)
ENGINE = MergeTree ORDER BY (cluster_key, wl_key, valid_from);

-- The resource table: one row per resource VERSION, keyed by resource_id
-- (the content hash of its covered attribute set). This is the dictionary's
-- source. Written by the controller (from the tables above) and, for the
-- edge-announcement lane, by the consumer (source = 'edge'); either writer
-- produces the same row for the same id, so ReplacingMergeTree just keeps
-- one.
CREATE TABLE IF NOT EXISTS {db}.resources
(
    resource_id UInt64,
    attrs Map(LowCardinality(String), String),
    cluster_key UInt64,
    pod_uid String,
    container LowCardinality(String),
    valid_from DateTime64(3),
    valid_to DateTime64(3),
    source LowCardinality(String)
)
ENGINE = ReplacingMergeTree ORDER BY resource_id;

-- The current state (what is alive now), for the controller's own use and
-- for "what exists" queries.
CREATE VIEW IF NOT EXISTS {db}.resources_current AS
SELECT * FROM {db}.resources FINAL WHERE valid_to > now64(3);
