-- The catalog dictionaries, normalized: resource_id -> pod version + container
-- index, then one dictionary per level. {db} is the catalog database.
--
-- Why normalized (results/dictionary.md, 90 days of the 20-cluster fleet):
-- a flat resource_id -> Map dictionary costs 6.3 KB per resource in
-- ClickHouse 26.10 (Map attributes are held as Fields; system.dictionaries
-- bytes_allocated under-reports them 100x), i.e. ~40 GB for 90 days; the
-- same map as a String 1.6 KB. Per level, the per-resource part is the
-- pod's own attributes and two keys.
--
-- Level attributes are serialized maps (toJSONString), parsed back with
-- JSONExtract in the views; the pod's name, uid and start time are typed attributes
-- (String, UUID, DateTime: the bulk of the per-pod bytes). HASHED_ARRAY: one
-- hash table per dictionary instead of one per attribute (d_pod 428 against
-- 771 B per entry; with the typed uid and start 350).

-- resource_id -> (pod version, container index), computed like
-- resources.sql for every pod version the controller has seen.
CREATE TABLE IF NOT EXISTS {db}.res_index
(
    resource_id UInt64,
    pod_key UInt64,
    ct UInt8,
    valid_from DateTime64(3),
    valid_to DateTime64(3)
)
ENGINE = ReplacingMergeTree ORDER BY resource_id;

CREATE DICTIONARY IF NOT EXISTS {db}.d_res (resource_id UInt64, pod_key UInt64, ct UInt8)
PRIMARY KEY resource_id SOURCE(CLICKHOUSE(QUERY 'SELECT resource_id, pod_key, ct FROM {db}.res_index FINAL'))
LAYOUT(HASHED_ARRAY()) LIFETIME(MIN 30 MAX 60);

CREATE DICTIONARY IF NOT EXISTS {db}.d_pod
(
    pod_key UInt64, name String, uid UUID, start DateTime, extra String DEFAULT '{}',
    wl_key UInt64, node_key UInt64, ns_key UInt64, cluster_key UInt64
)
PRIMARY KEY pod_key
SOURCE(CLICKHOUSE(QUERY 'SELECT pod_key, attrs[''k8s.pod.name''] AS name, toUUID(attrs[''k8s.pod.uid'']) AS uid,
    parseDateTimeBestEffort(attrs[''k8s.pod.start_time'']) AS start,
    if(length(attrs) > 3, toJSONString(mapFilter((k, v) -> k NOT IN (''k8s.pod.name'', ''k8s.pod.uid'', ''k8s.pod.start_time''), attrs)), ''{}'') AS extra,
    wl_key, node_key, ns_key, cluster_key FROM {db}.pods'))
LAYOUT(HASHED_ARRAY()) LIFETIME(MIN 30 MAX 60);

CREATE DICTIONARY IF NOT EXISTS {db}.d_wl (wl_key UInt64, attrs String DEFAULT '{}', containers String DEFAULT '[]')
PRIMARY KEY wl_key
SOURCE(CLICKHOUSE(QUERY 'SELECT wl_key, toJSONString(attrs) AS attrs,
    toJSONString(arrayMap(x -> map(''k8s.container.name'', x.1, ''container.image.name'', x.2, ''container.image.tag'', x.3), containers)) AS containers
    FROM {db}.workloads'))
LAYOUT(HASHED()) LIFETIME(MIN 30 MAX 60);

CREATE DICTIONARY IF NOT EXISTS {db}.d_node (node_key UInt64, attrs String DEFAULT '{}')
PRIMARY KEY node_key SOURCE(CLICKHOUSE(QUERY 'SELECT node_key, toJSONString(attrs) AS attrs FROM {db}.nodes'))
LAYOUT(HASHED()) LIFETIME(MIN 30 MAX 60);

CREATE DICTIONARY IF NOT EXISTS {db}.d_ns (ns_key UInt64, attrs String DEFAULT '{}')
PRIMARY KEY ns_key SOURCE(CLICKHOUSE(QUERY 'SELECT ns_key, toJSONString(attrs) AS attrs FROM {db}.namespaces'))
LAYOUT(HASHED()) LIFETIME(MIN 30 MAX 60);

CREATE DICTIONARY IF NOT EXISTS {db}.d_cluster (cluster_key UInt64, attrs String DEFAULT '{}')
PRIMARY KEY cluster_key SOURCE(CLICKHOUSE(QUERY 'SELECT cluster_key, toJSONString(attrs) AS attrs FROM {db}.clusters'))
LAYOUT(HASHED()) LIFETIME(MIN 30 MAX 60);
