-- Stage REAL rows for the entity-schema benchmark (real-cluster-telemetry.md
-- §4): entities/scripts/objects.py, bench.py and merge.py read staging tables
-- {dst}.traces and {dst}.logs that carry every schema's columns at once
-- (entities/scripts/telemetry.py builds them from synthetic data). This builds
-- them from the mirror central's option-2 tables instead, so the savings claim
-- (~2.8x insert, ~1/2 bytes, measured on synthetic rows) is re-measured on real
-- resource maps.
--
--   {src}  the mirror central's database (the consumer's DDL: option 2, with resource_id)
--   {dst}  the staging database (entities' default: ent_src)
--   {from} {to}  a window of received_at (an hour of real traffic is plenty)
--   {n}    objects per signal (objects.py --n, default 30)
--
-- Columns objects.py reads besides (a)'s: obj (the object number, 0..n-1:
-- here one real edge object, by content_key, in received_at order),
-- row_ordinal, resource_id (the covered set's hash, as the edges wrote it),
-- ResourceResidual (the keys outside the covered set, which is what shape b
-- keeps per row) and resource_id_full (the whole map's hash: shape ann).
-- The grace window of shape b (the whole covered set in the residual for a
-- resource's first rows) is not reproduced: real rows are all "late", which
-- flatters b by at most the grace rows' share (telemetry.py: rows within 10 s
-- of a resource's first sighting).

CREATE DATABASE IF NOT EXISTS {dst};

DROP TABLE IF EXISTS {dst}.traces SYNC;
DROP TABLE IF EXISTS {dst}.logs SYNC;

CREATE TABLE {dst}.traces ENGINE = MergeTree ORDER BY (obj, row_ordinal) AS
WITH ['cloud.account.id','cloud.availability.zone','cloud.platform','cloud.provider','cloud.region','container.image.name',
      'container.image.tag','deployment.environment.name','host.id','host.name','host.type','k8s.cluster.name','k8s.cluster.uid',
      'k8s.container.name','k8s.cronjob.name','k8s.daemonset.name','k8s.deployment.name','k8s.job.name','k8s.namespace.name',
      'k8s.node.name','k8s.node.uid','k8s.pod.name','k8s.pod.start_time','k8s.pod.uid','k8s.replicaset.name',
      'k8s.statefulset.name','service.name'] AS covered,
keys AS (
    SELECT content_key, toUInt32(row_number() OVER (ORDER BY min(received_at), content_key) - 1) AS obj
    FROM {src}.otel_traces WHERE received_at >= '{from}' AND received_at < '{to}'
    GROUP BY content_key ORDER BY min(received_at), content_key LIMIT {n}
)
SELECT t.*,
       CAST(mapFilter((k, v) -> NOT (has(covered, k) OR startsWith(k, 'k8s.pod.label.')), t.ResourceAttributes),
            'Map(LowCardinality(String), String)') AS ResourceResidual,
       xxh3(concat('res.v1\0', arrayStringConcat(arrayMap(x -> concat(x.1, '\0', x.2, '\0'),
            arraySort(arrayFilter(x -> x.2 != '', CAST(t.ResourceAttributes, 'Array(Tuple(String, String))'))))))) AS resource_id_full,
       keys.obj AS obj
FROM {src}.otel_traces AS t INNER JOIN keys USING content_key
WHERE t.received_at >= '{from}' AND t.received_at < '{to}';

CREATE TABLE {dst}.logs ENGINE = MergeTree ORDER BY (obj, row_ordinal) AS
WITH ['cloud.account.id','cloud.availability.zone','cloud.platform','cloud.provider','cloud.region','container.image.name',
      'container.image.tag','deployment.environment.name','host.id','host.name','host.type','k8s.cluster.name','k8s.cluster.uid',
      'k8s.container.name','k8s.cronjob.name','k8s.daemonset.name','k8s.deployment.name','k8s.job.name','k8s.namespace.name',
      'k8s.node.name','k8s.node.uid','k8s.pod.name','k8s.pod.start_time','k8s.pod.uid','k8s.replicaset.name',
      'k8s.statefulset.name','service.name'] AS covered,
keys AS (
    SELECT content_key, toUInt32(row_number() OVER (ORDER BY min(received_at), content_key) - 1) AS obj
    FROM {src}.otel_logs WHERE received_at >= '{from}' AND received_at < '{to}'
    GROUP BY content_key ORDER BY min(received_at), content_key LIMIT {n}
)
SELECT t.*,
       CAST(mapFilter((k, v) -> NOT (has(covered, k) OR startsWith(k, 'k8s.pod.label.')), t.ResourceAttributes),
            'Map(LowCardinality(String), String)') AS ResourceResidual,
       xxh3(concat('res.v1\0', arrayStringConcat(arrayMap(x -> concat(x.1, '\0', x.2, '\0'),
            arraySort(arrayFilter(x -> x.2 != '', CAST(t.ResourceAttributes, 'Array(Tuple(String, String))'))))))) AS resource_id_full,
       keys.obj AS obj
FROM {src}.otel_logs AS t INNER JOIN keys USING content_key
WHERE t.received_at >= '{from}' AND t.received_at < '{to}';

-- What was staged: objects, rows, resources, and how much of the map the residual keeps.
SELECT 'traces' AS signal, max(obj) + 1 AS objects, count() AS rows, uniqExact(resource_id) AS resources,
       round(avg(length(ResourceAttributes)), 1) AS res_keys, round(avg(length(ResourceResidual)), 1) AS residual_keys
FROM {dst}.traces
UNION ALL
SELECT 'logs', max(obj) + 1, count(), uniqExact(resource_id), round(avg(length(ResourceAttributes)), 1), round(avg(length(ResourceResidual)), 1)
FROM {dst}.logs
FORMAT TSVWithNames;
