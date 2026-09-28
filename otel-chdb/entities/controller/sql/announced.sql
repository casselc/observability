-- The catalog's resources, merged with the edges' announcements
-- (../../../FORMAT.md §2, ../../README.md §6.3). {db} is the catalog
-- database, {ann} the consumer's announcements table (db.otel_resources, with
-- its view {ann}_announced: one row per resource_id). The aggregator applies
-- this after aggregator.sql when run with -announced.
--
-- The merge: the controller is the authority for a resource's attributes and
-- history (valid_from, valid_to, the X1 uncertain flag); an announcement is
-- evidence that a resource with that covered set existed, seen by an edge
-- between first_seen and last_seen. Both are content-addressed by the same
-- resource_id, so an id in both has the same covered attributes (a 64-bit
-- collision aside, as for the controller alone). The view therefore takes
-- the controller's row where there is one and the announcement's otherwise:
--   - attrs: the announced covered set;
--   - cluster_key: rid.Key("cluster", k8s.cluster.uid), the controller's;
--   - pod_uid, container: from the covered set;
--   - valid_from: first_seen; valid_to: last_seen + 2 h. An edge announces a
--     live resource again every window (1 h) per lane epoch, so a resource
--     not announced for two windows is taken as gone; the interval is the
--     edges' resources.window doubled, not something the catalog knows;
--   - uncertain = 1 (X1): its lifetime is inferred, not observed.
-- A resource the controller never saw (born and dead inside a controller
-- outage, entityCatalog.qnt wOutageLife) thus reaches the catalog, and the
-- dictionaries built on it, through the announcement alone.

CREATE OR REPLACE VIEW {db}.resources AS
SELECT key AS resource_id, attrs, cluster_key, pod_uid, container, valid_from, valid_to, 'controller' AS source,
       first_event, first_ingested, last_observed, uncertain
FROM {db}.versions_final WHERE level = 'resource'
UNION ALL
SELECT resource_id,
       CAST(ResourceAttributes, 'Map(String, String)') AS attrs,
       xxh3(concat('cluster', '\0', ResourceAttributes['k8s.cluster.uid'])) AS cluster_key,
       ResourceAttributes['k8s.pod.uid'] AS pod_uid,
       ResourceAttributes['k8s.container.name'] AS container,
       toDateTime64(first_seen, 3, 'UTC') AS valid_from,
       toDateTime64(last_seen + INTERVAL 2 HOUR, 3, 'UTC') AS valid_to,
       'announce' AS source,
       toDateTime64(first_seen, 3, 'UTC') AS first_event,
       toDateTime64(first_ingested, 3, 'UTC') AS first_ingested,
       toDateTime64(last_seen, 3, 'UTC') AS last_observed,
       toUInt8(1) AS uncertain
FROM {ann}_announced
WHERE resource_id NOT IN (SELECT key FROM {db}.versions WHERE level = 'resource');

-- Per resource: whether the controller knows it, and the edges' evidence.
CREATE OR REPLACE VIEW {db}.resource_evidence AS
SELECT a.resource_id AS resource_id, a.first_seen AS announced_first, a.last_seen AS announced_last, a.producers AS producers,
       a.resource_id IN (SELECT key FROM {db}.versions WHERE level = 'resource') AS in_controller
FROM {ann}_announced AS a;
