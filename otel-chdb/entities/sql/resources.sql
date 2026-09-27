-- The resource table from the catalog: one row per (pod version, telemetry
-- container), its covered attribute set flattened from the chain
-- cluster -> node -> namespace -> workload version -> pod version -> container,
-- and resource_id, the content hash of that set (../README.md §resource_id):
--
--   resource_id = xxh3_64( "res.v1\0" || for (k, v) in sorted(covered, by k), v != "": k || "\0" || v || "\0" )
--
-- {db} is the catalog database, {where} a filter on the pods (p.*).
INSERT INTO {db}.resources
SELECT
    xxh3(concat('res.v1\0', arrayStringConcat(arrayMap(x -> concat(x.1, '\0', x.2, '\0'),
        arraySort(arrayFilter(x -> x.2 != '', CAST(attrs, 'Array(Tuple(String, String))'))))))) AS resource_id,
    attrs, cluster_key, pod_uid, container, valid_from, valid_to, 'controller' AS source
FROM
(
    SELECT
        mapConcat(c.attrs, n.attrs, s.attrs, w.attrs, p.attrs,
                  map('k8s.container.name', ct.1, 'container.image.name', ct.2, 'container.image.tag', ct.3)) AS attrs,
        p.cluster_key AS cluster_key, p.pod_uid AS pod_uid, ct.1 AS container, p.valid_from AS valid_from, p.valid_to AS valid_to
    FROM {db}.pods AS p
    INNER JOIN {db}.workloads AS w ON w.wl_key = p.wl_key
    INNER JOIN {db}.nodes AS n ON n.node_key = p.node_key
    INNER JOIN {db}.namespaces AS s ON s.ns_key = p.ns_key
    INNER JOIN {db}.clusters AS c ON c.cluster_key = p.cluster_key
    ARRAY JOIN w.containers AS ct
    WHERE {where}
);

-- The normalized dictionaries' index (dictionaries.sql): the same resource_id,
-- with the pod version and the container's index in the workload template.
INSERT INTO {db}.res_index
SELECT
    xxh3(concat('res.v1\0', arrayStringConcat(arrayMap(x -> concat(x.1, '\0', x.2, '\0'),
        arraySort(arrayFilter(x -> x.2 != '', CAST(attrs, 'Array(Tuple(String, String))'))))))) AS resource_id,
    pod_key, ct, valid_from, valid_to
FROM
(
    SELECT
        mapConcat(c.attrs, n.attrs, s.attrs, w.attrs, p.attrs,
                  map('k8s.container.name', ctr.1, 'container.image.name', ctr.2, 'container.image.tag', ctr.3)) AS attrs,
        p.pod_key AS pod_key, toUInt8(ct_i) AS ct, p.valid_from AS valid_from, p.valid_to AS valid_to
    FROM {db}.pods AS p
    INNER JOIN {db}.workloads AS w ON w.wl_key = p.wl_key
    INNER JOIN {db}.nodes AS n ON n.node_key = p.node_key
    INNER JOIN {db}.namespaces AS s ON s.ns_key = p.ns_key
    INNER JOIN {db}.clusters AS c ON c.cluster_key = p.cluster_key
    ARRAY JOIN w.containers AS ctr, arrayEnumerate(w.containers) AS ct_i
    WHERE {where}
);
