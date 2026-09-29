-- The consumer's llm_payloads (DECISIONS.md D36 phase 1, ../../research/langfuse.md §6.2, §8,
-- ../../FORMAT.md §2.3; from the spike's ../../langfuse/spike/sql/c_llm.sql): the content an edge
-- offloaded by reference, one row per payload an object carries in its `payloads` part (hash hex ->
-- content, on the first row that references it), inserted from the object BEFORE its rows
-- (src/consumer/worker.rs `payloads_first`, R-L9), as resource announcements are.
--
-- Content-addressed and idempotent, not counted: an object's payloads may be inserted more than once
-- (a retried statement, a copy of the object in a later epoch, the same content carried by two
-- lanes); the rows are identical, and ReplacingMergeTree only reclaims the space when parts merge. No
-- read needs FINAL for correctness (R-L11): any copy of a hash is its content (`LIMIT 1 BY hash`).
--
-- The hash is keyed per tenant and day at the edge (BLAKE3-128, key from the edge's cluster, the
-- resource's k8s.namespace.name and the day of received_at), so a hash names one tenant and one day:
-- received_day is the custody day of the object that carried it, which is the day of every row that
-- references it, and a payload lives exactly as long as those rows' partitions. cluster and namespace
-- are the carrying row's ResourceAttributes, the same scope columns the rows are filtered by (D22):
-- a reader who may see a row may see its payloads (and, in phase 2, only with `llm_content`).
--
-- {table} is the fully qualified table (db.llm_payloads). Statements are separated by a line ending
-- in ';'. central.rs splits them.
CREATE TABLE IF NOT EXISTS {table}
(
    `hash` FixedString(16),
    `received_day` Date,
    `cluster` LowCardinality(String),
    `namespace` LowCardinality(String),
    `bytes` UInt32,
    `content` String CODEC(ZSTD(3)),
    `signal` LowCardinality(String),
    `producer_id` LowCardinality(String),
    `producer_epoch` LowCardinality(String),
    `batch_id` UInt64,
    `ingested_at` DateTime64(3) DEFAULT now64(3) CODEC(Delta(8), ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY received_day
ORDER BY (cluster, namespace, hash)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 1024;
