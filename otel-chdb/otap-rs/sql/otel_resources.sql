-- The consumer's resource announcements (../../FORMAT.md §2, ../../entities/README.md §6.3):
-- one row per resource a trace or log object announces (its
-- `resource_announce`, the covered set, on the first row of that resource),
-- inserted from the object BEFORE its rows (src/consumer/worker.rs), so a
-- row is never in central before the announcement of its resource.
--
-- Idempotent, not counted: an object's announcements may be inserted more
-- than once (a retried statement, a copy of the object in a later epoch);
-- ReplacingMergeTree keys them by (resource_id, the announcing object's
-- producer, epoch and slot), so each announcement is stored once after
-- merges and read once with FINAL. seen_at is the object's received_at, so
-- a resource's first_seen / last_seen span its announcements (one per window
-- per lane epoch at the edge).
--
-- {table} is the fully qualified table (db.otel_resources). Statements are
-- separated by a line ending in ';'.
CREATE TABLE IF NOT EXISTS {table}
(
    `resource_id` UInt64,
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `signal` LowCardinality(String),
    `producer_id` LowCardinality(String),
    `producer_epoch` LowCardinality(String),
    `batch_id` UInt64,
    `seen_at` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `ingested_at` DateTime64(3) DEFAULT now64(3) CODEC(Delta(8), ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (resource_id, producer_id, producer_epoch, signal, batch_id)
SETTINGS non_replicated_deduplication_window = 1000;

-- One row per announced resource: the aggregator's merge with the
-- controller's catalog reads this (entities/controller/sql/announced.sql).
CREATE VIEW IF NOT EXISTS {table}_announced AS
SELECT resource_id, any(ResourceAttributes) AS ResourceAttributes, min(seen_at) AS first_seen, max(seen_at) AS last_seen,
       min(ingested_at) AS first_ingested, uniqExact(producer_id) AS producers, count() AS announcements
FROM {table} FINAL
GROUP BY resource_id;
