-- The consumer's llm_scores (DECISIONS.md D36 phase 1, ../../research/langfuse.md §7.1, §8; from the
-- spike's ../../langfuse/spike/sql/c_llm.sql): scores are OTel GenAI `gen_ai.evaluation.result` log
-- records in otel_logs, typed here by a materialized view in the consumer's own insert statement
-- (exactly-once as llm_spans is). They are append-only FACTS (R-L6, R-L11): a correction is a later
-- fact with the same score_id, a deletion a `retract` fact; nothing is replaced or merged. Resolution
-- is at query time (phase 2): per score_id, the fact with the greatest (received_at, producer_id,
-- batch_id, row_ordinal) received before the basis; a retract hides it.
--
-- `mapping` is the semantic-convention mapping version, as in llm_spans (R-L12; llm_mapping lists it).
--
-- {table} is the fully qualified otel_logs; {db} its database. Statements are separated by a line
-- ending in ';'. central.rs splits them.
CREATE TABLE IF NOT EXISTS {db}.llm_scores
(
    `cluster` LowCardinality(String),
    `namespace` LowCardinality(String),
    `score_id` String,
    `supersedes` String,
    `trace_id` String,
    `observation_id` String,
    `session_id` String,
    `name` LowCardinality(String),
    `source` LowCardinality(String),
    `data_type` LowCardinality(String),
    `value` Float64,
    `string_value` String,
    `comment` String,
    `kind` LowCardinality(String),
    `event_time` DateTime64(9, 'UTC'),
    `received_at` DateTime64(9, 'UTC'),
    `late_part` UInt8,
    `mapping` LowCardinality(String),
    `producer_id` LowCardinality(String),
    `batch_id` UInt64,
    `row_ordinal` UInt32,
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY (toDate(received_at), late_part)
ORDER BY (cluster, namespace, name, toStartOfHour(event_time), score_id)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.llm_scores_mv_v1 TO {db}.llm_scores AS
SELECT
    ResourceAttributes['k8s.cluster.name'] AS cluster,
    ResourceAttributes['k8s.namespace.name'] AS namespace,
    LogAttributes['langfuse.score.id'] AS score_id,
    LogAttributes['langfuse.score.supersedes'] AS supersedes,
    TraceId AS trace_id,
    SpanId AS observation_id,
    multiIf(LogAttributes['langfuse.session.id'] != '', LogAttributes['langfuse.session.id'], LogAttributes['session.id']) AS session_id,
    LogAttributes['gen_ai.evaluation.name'] AS name,
    multiIf(LogAttributes['langfuse.score.source'] != '', LogAttributes['langfuse.score.source'], 'API') AS source,
    multiIf(LogAttributes['langfuse.score.data_type'] != '', upper(LogAttributes['langfuse.score.data_type']),
            LogAttributes['gen_ai.evaluation.score.value'] != '', 'NUMERIC', 'CATEGORICAL') AS data_type,
    toFloat64OrZero(LogAttributes['gen_ai.evaluation.score.value']) AS value,
    LogAttributes['gen_ai.evaluation.score.label'] AS string_value,
    LogAttributes['gen_ai.evaluation.explanation'] AS comment,
    if(LogAttributes['langfuse.score.kind'] = 'retract', 'retract', 'assert') AS kind,
    Timestamp AS event_time,
    received_at,
    late_part,
    'v1' AS mapping,
    producer_id,
    batch_id,
    row_ordinal
FROM {table}
WHERE EventName = 'gen_ai.evaluation.result';
