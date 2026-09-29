-- Shape C, the proposal (research/langfuse.md §8): LLM spans stay otel_traces rows (the consumer's table,
-- unchanged), their large gen_ai content attributes replaced at the edge by content-hash references;
-- the content goes to llm_payloads (content-addressed, inserted before the rows that reference it, as
-- resource announcements are, D21); typed LLM columns come from a materialized view on otel_traces,
-- in the consumer's own insert statement (exactly-once as the key/value rollup is, D33). Scores are
-- gen_ai.evaluation.result log records in otel_logs, typed by a view into llm_scores, and resolved at
-- query time (append-only facts, latest by custody time; D32's pattern).
-- {db} is the database; otel_traces / otel_logs already exist in it (otap-rs/sql).

-- Content-addressed payloads. Duplicates are harmless (identical bytes); ReplacingMergeTree only
-- reclaims the space when parts merge; no read ever needs FINAL. The partition is the custody day of
-- the rows that referenced it: a payload lives exactly as long as the rows of its day (the edge's dedup
-- cache is per day, so a payload repeated across days is stored once per day).
CREATE TABLE {db}.llm_payloads
(
    `hash` FixedString(16),
    `received_day` Date,
    `cluster` LowCardinality(String),
    `namespace` LowCardinality(String),
    `bytes` UInt32,
    `content` String CODEC(ZSTD(3)),
    INDEX idx_fts_content lower(content) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = ReplacingMergeTree
PARTITION BY received_day
ORDER BY (cluster, namespace, hash)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 1024;

-- Typed LLM columns, Langfuse's events_core minus what our design makes unnecessary (event_ts,
-- is_deleted, updated_at: rows are never replaced; bookmarked/public: UI state, kept in the UI's own
-- store; cost columns: computed at query time from the price facts) plus the scope and custody columns.
CREATE TABLE {db}.llm_spans
(
    `cluster` LowCardinality(String),
    `namespace` LowCardinality(String),
    `resource_id` UInt64,
    `trace_id` String,
    `span_id` String,
    `parent_span_id` String,
    `start_time` DateTime64(6, 'UTC'),
    `end_time` DateTime64(6, 'UTC'),
    `received_at` DateTime64(9, 'UTC'),
    `late_part` UInt8,
    `type` LowCardinality(String),
    `name` LowCardinality(String),
    `service_name` LowCardinality(String),
    `environment` LowCardinality(String),
    `trace_name` String,
    `user_id` String,
    `session_id` String,
    `tags` Array(LowCardinality(String)),
    `status_code` LowCardinality(String),
    `provider` LowCardinality(String),
    `model` LowCardinality(String),
    `input_tokens` UInt32,
    `output_tokens` UInt32,
    `usage_details` Map(LowCardinality(String), UInt64),
    `provided_cost` Nullable(Decimal(18, 12)),
    `prompt_name` String,
    `prompt_version` UInt16,
    `tool_name` LowCardinality(String),
    `system_ref` Array(FixedString(16)),
    `input_refs` Array(FixedString(16)),
    `output_refs` Array(FixedString(16)),
    `input_bytes` UInt32,
    `output_bytes` UInt32,
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_session_id session_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY (toDate(received_at), late_part)
ORDER BY (cluster, namespace, toStartOfMinute(start_time), xxHash32(trace_id), span_id)
SETTINGS non_replicated_deduplication_window = 1000;

-- The references travel as a JSON array of "h:<32 hex>" strings in the attribute the content was in.
CREATE MATERIALIZED VIEW {db}.llm_spans_mv TO {db}.llm_spans AS
SELECT
    ResourceAttributes['k8s.cluster.name'] AS cluster,
    ResourceAttributes['k8s.namespace.name'] AS namespace,
    resource_id,
    TraceId AS trace_id, SpanId AS span_id, ParentSpanId AS parent_span_id,
    Timestamp AS start_time, Timestamp + toIntervalNanosecond(Duration) AS end_time,
    received_at, late_part,
    multiIf(SpanAttributes['langfuse.observation.type'] != '', upper(SpanAttributes['langfuse.observation.type']),
            SpanAttributes['gen_ai.operation.name'] IN ('chat', 'text_completion', 'generate_content'), 'GENERATION',
            SpanAttributes['gen_ai.operation.name'] = 'invoke_agent', 'AGENT',
            SpanAttributes['gen_ai.operation.name'] = 'execute_tool', 'TOOL',
            SpanAttributes['gen_ai.operation.name'] = 'embeddings', 'EMBEDDING', 'SPAN') AS type,
    SpanName AS name, ServiceName AS service_name,
    ResourceAttributes['deployment.environment.name'] AS environment,
    SpanAttributes['langfuse.trace.name'] AS trace_name,
    SpanAttributes['user.id'] AS user_id,
    if(SpanAttributes['gen_ai.conversation.id'] != '', SpanAttributes['gen_ai.conversation.id'], SpanAttributes['session.id']) AS session_id,
    JSONExtract(SpanAttributes['langfuse.trace.tags'], 'Array(String)') AS tags,
    StatusCode AS status_code,
    SpanAttributes['gen_ai.provider.name'] AS provider,
    if(SpanAttributes['gen_ai.response.model'] != '', SpanAttributes['gen_ai.response.model'], SpanAttributes['gen_ai.request.model']) AS model,
    toUInt32OrZero(SpanAttributes['gen_ai.usage.input_tokens']) AS input_tokens,
    toUInt32OrZero(SpanAttributes['gen_ai.usage.output_tokens']) AS output_tokens,
    CAST(mapFilter((k, v) -> v != 0, map('input', toUInt64(input_tokens), 'output', toUInt64(output_tokens),
         'cache_read', toUInt64OrZero(SpanAttributes['gen_ai.usage.cache_read.input_tokens']),
         'reasoning', toUInt64OrZero(SpanAttributes['gen_ai.usage.reasoning.output_tokens']))), 'Map(LowCardinality(String), UInt64)') AS usage_details,
    toDecimal64OrNull(SpanAttributes['gen_ai.usage.cost'], 12) AS provided_cost,
    SpanAttributes['langfuse.observation.prompt.name'] AS prompt_name,
    toUInt16OrZero(SpanAttributes['langfuse.observation.prompt.version']) AS prompt_version,
    SpanAttributes['gen_ai.tool.name'] AS tool_name,
    arrayMap(x -> toFixedString(unhex(substring(x, 3)), 16), JSONExtract(SpanAttributes['gen_ai.system_instructions'], 'Array(String)')) AS system_ref,
    arrayMap(x -> toFixedString(unhex(substring(x, 3)), 16), JSONExtract(SpanAttributes['gen_ai.input.messages'], 'Array(String)')) AS input_refs,
    arrayMap(x -> toFixedString(unhex(substring(x, 3)), 16), JSONExtract(SpanAttributes['gen_ai.output.messages'], 'Array(String)')) AS output_refs,
    toUInt32OrZero(SpanAttributes['otel.payload.input_bytes']) AS input_bytes,
    toUInt32OrZero(SpanAttributes['otel.payload.output_bytes']) AS output_bytes
FROM {db}.otel_traces
WHERE SpanAttributes['gen_ai.operation.name'] != '' OR SpanAttributes['langfuse.observation.type'] != '';

-- Scores as append-only facts. A correction is a later fact with the same score_id; a deletion is a
-- retract fact. Resolution (research/langfuse.md §6.2): per score_id, the fact with the greatest
-- (received_at, producer_id, batch_id, row_ordinal) among those received before the basis; a retract hides it.
CREATE TABLE {db}.llm_scores
(
    `cluster` LowCardinality(String),
    `namespace` LowCardinality(String),
    `score_id` String,
    `trace_id` String,
    `observation_id` String,
    `session_id` String,
    `name` LowCardinality(String),
    `source` LowCardinality(String),
    `data_type` LowCardinality(String),
    `value` Float64,
    `string_value` String,
    `kind` LowCardinality(String),
    `event_time` DateTime64(6, 'UTC'),
    `received_at` DateTime64(9, 'UTC'),
    `late_part` UInt8,
    `producer_id` LowCardinality(String),
    `batch_id` UInt64,
    `row_ordinal` UInt32,
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY (toDate(received_at), late_part)
ORDER BY (cluster, namespace, name, toStartOfHour(event_time), score_id)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW {db}.llm_scores_mv TO {db}.llm_scores AS
SELECT ResourceAttributes['k8s.cluster.name'] AS cluster, ResourceAttributes['k8s.namespace.name'] AS namespace,
       LogAttributes['langfuse.score.id'] AS score_id, TraceId AS trace_id, SpanId AS observation_id,
       LogAttributes['session.id'] AS session_id,
       LogAttributes['gen_ai.evaluation.name'] AS name, LogAttributes['langfuse.score.source'] AS source,
       LogAttributes['langfuse.score.data_type'] AS data_type,
       toFloat64OrZero(LogAttributes['gen_ai.evaluation.score.value']) AS value,
       LogAttributes['gen_ai.evaluation.score.label'] AS string_value,
       if(LogAttributes['langfuse.score.kind'] = 'retract', 'retract', 'assert') AS kind,
       Timestamp AS event_time, received_at, late_part, producer_id, batch_id, row_ordinal
FROM {db}.otel_logs WHERE EventName = 'gen_ai.evaluation.result';

-- Price facts: valid time (valid_from) and system time (system_from); cost at query time, as of a basis.
CREATE TABLE {db}.llm_prices (model String, usage_type LowCardinality(String), price Decimal(18, 12),
                              valid_from DateTime64(6, 'UTC'), system_from DateTime64(9, 'UTC'))
ENGINE = MergeTree ORDER BY (model, usage_type, valid_from);
