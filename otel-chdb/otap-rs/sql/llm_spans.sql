-- The consumer's llm_spans (DECISIONS.md D36 phase 1, ../../research/langfuse.md §8; from the spike's
-- ../../langfuse/spike/sql/c_llm.sql): a typed copy of the LLM spans of otel_traces, made by a
-- materialized view in the consumer's own insert statement, so it is exactly-once as the key/value
-- rollup is (the target's dedup window dedups the view's block of an exact retry, D33). LLM spans
-- stay otel_traces rows: HyperDX and every existing tool see them as traces.
--
-- Langfuse's events_core column set minus what our design makes unnecessary (event_ts, is_deleted,
-- updated_at: rows are never replaced, R-L11; bookmarked, public: UI state; cost columns: computed at
-- query time from price facts, R-L3; provided_cost is kept, labelled) plus the scope and custody
-- columns. Content is never copied here: input_refs / output_refs / system_ref are the 16-byte
-- references of the edge's reference documents (FORMAT.md §2.3), resolved against llm_payloads.
--
-- THE MAPPING IS POLICY (R-L12): `mapping` names the version of the semantic-convention mapping each
-- row was made with (this file's view, `llm_spans_mv_v1`, is mapping v1: OTel GenAI semconv
-- e57c543 + Langfuse v4.46.0's OtelIngestionProcessor / ObservationTypeMapper precedence, in the
-- order written below), and llm_mapping lists, per version, each column and the attributes it is
-- read from. A mapping change is a new view `llm_spans_mv_v<n+1>` with `'v<n+1>' AS mapping` created
-- with the old one dropped, a new llm_mapping version, and a run of every consumer of the mapped
-- columns (CAST 45/46); rows keep the version they were made with.
--
-- The references are validated (R-L7): only "h:<32 lower hex>" elements of a JSON array are read;
-- anything else in the attribute (an inline value, offloading off) yields no reference.
--
-- {table} is the fully qualified otel_traces; {db} its database. Statements are separated by a line
-- ending in ';'. central.rs splits them.
CREATE TABLE IF NOT EXISTS {db}.llm_spans
(
    `cluster` LowCardinality(String),
    `namespace` LowCardinality(String),
    `resource_id` UInt64,
    `trace_id` String,
    `span_id` String,
    `parent_span_id` String,
    `start_time` DateTime64(9, 'UTC'),
    `end_time` DateTime64(9, 'UTC'),
    `received_at` DateTime64(9, 'UTC'),
    `late_part` UInt8,
    `type` LowCardinality(String),
    `name` String,
    `service_name` LowCardinality(String),
    `environment` LowCardinality(String),
    `trace_name` String,
    `user_id` String,
    `session_id` String,
    `tags` Array(String),
    `status_code` LowCardinality(String),
    `level` LowCardinality(String),
    `provider` LowCardinality(String),
    `model` LowCardinality(String),
    `input_tokens` UInt64,
    `output_tokens` UInt64,
    `usage_details` Map(LowCardinality(String), UInt64),
    `provided_cost` Nullable(Decimal(18, 12)),
    `prompt_name` String,
    `prompt_version` UInt32,
    `tool_name` LowCardinality(String),
    `system_ref` Array(FixedString(16)),
    `input_refs` Array(FixedString(16)),
    `output_refs` Array(FixedString(16)),
    `input_bytes` UInt64,
    `output_bytes` UInt64,
    `truncated` UInt8,
    `mapping` LowCardinality(String),
    `producer_id` LowCardinality(String),
    `batch_id` UInt64,
    `row_ordinal` UInt32,
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_session_id session_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY (toDate(received_at), late_part)
ORDER BY (cluster, namespace, toStartOfMinute(start_time), xxHash32(trace_id), span_id)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.llm_spans_mv_v1 TO {db}.llm_spans AS
WITH
    SpanAttributes AS a,
    ResourceAttributes AS r,
    (x -> arrayMap(h -> toFixedString(unhex(substring(h, 3)), 16),
                   arrayFilter(h -> match(h, '^h:[0-9a-f]{32}$'), JSONExtract(x, 'Array(String)')))) AS refs,
    multiIf(
        a['langfuse.observation.type'] IN ('span', 'generation', 'event', 'embedding', 'agent', 'tool', 'chain', 'retriever', 'guardrail', 'evaluator'),
            upper(a['langfuse.observation.type']),
        a['openinference.span.kind'] IN ('CHAIN', 'RETRIEVER', 'EMBEDDING', 'AGENT', 'TOOL', 'GUARDRAIL', 'EVALUATOR'), a['openinference.span.kind'],
        a['openinference.span.kind'] = 'LLM', 'GENERATION',
        a['gen_ai.operation.name'] IN ('chat', 'completion', 'text_completion', 'generate_content', 'generate'), 'GENERATION',
        a['gen_ai.operation.name'] = 'embeddings', 'EMBEDDING',
        a['gen_ai.operation.name'] IN ('invoke_agent', 'create_agent'), 'AGENT',
        a['gen_ai.operation.name'] = 'execute_tool', 'TOOL',
        a['langfuse.observation.model.name'] != '' OR a['gen_ai.request.model'] != '' OR a['gen_ai.response.model'] != ''
            OR a['llm.model_name'] != '' OR a['model'] != '', 'GENERATION',
        'SPAN') AS obs_type,
    multiIf(a['gen_ai.input.messages'] != '', 'gen_ai.input.messages', a['langfuse.observation.input'] != '', 'langfuse.observation.input',
            a['input.value'] != '', 'input.value', 'gen_ai.prompt') AS input_key,
    multiIf(a['gen_ai.output.messages'] != '', 'gen_ai.output.messages', a['langfuse.observation.output'] != '', 'langfuse.observation.output',
            a['output.value'] != '', 'output.value', 'gen_ai.completion') AS output_key,
    a[input_key] AS input_doc,
    a[output_key] AS output_doc,
    ifNull(coalesce(toUInt64OrNull(a['gen_ai.usage.input_tokens']), toUInt64OrNull(JSONExtractRaw(a['langfuse.observation.usage_details'], 'input')),
             toUInt64OrNull(a['llm.token_count.prompt']), toUInt64OrNull(a['ai.usage.promptTokens'])), toUInt64(0)) AS in_tok,
    ifNull(coalesce(toUInt64OrNull(a['gen_ai.usage.output_tokens']), toUInt64OrNull(JSONExtractRaw(a['langfuse.observation.usage_details'], 'output')),
             toUInt64OrNull(a['llm.token_count.completion']), toUInt64OrNull(a['ai.usage.completionTokens'])), toUInt64(0)) AS out_tok
SELECT
    r['k8s.cluster.name'] AS cluster,
    r['k8s.namespace.name'] AS namespace,
    resource_id,
    TraceId AS trace_id,
    SpanId AS span_id,
    ParentSpanId AS parent_span_id,
    Timestamp AS start_time,
    Timestamp + toIntervalNanosecond(Duration) AS end_time,
    received_at,
    late_part,
    obs_type AS type,
    multiIf(a['gen_ai.tool.name'] != '', a['gen_ai.tool.name'], SpanName) AS name,
    ServiceName AS service_name,
    multiIf(a['langfuse.environment'] != '', a['langfuse.environment'], r['langfuse.environment'] != '', r['langfuse.environment'],
            a['deployment.environment.name'] != '', a['deployment.environment.name'], r['deployment.environment.name'] != '', r['deployment.environment.name'],
            a['deployment.environment'] != '', a['deployment.environment'], r['deployment.environment'] != '', r['deployment.environment'],
            'default') AS environment,
    a['langfuse.trace.name'] AS trace_name,
    multiIf(a['langfuse.user.id'] != '', a['langfuse.user.id'], a['user.id']) AS user_id,
    multiIf(a['langfuse.session.id'] != '', a['langfuse.session.id'], a['session.id'] != '', a['session.id'], a['gen_ai.conversation.id']) AS session_id,
    if(isValidJSON(a['langfuse.trace.tags']), JSONExtract(a['langfuse.trace.tags'], 'Array(String)'),
       arrayFilter(t -> t != '', splitByChar(',', a['langfuse.trace.tags']))) AS tags,
    StatusCode AS status_code,
    multiIf(a['langfuse.observation.level'] != '', upper(a['langfuse.observation.level']), StatusCode = 'Error', 'ERROR', 'DEFAULT') AS level,
    multiIf(a['gen_ai.provider.name'] != '', a['gen_ai.provider.name'], a['gen_ai.system']) AS provider,
    multiIf(a['langfuse.observation.model.name'] != '', a['langfuse.observation.model.name'], a['gen_ai.response.model'] != '', a['gen_ai.response.model'],
            a['ai.model.id'] != '', a['ai.model.id'], a['gen_ai.request.model'] != '', a['gen_ai.request.model'],
            a['llm.response.model'] != '', a['llm.response.model'], a['llm.model_name'] != '', a['llm.model_name'], a['model']) AS model,
    in_tok AS input_tokens,
    out_tok AS output_tokens,
    CAST(mapFilter((k, v) -> v != 0, map('input', in_tok, 'output', out_tok,
         'cache_read', toUInt64OrZero(a['gen_ai.usage.cache_read.input_tokens']),
         'cache_creation', toUInt64OrZero(a['gen_ai.usage.cache_creation.input_tokens']),
         'reasoning', toUInt64OrZero(a['gen_ai.usage.reasoning.output_tokens']))), 'Map(LowCardinality(String), UInt64)') AS usage_details,
    toDecimal64OrNull(a['gen_ai.usage.cost'], 12) AS provided_cost,
    a['langfuse.observation.prompt.name'] AS prompt_name,
    toUInt32OrZero(a['langfuse.observation.prompt.version']) AS prompt_version,
    a['gen_ai.tool.name'] AS tool_name,
    refs(a['gen_ai.system_instructions']) AS system_ref,
    refs(input_doc) AS input_refs,
    refs(output_doc) AS output_refs,
    toUInt64OrZero(a[concat('otel.payload.', input_key, '.bytes')]) AS input_bytes,
    toUInt64OrZero(a[concat('otel.payload.', output_key, '.bytes')]) AS output_bytes,
    toUInt8(a[concat('otel.payload.', input_key, '.truncated_from')] != '' OR a[concat('otel.payload.', output_key, '.truncated_from')] != '') AS truncated,
    'v1' AS mapping,
    producer_id,
    batch_id,
    row_ordinal
FROM {table}
WHERE obs_type != 'SPAN'
   OR arrayExists(k -> startsWith(k, 'gen_ai.') OR startsWith(k, 'langfuse.') OR startsWith(k, 'llm.'), mapKeys(a));

-- The mapping as data (R-L12): per version, each llm_spans / llm_scores column and the attributes
-- it is read from, in order of precedence, with the convention they come from. Rows are identical
-- on every run (inserted by `ensure`, deduplicated by the key).
CREATE TABLE IF NOT EXISTS {db}.llm_mapping
(
    `version` LowCardinality(String),
    `target` LowCardinality(String),
    `sources` Array(String),
    `convention` String
)
ENGINE = ReplacingMergeTree
ORDER BY (version, target)
SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO {db}.llm_mapping (version, target, sources, convention) VALUES
('v1', 'llm_spans.type', ['langfuse.observation.type', 'openinference.span.kind', 'gen_ai.operation.name', 'langfuse.observation.model.name|gen_ai.request.model|gen_ai.response.model|llm.model_name|model'], 'Langfuse v4.46.0 ObservationTypeMapper priorities 1, 2, 3, 10; OTel GenAI e57c543'),
('v1', 'llm_spans.name', ['gen_ai.tool.name', 'SpanName'], 'Langfuse v4.46.0 extractName'),
('v1', 'llm_spans.environment', ['langfuse.environment', 'deployment.environment.name', 'deployment.environment', '(resource) same keys', 'default'], 'Langfuse v4.46.0 extractEnvironment'),
('v1', 'llm_spans.user_id', ['langfuse.user.id', 'user.id'], 'Langfuse v4.46.0 extractUserId'),
('v1', 'llm_spans.session_id', ['langfuse.session.id', 'session.id', 'gen_ai.conversation.id'], 'Langfuse v4.46.0 extractSessionId'),
('v1', 'llm_spans.model', ['langfuse.observation.model.name', 'gen_ai.response.model', 'ai.model.id', 'gen_ai.request.model', 'llm.response.model', 'llm.model_name', 'model'], 'Langfuse v4.46.0 extractModelName'),
('v1', 'llm_spans.input_tokens', ['gen_ai.usage.input_tokens', 'langfuse.observation.usage_details.input', 'llm.token_count.prompt', 'ai.usage.promptTokens'], 'OTel GenAI e57c543; Langfuse; OpenInference; Vercel AI SDK'),
('v1', 'llm_spans.output_tokens', ['gen_ai.usage.output_tokens', 'langfuse.observation.usage_details.output', 'llm.token_count.completion', 'ai.usage.completionTokens'], 'OTel GenAI e57c543; Langfuse; OpenInference; Vercel AI SDK'),
('v1', 'llm_spans.input_refs', ['gen_ai.input.messages', 'langfuse.observation.input', 'input.value', 'gen_ai.prompt'], 'edge reference documents (FORMAT.md §2.3)'),
('v1', 'llm_spans.output_refs', ['gen_ai.output.messages', 'langfuse.observation.output', 'output.value', 'gen_ai.completion'], 'edge reference documents (FORMAT.md §2.3)'),
('v1', 'llm_spans.system_ref', ['gen_ai.system_instructions'], 'OTel GenAI e57c543'),
('v1', 'llm_scores.*', ['EventName = gen_ai.evaluation.result', 'gen_ai.evaluation.name', 'gen_ai.evaluation.score.value', 'gen_ai.evaluation.score.label', 'langfuse.score.id', 'langfuse.score.kind'], 'OTel GenAI e57c543 evaluation event; D36 facts');
