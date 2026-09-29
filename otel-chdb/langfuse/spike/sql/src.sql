-- Synthetic LLM agent traffic, generated inside ClickHouse (no data leaves the server).
-- {src} is the source database. Deterministic: every random choice is a cityHash64 of its coordinates.
--
-- Shape of the traffic (research/langfuse.md §7.1):
--   {sessions} sessions (conversations) over 3 days, 20 apps, 4 namespaces (tenants), 3 clusters,
--   1..8 turns per session; a turn is one trace: an agent root span, a chat generation, and in half the
--   turns a tool call plus a second generation. A generation's input is the app's system prompt, the
--   whole conversation so far, and the new user message (plus the tool call and result for the second
--   generation): 2-20 KB, growing with the turn, as agent frameworks send it.
--   Spans are exported when they end; received_at = end + 0.2-3 s; 1% of spans arrive 20-120 min late
--   (an edge outage), late_part = 1 for those (the edges' split bound is 15 min, D31).
--   Scores: an LLM-as-judge score on 70% of traces 5-60 s after the trace; a human annotation on 5% of
--   traces 1-48 h later; 2% of judge scores corrected (same score id, new value) 10 min-6 h later;
--   0.5% of judge scores deleted 1-24 h later.
CREATE TABLE {src}.words ENGINE = MergeTree ORDER BY id AS
SELECT number AS id,
       arrayStringConcat(arrayMap(j -> char(97 + cityHash64(number, j) % 26), range(2 + cityHash64(number) % 9)), '') AS w
FROM numbers(4000);

CREATE TABLE {src}.apps ENGINE = MergeTree ORDER BY app AS
WITH (SELECT groupArray(w) FROM {src}.words) AS W
SELECT toUInt8(number) AS app,
       concat('ns-p', toString(number % 4)) AS namespace,
       concat('c', toString(number % 3)) AS cluster,
       concat('agent-', toString(number)) AS service,
       ['gpt-5.1', 'gpt-5.1-mini', 'claude-sonnet-4-5', 'claude-haiku-4-5', 'gemini-2.5-pro', 'llama-4-70b'][1 + number % 6] AS model,
       ['openai', 'openai', 'anthropic', 'anthropic', 'gcp.gemini', 'aws.bedrock'][1 + number % 6] AS provider,
       arrayStringConcat(arrayMap(i -> W[1 + toUInt32(pow((cityHash64(number, 'sys', i) % 1000003) / 1000003., 2.5) * 3999)],
                                  range(150 + cityHash64(number, 'syslen') % 450)), ' ') AS sys_prompt
FROM numbers(20);

CREATE TABLE {src}.turns ENGINE = MergeTree ORDER BY (s, t) AS
WITH (SELECT groupArray(w) FROM {src}.words) AS W
SELECT s, t, app,
       arrayStringConcat(arrayMap(i -> W[1 + toUInt32(pow((cityHash64(s, t, 'u', i) % 1000003) / 1000003., 2.5) * 3999)],
                                  range(40 + cityHash64(s, t, 'ul') % 41)), ' ') AS user_text,
       arrayStringConcat(arrayMap(i -> W[1 + toUInt32(pow((cityHash64(s, t, 'a', i) % 1000003) / 1000003., 2.5) * 3999)],
                                  range(100 + cityHash64(s, t, 'al') % 201)), ' ') AS asst_text,
       cityHash64(s, t, 'tool') % 2 = 0 AS has_tool,
       if(has_tool, arrayStringConcat(arrayMap(i -> W[1 + toUInt32(pow((cityHash64(s, t, 'r', i) % 1000003) / 1000003., 2.5) * 3999)],
                                  range(100 + cityHash64(s, t, 'rl') % 101)), ' '), '') AS tool_text,
       if(has_tool, concat('search ', arrayStringConcat(arrayMap(i -> W[1 + cityHash64(s, t, 'q', i) % 4000], range(5)), ' ')), '') AS tool_args,
       toDateTime64('2026-09-20 00:00:00', 6, 'UTC') + toIntervalMillisecond(toUInt64(cityHash64(s, 'start') % 259200000) + (t - 1) * 90000 + cityHash64(s, t, 'gap') % 30000) AS turn_start,
       500 + cityHash64(s, t, 'd1') % 7500 AS d1_ms,
       50 + cityHash64(s, t, 'dt') % 1950 AS dt_ms,
       500 + cityHash64(s, t, 'd2') % 7500 AS d2_ms
FROM (SELECT number AS s, toUInt8(number % 20) AS app, arrayJoin(range(1, 2 + toUInt32(cityHash64(number, 'T') % 8))) AS t
      FROM numbers({sessions}));

-- One row per span, with the messages as (role, content) arrays; each shape builds its own wire form.
-- System instructions are kept apart (semconv: gen_ai.system_instructions).
CREATE TABLE {src}.spans ENGINE = MergeTree ORDER BY (s, t, k) AS
WITH hist AS (
    SELECT s, groupArray(t) AS ts, groupArray(user_text) AS us, groupArray(asst_text) AS as_
    FROM (SELECT s, t, user_text, asst_text FROM {src}.turns ORDER BY s, t) GROUP BY s
)
SELECT
    x.s AS s, x.t AS t, k, x.app AS app, a.namespace AS namespace, a.cluster AS cluster, a.service AS service,
    a.model AS model, a.provider AS provider,
    lower(hex(MD5(concat('trace-', toString(x.s), '-', toString(x.t))))) AS trace_id,
    substring(lower(hex(MD5(concat('span-', toString(x.s), '-', toString(x.t), '-', toString(k))))), 1, 16) AS span_id,
    if(k = 0, '', substring(lower(hex(MD5(concat('span-', toString(x.s), '-', toString(x.t), '-0')))), 1, 16)) AS parent_span_id,
    concat('sess-', toString(x.s)) AS session_id,
    concat('user-', toString(cityHash64(x.s, 'user') % 3000)) AS user_id,
    concat('support-chat-', toString(x.app % 5)) AS trace_name,
    [concat('app-', toString(x.app)), if(cityHash64(x.s, 'vip') % 10 = 0, 'vip', 'standard')] AS tags,
    ['invoke_agent', 'chat', 'execute_tool', 'chat'][k + 1] AS op,
    ['agent.run', concat('chat ', a.model), 'execute_tool search', concat('chat ', a.model)][k + 1] AS name,
    -- times: root [start, last end + 10 ms]; gen1 [start + 10 ms, + d1]; tool after gen1; gen2 after tool
    x.turn_start + toIntervalMillisecond(multiIf(k = 0, 0, k = 1, 10, k = 2, 15 + x.d1_ms, 20 + x.d1_ms + x.dt_ms)) AS start_time,
    x.turn_start + toIntervalMillisecond(multiIf(k = 0, if(x.has_tool, 30 + x.d1_ms + x.dt_ms + x.d2_ms, 20 + x.d1_ms),
                                                  k = 1, 10 + x.d1_ms, k = 2, 15 + x.d1_ms + x.dt_ms, 20 + x.d1_ms + x.dt_ms + x.d2_ms)) AS end_time,
    if(k IN (1, 3), a.sys_prompt, '') AS system_text,
    -- input messages: history (previous turns), the user's message, and for the second generation the tool exchange
    multiIf(k = 0, [('user', x.user_text)],
            k = 2, [('tool_call', x.tool_args)],
            arrayConcat(
                arrayFlatten(arrayMap(i -> [('user', h.us[i]), ('assistant', h.as_[i])], range(1, x.t))),
                [('user', x.user_text)],
                if(k = 3, [('tool_call', x.tool_args), ('tool', x.tool_text)], []))) AS input_msgs,
    multiIf(k = 0, [('assistant', x.asst_text)],
            k = 2, [('tool', x.tool_text)],
            k = 1 AND x.has_tool, [('tool_call', x.tool_args)],
            [('assistant', x.asst_text)]) AS output_msgs,
    toUInt32((length(system_text) + arraySum(arrayMap(m -> length(m.2) + 30, input_msgs))) / 4) AS input_tokens,
    toUInt32(arraySum(arrayMap(m -> length(m.2) + 30, output_msgs)) / 4) AS output_tokens,
    cityHash64(x.s, x.t, k, 'late') % 100 = 0 AS is_late,
    end_time + toIntervalMillisecond(if(is_late, 1200000 + cityHash64(x.s, x.t, k, 'lt') % 6000000, 200 + cityHash64(x.s, x.t, k, 'rd') % 2800)) AS received_at_ms,
    toDateTime64(received_at_ms, 9, 'UTC') AS received_at,
    toUInt8(is_late) AS late_part
FROM {src}.turns AS x
ARRAY JOIN if(x.has_tool, [0, 1, 2, 3], [0, 1]) AS k
INNER JOIN {src}.apps AS a ON a.app = x.app
INNER JOIN hist AS h ON h.s = x.s
SETTINGS max_memory_usage = 3000000000, join_algorithm = 'hash';

-- "Objects": spans in received_at order, 10,000 per chunk (the consumer inserts one statement per batch of objects).
CREATE TABLE {src}.chunks ENGINE = MergeTree ORDER BY (s, t, k) AS
SELECT s, t, k, toUInt32(intDiv(row_number() OVER (ORDER BY received_at, span_id) - 1, 10000)) AS chunk FROM {src}.spans;

-- Scores as facts with a custody time. kind: 'assert' or 'retract'.
CREATE TABLE {src}.scores ENGINE = MergeTree ORDER BY (event_time) AS
SELECT * FROM (
    SELECT namespace, cluster, trace_id, '' AS observation_id, session_id,
           concat('judge-', trace_id) AS score_id, 'helpfulness' AS name, 'EVAL' AS source, 'NUMERIC' AS data_type,
           round((cityHash64(trace_id, 'v') % 1000) / 1000., 3) AS value,
           end_time + toIntervalMillisecond(5000 + cityHash64(trace_id, 'sd') % 55000) AS event_time,
           'assert' AS kind
    FROM {src}.spans WHERE k = 0 AND cityHash64(trace_id, 'judge') % 10 < 7
    UNION ALL
    SELECT namespace, cluster, trace_id, '', session_id, concat('human-', trace_id), 'correctness', 'ANNOTATION', 'NUMERIC',
           toFloat64(cityHash64(trace_id, 'hv') % 2), end_time + toIntervalMillisecond(3600000 + cityHash64(trace_id, 'hd') % 169200000), 'assert'
    FROM {src}.spans WHERE k = 0 AND cityHash64(trace_id, 'human') % 20 = 0
    UNION ALL
    SELECT namespace, cluster, trace_id, '', session_id, concat('judge-', trace_id), 'helpfulness', 'EVAL', 'NUMERIC',
           round((cityHash64(trace_id, 'v2') % 1000) / 1000., 3), end_time + toIntervalMillisecond(600000 + cityHash64(trace_id, 'cd') % 21000000), 'assert'
    FROM {src}.spans WHERE k = 0 AND cityHash64(trace_id, 'judge') % 10 < 7 AND cityHash64(trace_id, 'corr') % 50 = 0
    UNION ALL
    SELECT namespace, cluster, trace_id, '', session_id, concat('judge-', trace_id), 'helpfulness', 'EVAL', 'NUMERIC',
           0., end_time + toIntervalMillisecond(3600000 + cityHash64(trace_id, 'dd') % 82800000), 'retract'
    FROM {src}.spans WHERE k = 0 AND cityHash64(trace_id, 'judge') % 10 < 7 AND cityHash64(trace_id, 'del') % 200 = 0
) ORDER BY event_time;

-- Model prices, effective-dated (valid time) and recorded (system time). One price cut on day 2 (20%) for
-- two models, recorded 6 h after it took effect: an answer before the recording and after it differ.
CREATE TABLE {src}.prices (model String, usage_type LowCardinality(String), price Decimal(18, 12),
                           valid_from DateTime64(6, 'UTC'), system_from DateTime64(9, 'UTC'))
ENGINE = MergeTree ORDER BY (model, usage_type, valid_from);
INSERT INTO {src}.prices VALUES
 ('gpt-5.1','input',0.00000125,'2026-01-01 00:00:00','2026-01-01 00:00:00'),('gpt-5.1','output',0.00001,'2026-01-01 00:00:00','2026-01-01 00:00:00'),
 ('gpt-5.1-mini','input',0.00000025,'2026-01-01 00:00:00','2026-01-01 00:00:00'),('gpt-5.1-mini','output',0.000002,'2026-01-01 00:00:00','2026-01-01 00:00:00'),
 ('claude-sonnet-4-5','input',0.000003,'2026-01-01 00:00:00','2026-01-01 00:00:00'),('claude-sonnet-4-5','output',0.000015,'2026-01-01 00:00:00','2026-01-01 00:00:00'),
 ('claude-haiku-4-5','input',0.000001,'2026-01-01 00:00:00','2026-01-01 00:00:00'),('claude-haiku-4-5','output',0.000005,'2026-01-01 00:00:00','2026-01-01 00:00:00'),
 ('gemini-2.5-pro','input',0.00000125,'2026-01-01 00:00:00','2026-01-01 00:00:00'),('gemini-2.5-pro','output',0.00001,'2026-01-01 00:00:00','2026-01-01 00:00:00'),
 ('llama-4-70b','input',0.0000006,'2026-01-01 00:00:00','2026-01-01 00:00:00'),('llama-4-70b','output',0.0000006,'2026-01-01 00:00:00','2026-01-01 00:00:00'),
 ('gpt-5.1-mini','input',0.0000002,'2026-09-21 00:00:00','2026-09-21 06:00:00'),('gpt-5.1-mini','output',0.0000016,'2026-09-21 00:00:00','2026-09-21 06:00:00'),
 ('claude-haiku-4-5','input',0.0000008,'2026-09-21 00:00:00','2026-09-21 06:00:00'),('claude-haiku-4-5','output',0.000004,'2026-09-21 00:00:00','2026-09-21 06:00:00');
