#!/usr/bin/env python3
"""Langfuse-shaped LLM traces in three central shapes: bytes, insert CPU, UI query latency, basis behaviour.

A  Langfuse v4's own ClickHouse DDL (events_full + events_core + its MV, scores), rendered from the real
   migrations of a Langfuse checkout (--langfuse), rows as Langfuse's worker writes them: content inline,
   cost stored at ingest, ReplacingMergeTree(event_ts, is_deleted), reads deduplicate (LIMIT 1 BY / FINAL).
B  Our consumer's otel_traces / otel_logs (otap-rs/sql, ClickStack 2.39.1's DDL, option 2), LLM content
   inline in SpanAttributes as the OTel GenAI conventions put it; scores as gen_ai.evaluation.result logs.
C  The proposal: B's tables with content replaced by content-hash references (edge offload), llm_payloads,
   and typed views llm_spans / llm_scores; cost and score resolution at query time, at a basis.

Everything runs inside the shared ClickHouse (HTTP :18123, native :19000 for the client's ProfileEvents).
Databases lfz_src, lfz_a, lfz_b, lfz_c; `drop` removes them. See ../README.md for the method and results.
"""
import argparse, json, os, re, statistics, subprocess, sys, time
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
CH = os.environ.get("CH_URL", "http://localhost:18123/")
CHC = os.environ.get("CHC", "clickhouse")
CHC_PORT = os.environ.get("CHC_PORT", "19000")
MEM = dict(max_memory_usage=3_000_000_000)
DBS = ["lfz_src", "lfz_a", "lfz_b", "lfz_c"]


def q(sql, fmt=None, **settings):
    params = dict(MEM)
    params.update(settings)
    r = requests.post(CH, data=sql.encode(), params=params, timeout=1800)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code}: {r.text[:800]}\n--- {sql[:400]}")
    return r


def qs(sql, **settings):
    return q(sql, **settings).text.strip()


def run_script(text):
    for stmt in [s.strip() for s in re.split(r";\s*\n", text) if s.strip()]:
        body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
        if body:
            q(body)


def client_events(sql):
    """Run one statement with clickhouse client; return its own ProfileEvents totals (as schema_bench.py)."""
    args = [CHC, "client", "--port", CHC_PORT, "--print-profile-events", "--profile-events-delay-ms=-1",
            "--max_memory_usage", str(MEM["max_memory_usage"]), "--query", sql]
    r = subprocess.run(args, capture_output=True, text=True, timeout=1800)
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-1200:] + "\n--- " + sql[:300])
    ev = {}
    for m in re.finditer(r"\[ 0 \] (\w+): (\d+) \(increment\)", r.stderr):
        ev[m.group(1)] = ev.get(m.group(1), 0) + int(m.group(2))
    return ev


# ---------------------------------------------------------------- DDL

def render_langfuse(langfuse_dir, db):
    """The real Langfuse migrations, unclustered, only the statements on the tables shape A uses."""
    mig = os.path.join(langfuse_dir, "packages/shared/clickhouse/migrations/canonical")
    keep = {"scores", "events_full", "events_core", "events_core_mv"}
    out = []
    for f in sorted(os.listdir(mig)):
        if not f.endswith(".up.sql"):
            continue
        t = open(os.path.join(mig, f)).read()
        t = t.replace("{CLICKHOUSE_CLUSTER_CLAUSE}", "").replace("{CLICKHOUSE_REPLICATION_PREFIX}", "")
        t = re.sub(r"\{CLICKHOUSE_CLUSTERED_ONLY:[^}]*\}", "", t)
        t = re.sub(r"\{CLICKHOUSE_UNCLUSTERED_ONLY:([^}]*)\}", r"\1", t)
        t = re.sub(r"\{CLICKHOUSE_HISTORICAL_FINAL_NEWLINES:[^}]*\}", "", t)
        for stmt in re.split(r";\s*(?:\n|$)", t):
            s = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
            m = re.match(r"(CREATE TABLE|CREATE MATERIALIZED VIEW|ALTER TABLE)\s+(?:IF NOT EXISTS\s+)?(\w+)", s)
            if not m or m.group(2) not in keep:
                continue
            s = re.sub(r"\b(events_full|events_core_mv|events_core|scores)\b", lambda x: f"{db}.{x.group(1)}", s)
            # the view's source/target names were qualified above; `FROM {db}.events_full` etc.
            out.append((f, s))
    return out


def setup(args):
    for db in DBS:
        q(f"DROP DATABASE IF EXISTS {db} SYNC")
        q(f"CREATE DATABASE {db}")
    t0 = time.time()
    run_script(open(os.path.join(HERE, "sql/src.sql")).read().replace("{src}", "lfz_src").replace("{sessions}", str(args.sessions)))
    print(f"source generated in {time.time() - t0:.0f} s:",
          qs("SELECT count(), countIf(k IN (1,3)), round(avgIf(length(system_text) + arraySum(arrayMap(m -> length(m.2), input_msgs)), k IN (1,3))), "
             "maxIf(length(system_text) + arraySum(arrayMap(m -> length(m.2), input_msgs)), k IN (1,3)), countIf(late_part = 1) FROM lfz_src.spans"))
    # payload facts: one per (message, cluster, namespace, received day), first seen in chunk `first_chunk`
    q("""CREATE TABLE lfz_src.payloads ENGINE = MergeTree ORDER BY (cluster, namespace, hash) AS
         SELECT cluster, namespace, toDate(received_at) AS received_day, sipHash128(j) AS hash, any(j) AS content, min(chunk) AS first_chunk
         FROM (SELECT sp.cluster AS cluster, sp.namespace AS namespace, sp.received_at AS received_at, ch.chunk AS chunk,
                      arrayJoin(arrayConcat(
                          if(sp.system_text = '', [], [concat('[{"type":"text","content":"', sp.system_text, '"}]')]),
                          arrayMap(m -> concat('{"role":"', m.1, '","parts":[{"type":"text","content":"', m.2, '"}]}'), sp.input_msgs),
                          arrayMap(m -> concat('{"role":"', m.1, '","parts":[{"type":"text","content":"', m.2, '"}]}'), sp.output_msgs))) AS j
               FROM lfz_src.spans AS sp INNER JOIN lfz_src.chunks AS ch ON ch.s = sp.s AND ch.t = sp.t AND ch.k = sp.k)
         GROUP BY cluster, namespace, received_day, hash""")
    # A: Langfuse's DDL
    stmts = render_langfuse(args.langfuse, "lfz_a")
    for f, s in stmts:
        try:
            q(s)
        except RuntimeError as e:
            print(f"A: {f}: skipped: {str(e)[:160]}")
    print(f"A: {len(stmts)} Langfuse statements applied from {args.langfuse}")
    # B and C: the consumer's tables
    for db in ("lfz_b", "lfz_c"):
        for sig in ("traces", "logs"):
            run_script(open(os.path.join(ROOT, f"otap-rs/sql/otel_{sig}.sql")).read().replace("{table}", f"{db}.otel_{sig}"))
    run_script(open(os.path.join(HERE, "sql/c_llm.sql")).read().replace("{db}", "lfz_c"))
    q("INSERT INTO lfz_c.llm_prices SELECT * FROM lfz_src.prices")


# ---------------------------------------------------------------- the rows each shape receives

MSG = "concat('{\"role\":\"', m.1, '\",\"parts\":[{\"type\":\"text\",\"content\":\"', m.2, '\"}]}')"
SYS = "concat('[{\"type\":\"text\",\"content\":\"', sp.system_text, '\"}]')"
IN_JSON = f"concat('[', arrayStringConcat(arrayMap(m -> {MSG}, sp.input_msgs), ','), ']')"
OUT_JSON = f"concat('[', arrayStringConcat(arrayMap(m -> {MSG}, sp.output_msgs), ','), ']')"
IN_REFS = f"concat('[', arrayStringConcat(arrayMap(m -> concat('\"h:', lower(hex(sipHash128({MSG}))), '\"'), sp.input_msgs), ','), ']')"
OUT_REFS = f"concat('[', arrayStringConcat(arrayMap(m -> concat('\"h:', lower(hex(sipHash128({MSG}))), '\"'), sp.output_msgs), ','), ']')"
SYS_REF = f"concat('[\"h:', lower(hex(sipHash128({SYS}))), '\"]')"
FROM_CHUNK = "FROM lfz_src.spans AS sp INNER JOIN lfz_src.chunks AS ch ON ch.s = sp.s AND ch.t = sp.t AND ch.k = sp.k WHERE ch.chunk = {chunk}"
# price at ingest, as Langfuse's worker computes it: the price valid at the span's start among those the
# price table held when the row was processed (system_from <= received_at).
PRICE = ("tupleElement(arrayLast(x -> x.4 <= sp.start_time AND x.5 <= {at}, arraySort(x -> x.4, arrayFilter(x -> x.1 = sp.model AND x.2 = '{ut}', "
         "(SELECT groupArray((model, usage_type, price, valid_from, system_from)) FROM lfz_src.prices)))), 3)")


def select_a(chunk):
    pin, pout = PRICE.format(at="sp.received_at", ut="input"), PRICE.format(at="sp.received_at", ut="output")
    gen = "sp.k IN (1, 3)"
    return f"""SELECT sp.namespace AS project_id, sp.trace_id, sp.span_id, sp.parent_span_id, sp.start_time, sp.end_time, sp.name,
       ['AGENT', 'GENERATION', 'TOOL', 'GENERATION'][sp.k + 1] AS type, 'production' AS environment, sp.trace_name, sp.user_id, sp.session_id,
       sp.tags, 'DEFAULT' AS level, sp.k = 0 AS is_app_root,
       if({gen}, 'support-agent', '') AS prompt_name, if({gen}, 3, NULL) AS prompt_version,
       if({gen}, sp.model, '') AS model_id, if({gen}, sp.model, '') AS provided_model_name,
       if({gen}, '{{"temperature":0.2}}', '') AS model_parameters,
       if({gen}, map('input', toUInt64(sp.input_tokens), 'output', toUInt64(sp.output_tokens), 'total', toUInt64(sp.input_tokens + sp.output_tokens)), map()) AS usage_details,
       if({gen}, map('input', toDecimal128(sp.input_tokens * {pin}, 12), 'output', toDecimal128(sp.output_tokens * {pout}, 12),
                     'total', toDecimal128(sp.input_tokens * {pin} + sp.output_tokens * {pout}, 12)), map()) AS cost_details,
       if(sp.k = 2, ['search'], []) AS tool_call_names,
       if({gen}, concat('[', {SYS}, ',', substring({IN_JSON}, 2)), {IN_JSON}) AS input, {OUT_JSON} AS output,
       ['app', 'cluster'] AS metadata_names, [toString(sp.app), sp.cluster] AS metadata_values,
       'otel' AS source, sp.service AS service_name, '1.4.2' AS service_version,
       'opentelemetry.instrumentation.openai_v2' AS scope_name, '0.5b0' AS scope_version,
       'python' AS telemetry_sdk_language, 'opentelemetry' AS telemetry_sdk_name, '1.37.0' AS telemetry_sdk_version,
       concat('otel/', sp.namespace, '/', sp.trace_id, '.json') AS blob_storage_file_path,
       length(input) + length(output) AS event_bytes, sp.received_at AS created_at, sp.received_at AS updated_at,
       sp.received_at AS event_ts, 0 AS is_deleted
{FROM_CHUNK.format(chunk=chunk)}"""


A_COLS = ("project_id, trace_id, span_id, parent_span_id, start_time, end_time, name, type, environment, trace_name, user_id, session_id, "
          "tags, level, is_app_root, prompt_name, prompt_version, model_id, provided_model_name, model_parameters, usage_details, cost_details, "
          "tool_call_names, input, output, metadata_names, metadata_values, source, service_name, service_version, scope_name, scope_version, "
          "telemetry_sdk_language, telemetry_sdk_name, telemetry_sdk_version, blob_storage_file_path, event_bytes, created_at, updated_at, event_ts, is_deleted")


def select_otel(chunk, refs):
    gen = "sp.k IN (1, 3)"
    sysv = SYS_REF if refs else SYS
    inv = IN_REFS if refs else IN_JSON
    outv = OUT_REFS if refs else OUT_JSON
    extra = (f", 'otel.payload.input_bytes', toString(length({IN_JSON}) + if({gen}, length({SYS}), 0)), 'otel.payload.output_bytes', toString(length({OUT_JSON}))"
             if refs else "")
    return f"""SELECT sp.start_time AS Timestamp, sp.trace_id AS TraceId, sp.span_id AS SpanId, sp.parent_span_id AS ParentSpanId, '' AS TraceState,
       sp.name AS SpanName, if({gen}, 'Client', 'Internal') AS SpanKind, sp.service AS ServiceName,
       map('service.name', sp.service, 'service.version', '1.4.2', 'k8s.cluster.name', sp.cluster, 'k8s.namespace.name', sp.namespace,
           'k8s.deployment.name', sp.service, 'k8s.pod.name', concat(sp.service, '-7d9f8-', toString(cityHash64(sp.s) % 5)),
           'deployment.environment.name', 'production', 'telemetry.sdk.language', 'python', 'telemetry.sdk.name', 'opentelemetry',
           'telemetry.sdk.version', '1.37.0') AS ResourceAttributes,
       'opentelemetry.instrumentation.openai_v2' AS ScopeName, '0.5b0' AS ScopeVersion,
       mapFilter((k, v) -> v != '', map(
           'gen_ai.operation.name', sp.op, 'langfuse.trace.name', sp.trace_name, 'user.id', sp.user_id, 'session.id', sp.session_id,
           'gen_ai.conversation.id', sp.session_id, 'langfuse.trace.tags', toJSONString(sp.tags),
           'gen_ai.agent.name', if(sp.k = 0, sp.service, ''),
           'gen_ai.provider.name', if({gen}, sp.provider, ''), 'gen_ai.request.model', if({gen}, sp.model, ''),
           'gen_ai.response.model', if({gen}, sp.model, ''), 'gen_ai.request.temperature', if({gen}, '0.2', ''),
           'gen_ai.response.finish_reasons', if({gen}, '["stop"]', ''),
           'gen_ai.usage.input_tokens', if({gen}, toString(sp.input_tokens), ''), 'gen_ai.usage.output_tokens', if({gen}, toString(sp.output_tokens), ''),
           'langfuse.observation.prompt.name', if({gen}, 'support-agent', ''), 'langfuse.observation.prompt.version', if({gen}, '3', ''),
           'gen_ai.tool.name', if(sp.k = 2, 'search', ''),
           'gen_ai.system_instructions', if({gen}, {sysv}, ''),
           'gen_ai.input.messages', {inv}, 'gen_ai.output.messages', {outv}{extra})) AS SpanAttributes,
       toUInt64(dateDiff('microsecond', sp.start_time, sp.end_time) * 1000) AS Duration, 'Ok' AS StatusCode, '' AS StatusMessage,
       cityHash64(sp.service, sp.cluster, sp.namespace, cityHash64(sp.s) % 5) AS resource_id,
       concat('edge-', sp.cluster) AS producer_id, 'e1' AS producer_epoch, toUInt64(ch.chunk) AS batch_id,
       toUInt32(rowNumberInAllBlocks()) AS row_ordinal, sp.received_at AS received_at, 2 AS schema_version,
       concat('ck-', toString(ch.chunk)) AS content_key, sp.late_part AS late_part
{FROM_CHUNK.format(chunk=chunk)}"""


OTEL_COLS = ("Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind, ServiceName, ResourceAttributes, ScopeName, ScopeVersion, "
             "SpanAttributes, Duration, StatusCode, StatusMessage, resource_id, producer_id, producer_epoch, batch_id, row_ordinal, received_at, "
             "schema_version, content_key, late_part")


def select_payloads(chunk):
    return f"""SELECT hash, received_day, cluster, namespace, toUInt32(length(content)) AS bytes, content
FROM lfz_src.payloads WHERE first_chunk = {chunk}"""


# ---------------------------------------------------------------- loading and measuring inserts

def load(args):
    nchunks = int(qs("SELECT max(chunk) + 1 FROM lfz_src.chunks"))
    rows = {int(a): (int(b), int(c)) for a, b, c in
            (l.split("\t") for l in qs("SELECT ch.chunk, count(), countIf(sp.k IN (1,3)) FROM lfz_src.spans AS sp INNER JOIN lfz_src.chunks AS ch "
                                       "ON ch.s = sp.s AND ch.t = sp.t AND ch.k = sp.k GROUP BY ch.chunk FORMAT TSV").splitlines())}
    acc = {s: dict(cpu=0, sel=0, rows=0, gens=0) for s in ("A", "B", "C", "C_payloads")}
    plan = {
        "A": lambda c: (f"INSERT INTO lfz_a.events_full ({A_COLS}) {select_a(c)}", select_a(c)),
        "B": lambda c: (f"INSERT INTO lfz_b.otel_traces ({OTEL_COLS}) {select_otel(c, False)}", select_otel(c, False)),
        "C_payloads": lambda c: (f"INSERT INTO lfz_c.llm_payloads {select_payloads(c)}", select_payloads(c)),
        "C": lambda c: (f"INSERT INTO lfz_c.otel_traces ({OTEL_COLS}) {select_otel(c, True)}", select_otel(c, True)),
    }
    for c in range(nchunks):
        for shape in ("A", "B", "C_payloads", "C"):
            ins, sel = plan[shape](c)
            e_sel = client_events(sel + " FORMAT Null")
            e_ins = client_events(ins)
            acc[shape]["cpu"] += e_ins.get("OSCPUVirtualTimeMicroseconds", 0)
            acc[shape]["sel"] += e_sel.get("OSCPUVirtualTimeMicroseconds", 0)
            acc[shape]["rows"] += rows[c][0]
            acc[shape]["gens"] += rows[c][1]
        print(f"chunk {c + 1}/{nchunks}", flush=True)
    # scores: one statement per shape (A: Langfuse's scores table; B, C: gen_ai.evaluation.result log records)
    score_a = """INSERT INTO lfz_a.scores (id, timestamp, project_id, environment, trace_id, observation_id, session_id, name, value, source, data_type,
                 created_at, updated_at, event_ts, is_deleted)
                 SELECT score_id, min(event_time) OVER (PARTITION BY score_id), namespace, 'production', trace_id, NULL, session_id, name, value, source, data_type,
                        min(event_time) OVER (PARTITION BY score_id), event_time, event_time + toIntervalSecond(1), kind = 'retract' FROM lfz_src.scores"""
    score_log = """INSERT INTO {db}.otel_logs (Timestamp, TraceId, SpanId, SeverityText, SeverityNumber, ServiceName, Body, ResourceAttributes,
                   LogAttributes, EventName, resource_id, producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version, content_key, late_part)
                   SELECT event_time, trace_id, '', 'INFO', 9, 'langfuse-evals', '',
                          map('service.name', 'langfuse-evals', 'k8s.cluster.name', cluster, 'k8s.namespace.name', namespace),
                          map('gen_ai.evaluation.name', name, 'gen_ai.evaluation.score.value', toString(value), 'langfuse.score.id', score_id,
                              'langfuse.score.source', source, 'langfuse.score.data_type', data_type, 'langfuse.score.kind', kind, 'session.id', session_id),
                          'gen_ai.evaluation.result', cityHash64(cluster, namespace), 'edge-scores', 'e1', 0, toUInt32(rowNumberInAllBlocks()),
                          toDateTime64(event_time + toIntervalSecond(1), 9), 2, 'ck-scores', 0
                   FROM lfz_src.scores"""
    sc = {}
    for shape, stmt in (("A", score_a), ("B", score_log.format(db="lfz_b")), ("C", score_log.format(db="lfz_c"))):
        sc[shape] = client_events(stmt).get("OSCPUVirtualTimeMicroseconds", 0)
    nscores = int(qs("SELECT count() FROM lfz_src.scores"))
    res = dict(chunks=nchunks, insert=acc, scores_cpu=sc, nscores=nscores, load=os.getloadavg())
    json.dump(res, open(os.path.join(HERE, "results/insert.json"), "w"), indent=1)
    print(json.dumps(res, indent=1))


# ---------------------------------------------------------------- bytes

TABLES = {"A": ["lfz_a.events_full", "lfz_a.events_core", "lfz_a.scores"],
          "B": ["lfz_b.otel_traces", "lfz_b.otel_traces_kv_rollup_15m", "lfz_b.otel_logs", "lfz_b.otel_logs_kv_rollup_15m"],
          "C": ["lfz_c.otel_traces", "lfz_c.otel_traces_kv_rollup_15m", "lfz_c.llm_payloads", "lfz_c.llm_spans", "lfz_c.otel_logs",
                "lfz_c.otel_logs_kv_rollup_15m", "lfz_c.llm_scores"]}


def sizes(args):
    out = {}
    merge = {}
    for shape, tabs in TABLES.items():
        for t in tabs:
            ev = client_events(f"OPTIMIZE TABLE {t} FINAL")
            merge[t] = ev.get("OSCPUVirtualTimeMicroseconds", 0)
    time.sleep(2)
    for shape, tabs in TABLES.items():
        for t in tabs:
            db, name = t.split(".")
            r = qs(f"SELECT sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes), sum(bytes_on_disk) FROM system.parts "
                   f"WHERE database = '{db}' AND table = '{name}' AND active FORMAT TSV").split("\t")
            cols = qs(f"SELECT name, data_compressed_bytes FROM system.columns WHERE database = '{db}' AND table = '{name}' "
                      f"ORDER BY data_compressed_bytes DESC LIMIT 6 FORMAT TSV")
            out[t] = dict(rows=int(r[0] or 0), compressed=int(r[1] or 0), uncompressed=int(r[2] or 0), on_disk=int(r[3] or 0),
                          merge_cpu_us=merge[t], top_columns=[l.split("\t") for l in cols.splitlines()])
    json.dump(out, open(os.path.join(HERE, "results/sizes.json"), "w"), indent=1)
    for t, v in out.items():
        print(f"{t:40s} rows {v['rows']:>9} on disk {v['on_disk'] / 1e6:9.1f} MB  uncompressed {v['uncompressed'] / 1e6:9.1f} MB  merge {v['merge_cpu_us'] / 1e6:6.1f} s")


# ---------------------------------------------------------------- queries (the Langfuse UI's main reads)

NS = "ns-p1"
DAY = ("'2026-09-22 00:00:00'", "'2026-09-23 00:00:00'")
WEEK = ("'2026-09-19 00:00:00'", "'2026-09-26 00:00:00'")
PRICE_Q = ("tupleElement(arrayLast(x -> x.4 <= start_time AND x.5 < {basis}, arraySort(x -> x.4, arrayFilter(x -> x.1 = model AND x.2 = '{ut}', "
           "(SELECT groupArray((model, usage_type, price, valid_from, system_from)) FROM {db}.llm_prices)))), 3)")


def cost_expr(db, basis, tokens_in="input_tokens", tokens_out="output_tokens"):
    return (f"({tokens_in} * {PRICE_Q.format(basis=basis, ut='input', db=db)} + "
            f"{tokens_out} * {PRICE_Q.format(basis=basis, ut='output', db=db)})")


def resolved_scores(db, basis, where=""):
    # latest fact per score id among those received before the basis; a retract hides the score
    return f"""(SELECT score_id, f.1 AS trace_id, f.2 AS name, f.3 AS value FROM
               (SELECT score_id, argMax(tuple(trace_id, name, value, kind), (received_at, producer_id, batch_id, row_ordinal)) AS f
                FROM {db}.llm_scores WHERE namespace = '{NS}' AND received_at < {basis} {where} GROUP BY score_id)
               WHERE f.4 = 'assert')"""


A_READ = ("project_id, trace_id, span_id, parent_span_id, name, type, is_app_root, trace_name, user_id, session_id, tags, start_time, end_time, "
          "provided_model_name, usage_details, calculated_total_cost, event_ts")


def queries(trace_id, word, basis):
    A, C = "lfz_a", "lfz_c"
    b_cost_in = "toUInt32OrZero(SpanAttributes['gen_ai.usage.input_tokens'])"
    b_cost_out = "toUInt32OrZero(SpanAttributes['gen_ai.usage.output_tokens'])"
    b_model = "SpanAttributes['gen_ai.request.model']"
    b_price = lambda ut: (f"tupleElement(arrayLast(x -> x.4 <= Timestamp AND x.5 < {basis}, arraySort(x -> x.4, arrayFilter(x -> x.1 = {b_model} AND x.2 = '{ut}', "
                          f"(SELECT groupArray((model, usage_type, price, valid_from, system_from)) FROM lfz_c.llm_prices)))), 3)")
    b_cost = f"({b_cost_in} * {b_price('input')} + {b_cost_out} * {b_price('output')})"
    b_scope = f"ResourceAttributes['k8s.namespace.name'] = '{NS}'"
    b_scores = f"""(SELECT score_id, f.1 AS trace_id, f.2 AS name, f.3 AS value FROM (SELECT LogAttributes['langfuse.score.id'] AS score_id,
                           argMax(tuple(TraceId, LogAttributes['gen_ai.evaluation.name'], toFloat64OrZero(LogAttributes['gen_ai.evaluation.score.value']),
                                        LogAttributes['langfuse.score.kind']), (received_at, producer_id, batch_id, row_ordinal)) AS f
                    FROM lfz_b.otel_logs WHERE {b_scope} AND EventName = 'gen_ai.evaluation.result' AND received_at < {basis}
                    GROUP BY score_id) WHERE f.4 = 'assert')"""
    c_payload_map = lambda sp: f"""(SELECT CAST((groupArray(hash), groupArray(content)), 'Map(FixedString(16), String)') AS m FROM
            (SELECT hash, any(content) AS content FROM {C}.llm_payloads
             WHERE cluster IN (SELECT DISTINCT cluster FROM {sp}) AND namespace = '{NS}'
               AND received_day IN (SELECT DISTINCT toDate(received_at) FROM {sp})
               AND hash IN (SELECT arrayJoin(arrayConcat(system_ref, input_refs, output_refs)) FROM {sp}) GROUP BY hash))"""
    return {
        # Q1: the traces table, one day, newest 50, with latency, cost, tokens and the average score
        "Q1 trace list": {
            "A": f"""WITH tr AS (SELECT trace_id, anyIf(name, is_app_root) AS name, any(trace_name) AS tname, any(user_id) AS uid,
                            any(session_id) AS sid, any(tags) AS tg, min(start_time) AS ts, max(end_time) - min(start_time) AS latency,
                            sum(calculated_total_cost) AS cost, sum(usage_details['total']) AS tokens
                     FROM (SELECT {A_READ} FROM {A}.events_core WHERE project_id = '{NS}' AND start_time >= {DAY[0]} AND start_time < {DAY[1]}
                           ORDER BY event_ts DESC LIMIT 1 BY span_id, project_id)
                     GROUP BY trace_id ORDER BY ts DESC LIMIT 50)
                     SELECT tr.*, s.avg_score FROM tr LEFT JOIN (SELECT trace_id, avg(value) AS avg_score FROM {A}.scores FINAL
                         WHERE project_id = '{NS}' AND trace_id IN (SELECT trace_id FROM tr) GROUP BY trace_id) AS s USING trace_id ORDER BY ts DESC""",
            "B": f"""WITH tr AS (SELECT TraceId, anyIf(SpanName, ParentSpanId = '') AS name, any(SpanAttributes['langfuse.trace.name']) AS tname,
                            any(SpanAttributes['user.id']) AS uid, any(SpanAttributes['session.id']) AS sid, min(Timestamp) AS ts,
                            max(Timestamp + toIntervalNanosecond(Duration)) - min(Timestamp) AS latency, sum({b_cost}) AS cost,
                            sum({b_cost_in} + {b_cost_out}) AS tokens
                     FROM lfz_b.otel_traces WHERE {b_scope} AND SpanAttributes['gen_ai.operation.name'] != ''
                       AND Timestamp >= {DAY[0]} AND Timestamp < {DAY[1]} AND received_at < {basis}
                     GROUP BY TraceId ORDER BY ts DESC LIMIT 50)
                     SELECT tr.*, s.avg_score FROM tr LEFT JOIN (SELECT trace_id AS TraceId, avg(value) AS avg_score FROM {b_scores}
                         WHERE trace_id IN (SELECT TraceId FROM tr) GROUP BY TraceId) AS s USING TraceId ORDER BY ts DESC""",
            "C": f"""WITH tr AS (SELECT trace_id, anyIf(name, parent_span_id = '') AS name, any(trace_name) AS tname, any(user_id) AS uid,
                            any(session_id) AS sid, any(tags) AS tg, min(start_time) AS ts, max(end_time) - min(start_time) AS latency,
                            sum({cost_expr(C, basis)}) AS cost, sum(input_tokens + output_tokens) AS tokens
                     FROM {C}.llm_spans WHERE namespace = '{NS}' AND start_time >= {DAY[0]} AND start_time < {DAY[1]} AND received_at < {basis}
                     GROUP BY trace_id ORDER BY ts DESC LIMIT 50)
                     SELECT tr.*, s.avg_score FROM tr LEFT JOIN (SELECT trace_id, avg(value) AS avg_score FROM {resolved_scores(C, basis)}
                         WHERE trace_id IN (SELECT trace_id FROM tr) GROUP BY trace_id) AS s USING trace_id ORDER BY ts DESC""",
        },
        # Q2: one trace, every observation with its full input and output, and its scores
        "Q2 trace detail": {
            "A": f"""SELECT span_id, parent_span_id, name, type, start_time, end_time, input, output, cost_details, usage_details,
                            (SELECT groupArray((name, value)) FROM {A}.scores FINAL WHERE project_id = '{NS}' AND trace_id = '{trace_id}') AS sc
                     FROM {A}.events_full WHERE project_id = '{NS}' AND trace_id = '{trace_id}'
                     ORDER BY event_ts DESC LIMIT 1 BY span_id, project_id""",
            "B": f"""SELECT SpanId, ParentSpanId, SpanName, Timestamp, Duration, SpanAttributes,
                            (SELECT groupArray((name, value)) FROM {b_scores} WHERE trace_id = '{trace_id}') AS sc
                     FROM lfz_b.otel_traces WHERE {b_scope} AND TraceId = '{trace_id}' AND received_at < {basis}""",
            "C": f"""WITH sp AS (SELECT * FROM {C}.llm_spans WHERE namespace = '{NS}' AND trace_id = '{trace_id}' AND received_at < {basis})
                     SELECT span_id, parent_span_id, name, type, start_time, end_time,
                            concat('[', arrayStringConcat(arrayMap(h -> pm.m[h], arrayConcat(system_ref, input_refs)), ','), ']') AS input,
                            concat('[', arrayStringConcat(arrayMap(h -> pm.m[h], output_refs), ','), ']') AS output,
                            {cost_expr(C, basis)} AS cost,
                            (SELECT groupArray((name, value)) FROM {resolved_scores(C, basis, f"AND trace_id = '{trace_id}'")}) AS sc
                     FROM sp CROSS JOIN {c_payload_map('sp')} AS pm""",
        },
        # Q3: sessions, a week, 50 most recent
        "Q3 sessions": {
            "A": f"""SELECT session_id, uniq(trace_id) AS traces, min(start_time) AS first, max(end_time) AS last, sum(calculated_total_cost) AS cost,
                            sum(usage_details['total']) AS tokens, any(user_id)
                     FROM (SELECT {A_READ} FROM {A}.events_core WHERE project_id = '{NS}' AND start_time >= {WEEK[0]} AND start_time < {WEEK[1]} AND session_id != ''
                           ORDER BY event_ts DESC LIMIT 1 BY span_id, project_id)
                     GROUP BY session_id ORDER BY last DESC LIMIT 50""",
            "B": f"""SELECT SpanAttributes['session.id'] AS sid, uniq(TraceId), min(Timestamp), max(Timestamp + toIntervalNanosecond(Duration)) AS last,
                            sum({b_cost}), sum({b_cost_in} + {b_cost_out}), any(SpanAttributes['user.id'])
                     FROM lfz_b.otel_traces WHERE {b_scope} AND SpanAttributes['gen_ai.operation.name'] != '' AND sid != ''
                       AND Timestamp >= {WEEK[0]} AND Timestamp < {WEEK[1]} AND received_at < {basis}
                     GROUP BY sid ORDER BY last DESC LIMIT 50""",
            "C": f"""SELECT session_id, uniq(trace_id), min(start_time), max(end_time) AS last, sum({cost_expr(C, basis)}),
                            sum(input_tokens + output_tokens), any(user_id)
                     FROM {C}.llm_spans WHERE namespace = '{NS}' AND session_id != '' AND start_time >= {WEEK[0]} AND start_time < {WEEK[1]}
                       AND received_at < {basis}
                     GROUP BY session_id ORDER BY last DESC LIMIT 50""",
        },
        # Q4: cost by model and day
        "Q4 cost by model/day": {
            "A": f"""SELECT toDate(start_time) AS d, provided_model_name, sum(calculated_total_cost), sum(usage_details['total'])
                     FROM (SELECT {A_READ} FROM {A}.events_core WHERE project_id = '{NS}' AND type = 'GENERATION' AND start_time >= {WEEK[0]} AND start_time < {WEEK[1]}
                           ORDER BY event_ts DESC LIMIT 1 BY span_id, project_id)
                     GROUP BY d, provided_model_name ORDER BY d, provided_model_name""",
            "B": f"""SELECT toDate(Timestamp) AS d, {b_model} AS m, sum({b_cost}), sum({b_cost_in} + {b_cost_out})
                     FROM lfz_b.otel_traces WHERE {b_scope} AND SpanAttributes['gen_ai.operation.name'] = 'chat'
                       AND Timestamp >= {WEEK[0]} AND Timestamp < {WEEK[1]} AND received_at < {basis}
                     GROUP BY d, m ORDER BY d, m""",
            "C": f"""SELECT toDate(start_time) AS d, model, sum({cost_expr(C, basis)}), sum(input_tokens + output_tokens)
                     FROM {C}.llm_spans WHERE namespace = '{NS}' AND type = 'GENERATION' AND start_time >= {WEEK[0]} AND start_time < {WEEK[1]}
                       AND received_at < {basis}
                     GROUP BY d, model ORDER BY d, model""",
        },
        # Q5: score distributions, and the average judge score per model (scores joined to the traces' generations)
        "Q5 scores": {
            "A": f"""SELECT name, floor(value * 10) AS bucket, count() FROM {A}.scores FINAL
                     WHERE project_id = '{NS}' AND timestamp >= {WEEK[0]} AND timestamp < {WEEK[1]} GROUP BY name, bucket ORDER BY name, bucket
                     UNION ALL
                     SELECT concat('avg:', e.provided_model_name), 0, avg(s.value) FROM {A}.scores AS s FINAL
                     INNER JOIN (SELECT DISTINCT trace_id, provided_model_name FROM {A}.events_core WHERE project_id = '{NS}' AND type = 'GENERATION'
                                 AND start_time >= {WEEK[0]} AND start_time < {WEEK[1]}) AS e ON e.trace_id = s.trace_id
                     WHERE s.project_id = '{NS}' AND s.name = 'helpfulness' GROUP BY e.provided_model_name""",
            "B": f"""SELECT name, floor(value * 10) AS bucket, count() FROM {b_scores} GROUP BY name, bucket ORDER BY name, bucket
                     UNION ALL
                     SELECT concat('avg:', e.m), 0, avg(s.value) FROM {b_scores} AS s
                     INNER JOIN (SELECT DISTINCT TraceId, {b_model} AS m FROM lfz_b.otel_traces WHERE {b_scope}
                                 AND SpanAttributes['gen_ai.operation.name'] = 'chat' AND Timestamp >= {WEEK[0]} AND Timestamp < {WEEK[1]}
                                 AND received_at < {basis}) AS e ON e.TraceId = s.trace_id
                     WHERE s.name = 'helpfulness' GROUP BY e.m""",
            "C": f"""SELECT name, floor(value * 10) AS bucket, count() FROM {resolved_scores(C, basis)} GROUP BY name, bucket ORDER BY name, bucket
                     UNION ALL
                     SELECT concat('avg:', e.model), 0, avg(s.value) FROM {resolved_scores(C, basis, "AND name = 'helpfulness'")} AS s
                     INNER JOIN (SELECT DISTINCT trace_id, model FROM {C}.llm_spans WHERE namespace = '{NS}' AND type = 'GENERATION'
                                 AND start_time >= {WEEK[0]} AND start_time < {WEEK[1]} AND received_at < {basis}) AS e ON e.trace_id = s.trace_id
                     GROUP BY e.model""",
        },
        # Q6: full-text search in generation outputs, one day
        "Q6 output search": {
            "A": f"""SELECT trace_id, span_id, start_time, leftUTF8(output, 200) FROM {A}.events_full
                     WHERE project_id = '{NS}' AND type = 'GENERATION' AND start_time >= {DAY[0]} AND start_time < {DAY[1]}
                       AND hasToken(lower(output), '{word}') ORDER BY start_time DESC LIMIT 50""",
            "B": f"""SELECT TraceId, SpanId, Timestamp, leftUTF8(SpanAttributes['gen_ai.output.messages'], 200) FROM lfz_b.otel_traces
                     WHERE {b_scope} AND SpanAttributes['gen_ai.operation.name'] = 'chat' AND Timestamp >= {DAY[0]} AND Timestamp < {DAY[1]}
                       AND received_at < {basis} AND hasToken(lower(SpanAttributes['gen_ai.output.messages']), '{word}')
                     ORDER BY Timestamp DESC LIMIT 50""",
            "C": f"""SELECT trace_id, span_id, start_time FROM {C}.llm_spans
                     WHERE namespace = '{NS}' AND type = 'GENERATION' AND start_time >= {DAY[0]} AND start_time < {DAY[1]} AND received_at < {basis}
                       AND hasAny(output_refs, (SELECT groupArray(hash) FROM {C}.llm_payloads WHERE namespace = '{NS}'
                                                AND received_day >= toDate({DAY[0]}) AND received_day <= toDate({DAY[1]}) + 1
                                                AND hasToken(lower(content), '{word}')))
                     ORDER BY start_time DESC LIMIT 50""",
        },
    }


def time_query(sql, reps):
    ms, summ = [], None
    for i in range(reps + 1):
        t0 = time.perf_counter()
        r = q(sql + " FORMAT Null", use_query_cache=0)
        dt = (time.perf_counter() - t0) * 1000
        summ = json.loads(r.headers.get("X-ClickHouse-Summary", "{}"))
        if i:
            ms.append(dt)
    return statistics.median(ms), min(ms), max(ms), int(summ.get("read_rows", 0)), int(summ.get("read_bytes", 0))


def bench_queries(args):
    trace_id = qs(f"SELECT trace_id FROM lfz_src.spans WHERE namespace = '{NS}' AND k = 3 AND t = 6 AND start_time >= {DAY[0]} ORDER BY start_time LIMIT 1")
    word = qs("SELECT w FROM lfz_src.words WHERE id = 2500")
    basis = "toDateTime64('2026-09-26 00:00:00', 9, 'UTC')"
    out = {}
    for name, per in queries(trace_id, word, basis).items():
        out[name] = {}
        for shape, sql in per.items():
            out[name][shape] = time_query(sql, args.reps)
            print(f"{name:22s} {shape}: median {out[name][shape][0]:8.1f} ms  [{out[name][shape][1]:.1f}-{out[name][shape][2]:.1f}]  "
                  f"read {out[name][shape][3]:>9} rows {out[name][shape][4] / 1e6:8.1f} MB", flush=True)
    # correctness cross-checks: the same answers from the three shapes
    checks = {}
    for name, col in (("Q3 sessions", 4), ("Q4 cost by model/day", 2)):
        per = queries(trace_id, word, basis)[name]
        checks[name] = {}
        for sh, sql in per.items():
            lines = [l.split("\t") for l in qs(sql + " FORMAT TSV").splitlines()]
            checks[name][sh] = [len(lines), round(sum(float(l[col]) for l in lines), 4)]
    out["_checks"] = checks
    out["_trace_id"], out["_word"] = trace_id, word
    json.dump(out, open(os.path.join(HERE, "results/queries.json"), "w"), indent=1)
    print(json.dumps(checks, indent=1))


# ---------------------------------------------------------------- the basis: late scores and price changes

def basis_demo(args):
    """What an answer about day 1 says, asked at different custody times, in A and in C."""
    C = "lfz_c"
    d1 = ("'2026-09-20 00:00:00'", "'2026-09-21 00:00:00'")
    res = {}
    for label, basis in (("end of day 1 + 1 h", "'2026-09-21 01:00:00'"), ("day 3", "'2026-09-23 00:00:00'"), ("end", "'2026-09-26 00:00:00'")):
        b = f"toDateTime64({basis}, 9, 'UTC')"
        res[f"C avg helpfulness, day-1 traces, at {label}"] = qs(
            f"""SELECT count(), round(avg(s.value), 4) FROM {resolved_scores(C, b, "AND name = 'helpfulness'")} AS s
                WHERE s.trace_id IN (SELECT trace_id FROM {C}.llm_spans WHERE namespace = '{NS}' AND parent_span_id = '' AND start_time >= {d1[0]}
                                AND start_time < {d1[1]} AND received_at < {b})""")
    res["A avg helpfulness, day-1 traces (FINAL: latest only; no earlier answer is reproducible)"] = qs(
        f"""SELECT count(), round(avg(value), 4) FROM lfz_a.scores FINAL WHERE project_id = '{NS}' AND name = 'helpfulness'
            AND trace_id IN (SELECT trace_id FROM lfz_a.events_core WHERE project_id = '{NS}' AND is_app_root AND start_time >= {d1[0]} AND start_time < {d1[1]})""")
    # price: day 2, 00:00-06:00 (the new price was valid from 00:00 but recorded at 06:00)
    d2 = ("'2026-09-21 00:00:00'", "'2026-09-21 06:00:00'")
    for label, basis in (("before the price was recorded (05:00)", "'2026-09-21 05:00:00'"), ("after (end)", "'2026-09-26 00:00:00'")):
        b = f"toDateTime64({basis}, 9, 'UTC')"
        res[f"C cost, day 2 00-06h, gpt-5.1-mini + haiku, at {label}"] = qs(
            f"""SELECT count(), round(sum(toFloat64({cost_expr(C, b)})), 4) FROM {C}.llm_spans WHERE namespace = '{NS}' AND type = 'GENERATION'
                AND model IN ('gpt-5.1-mini', 'claude-haiku-4-5') AND start_time >= {d2[0]} AND start_time < {d2[1]} AND received_at < {b}""")
    res["A stored cost, same spans (computed at ingest from the table as it then was)"] = qs(
        f"""SELECT count(), round(sum(toFloat64(calculated_total_cost)), 4) FROM lfz_a.events_core FINAL WHERE project_id = '{NS}' AND type = 'GENERATION'
            AND provided_model_name IN ('gpt-5.1-mini', 'claude-haiku-4-5') AND start_time >= {d2[0]} AND start_time < {d2[1]}""")
    json.dump(res, open(os.path.join(HERE, "results/basis.json"), "w"), indent=1)
    for k, v in res.items():
        print(f"{k}: {v}")


def drop(args):
    for db in DBS:
        q(f"DROP DATABASE IF EXISTS {db} SYNC")
    print("dropped", DBS)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", choices=["setup", "load", "sizes", "queries", "basis", "drop", "all"])
    ap.add_argument("--langfuse", default=os.environ.get("LANGFUSE_DIR", ""))
    ap.add_argument("--sessions", type=int, default=12500)
    ap.add_argument("--reps", type=int, default=5)
    a = ap.parse_args()
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    phases = ["setup", "load", "sizes", "queries", "basis"] if a.phase == "all" else [a.phase]
    for p in phases:
        globals()[{"queries": "bench_queries", "basis": "basis_demo"}.get(p, p)](a)
