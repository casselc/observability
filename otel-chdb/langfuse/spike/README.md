# langfuse/spike: three central shapes for Langfuse-shaped LLM traces

Evidence for [research/langfuse.md](../../research/langfuse.md) §9 and
[D36](../../DECISIONS.md#d36-langfuse-shaped-llm-traces-one-store-content-by-reference-facts-resolved-at-a-basis-proposed).
Everything runs inside one ClickHouse; nothing goes through the edges or the consumer (the
consumer's statements are imitated: one `INSERT … SELECT` per 10,000-span "object batch", in
custody order).

| Shape | Tables | Content | Versions |
|---|---|---|---|
| **A** Langfuse v4 | `events_full`, `events_core` + MV, `scores`: **rendered from Langfuse's own migrations** (`render_langfuse`: the 50 canonical migrations, unclustered, only statements on these tables) | inline `ZSTD(3)`, 200-char copy in `events_core` | `ReplacingMergeTree(event_ts, is_deleted)`; reads `LIMIT 1 BY` / `FINAL` as Langfuse's repositories do; cost stored at ingest from the price table as it then was |
| **B** our tables, content inline | `otel_traces`, `otel_logs` from `otap-rs/sql` (ClickStack 2.39.1 option 2, with the key/value rollups) | GenAI semconv attributes in `SpanAttributes` | scores as `gen_ai.evaluation.result` log records, resolved at the basis; cost at query time |
| **C** the proposal | B's tables with content replaced by `h:<hash>` references, `llm_payloads`, `llm_spans` / `llm_scores` via materialized views (`sql/c_llm.sql`) | one payload per distinct message per tenant and day | as B |

The traffic (`sql/src.sql`): agent conversations, 20 apps in 4 namespaces and 3 clusters, 1–8
turns each, an agent root span, a chat generation and in half the turns a tool call and a second
generation; each generation's input is the system prompt, the conversation so far and the new
message (2–20 KB, growing with the turn), as agent frameworks resend it. Words drawn from a
4,000-word synthetic vocabulary with a skewed distribution. 1% of spans arrive 20–120 min late
(`late_part`). Scores: a judge score on 70% of traces, human scores on 5% (1–48 h later),
2% of judge scores corrected, 0.5% deleted. Prices: one 20% cut on day 2 at 00:00, recorded at 06:00.

```
CHC=<clickhouse binary> LANGFUSE_DIR=<langfuse checkout> python3 lfz_bench.py all --sessions N
python3 lfz_bench.py drop
```

Insert CPU is the server's `OSCPUVirtualTimeMicroseconds` of each `INSERT … SELECT` minus the same
`SELECT … FORMAT Null` (the generation of the rows), from `clickhouse client --print-profile-events`
as `hyperdx/scripts/schema_bench.py` does. Bytes are `system.parts` after `OPTIMIZE FINAL`.
Query latency: median of 5 over HTTP after one warm-up, query cache off.

## Results

See [research/langfuse.md §9](../../research/langfuse.md#9-measurements); raw numbers in `results/`.
