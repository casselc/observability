package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The GenAI corpus (DECISIONS.md D36): LLM spans as the producers we expect
// send them, for the payload offloader at both edges (FORMAT.md §2.3), the
// consumer's llm_payloads / llm_spans / llm_scores, and the differential
// against Langfuse's own mapper (../../langfuse/differential):
//
//   - OTel GenAI semconv (e57c543) chat, tool and agent spans: a
//     conversation resent call after call (gen_ai.input.messages grows, its
//     earlier messages shared), system instructions, tool definitions,
//     arguments and results, usage;
//   - OpenInference (input.value / output.value, openinference.span.kind);
//   - the Langfuse SDKs (v3+ Python, v5 JS): langfuse.observation.*,
//     langfuse.trace.*, user.id / session.id, usage_details and model
//     parameters as JSON, tags;
//   - what the Langfuse opencode, Codex and Claude Code integrations send
//     (research/langfuse.md §6.1 addendum): Langfuse SDK spans named after
//     the tool (session spans, generations, tool calls), with media upload
//     off, so a base64 data: URI reaches the edge in the input;
//   - hostile shapes: a message list that is not JSON, one nested past the
//     split's depth bound, invalid UTF-8 inside a message, a value that
//     looks like a reference document, a producer key in the marker space,
//     a large unlisted attribute and a large event attribute;
//   - evaluation results as gen_ai.evaluation.result log records (scores as
//     facts: numeric, categorical, a correction and a retraction).
//
// Resources carry k8s.namespace.name (the tenant of the payload hash):
// two tenants share a conversation, so equal content has two hashes.

func js(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

type msg map[string]any

func genaiResource(r pcommon.Resource, ns string, svc string) {
	a := r.Attributes()
	a.PutStr("service.name", svc)
	if ns != "" {
		a.PutStr("k8s.namespace.name", ns)
		a.PutStr("k8s.cluster.name", "conf")
		a.PutStr("k8s.pod.name", svc+"-0")
	}
	a.PutStr("deployment.environment.name", "staging")
	a.PutStr("telemetry.sdk.language", "python")
}

func genaiSpan(ss ptrace.ScopeSpans, trace byte, i int, parent int, name string) ptrace.Span {
	sp := ss.Spans().AppendEmpty()
	sp.SetName(name)
	sp.SetTraceID(pcommon.TraceID{0x6a, trace, 1})
	sp.SetSpanID(pcommon.SpanID{0x6a, trace, byte(i)})
	if parent >= 0 {
		sp.SetParentSpanID(pcommon.SpanID{0x6a, trace, byte(parent)})
	}
	sp.SetKind(ptrace.SpanKindClient)
	sp.SetStartTimestamp(pcommon.Timestamp(base + int64(trace)*1e9 + int64(i)*1e7))
	sp.SetEndTimestamp(pcommon.Timestamp(base + int64(trace)*1e9 + int64(i)*1e7 + 5e6))
	return sp
}

func genaiTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	system := "You are a terse assistant. " + strings.Repeat("Answer in one line. ", 200) // ~4 KiB, shared by every call
	tools := []msg{{"type": "function", "name": "get_weather", "description": strings.Repeat("Weather lookup. ", 100),
		"parameters": msg{"type": "object", "properties": msg{"city": msg{"type": "string"}}}}}
	for t, ns := range []string{"team-a", "team-b", ""} {
		rs := td.ResourceSpans().AppendEmpty()
		genaiResource(rs.Resource(), ns, "agent")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("opentelemetry.instrumentation.genai")
		trace := byte(t + 1)
		root := genaiSpan(ss, trace, 0, -1, "invoke_agent weather-bot")
		root.Attributes().PutStr("gen_ai.operation.name", "invoke_agent")
		root.Attributes().PutStr("gen_ai.agent.name", "weather-bot")
		root.Attributes().PutStr("gen_ai.conversation.id", fmt.Sprintf("conv-%d", t))
		// A conversation resent call after call: every call carries the
		// whole history (the edge splits it per message and each message is
		// stored once per tenant and day).
		var history []msg
		for c := 0; c < 5; c++ {
			history = append(history, msg{"role": "user", "parts": []msg{{"type": "text", "content": fmt.Sprintf("question %d: %s", c, strings.Repeat("why? ", 50*c))}}})
			sp := genaiSpan(ss, trace, 1+2*c, 0, "chat gpt-5")
			a := sp.Attributes()
			a.PutStr("gen_ai.operation.name", "chat")
			a.PutStr("gen_ai.provider.name", "openai")
			a.PutStr("gen_ai.request.model", "gpt-5")
			a.PutStr("gen_ai.response.model", "gpt-5-2026-08-01")
			a.PutInt("gen_ai.usage.input_tokens", int64(120+40*c))
			a.PutInt("gen_ai.usage.output_tokens", int64(30+c))
			a.PutInt("gen_ai.usage.cache_read.input_tokens", int64(100*c))
			a.PutStr("gen_ai.system_instructions", js([]msg{{"type": "text", "content": system}}))
			a.PutStr("gen_ai.input.messages", js(history))
			answer := msg{"role": "assistant", "parts": []msg{{"type": "text", "content": fmt.Sprintf("answer %d", c)}}, "finish_reason": "stop"}
			a.PutStr("gen_ai.output.messages", js([]msg{answer}))
			a.PutStr("gen_ai.tool.definitions", js(tools))
			a.PutStr("gen_ai.conversation.id", fmt.Sprintf("conv-%d", t))
			history = append(history, answer)
			// The tool call the model asked for.
			tool := genaiSpan(ss, trace, 2+2*c, 1+2*c, "execute_tool get_weather")
			ta := tool.Attributes()
			ta.PutStr("gen_ai.operation.name", "execute_tool")
			ta.PutStr("gen_ai.tool.name", "get_weather")
			ta.PutStr("gen_ai.tool.call.id", fmt.Sprintf("call-%d-%d", t, c))
			ta.PutStr("gen_ai.tool.call.arguments", js(msg{"city": "Zürich"}))
			ta.PutStr("gen_ai.tool.call.result", js(msg{"temp_c": 21 + c, "report": strings.Repeat("sunny ", 20*c)}))
		}
		// The older per-message events and a large event attribute.
		ev := root.Events().AppendEmpty()
		ev.SetName("gen_ai.client.inference.operation.details")
		ev.SetTimestamp(pcommon.Timestamp(base + int64(trace)*1e9 + 1))
		ev.Attributes().PutStr("gen_ai.input.messages", js(history[:2]))
		ev.Attributes().PutStr("blob", strings.Repeat("e", 3000))

		// OpenInference.
		oi := genaiSpan(ss, trace, 20, 0, "ChatCompletion")
		oa := oi.Attributes()
		oa.PutStr("openinference.span.kind", "LLM")
		oa.PutStr("llm.model_name", "claude-sonnet-5")
		oa.PutStr("input.value", js(msg{"messages": history[:1]}))
		oa.PutStr("input.mime_type", "application/json")
		oa.PutStr("output.value", "plain text answer")
		oa.PutInt("llm.token_count.prompt", 77)
		oa.PutInt("llm.token_count.completion", 11)

		// The Langfuse SDKs, as the opencode / Codex / Claude Code
		// integrations send them (media upload off: a data: URI inline).
		lf := genaiSpan(ss, trace, 21, 0, "claude-code session")
		la := lf.Attributes()
		la.PutStr("langfuse.observation.type", "span")
		la.PutStr("langfuse.trace.name", "claude-code")
		la.PutStr("session.id", fmt.Sprintf("sess-%d", t))
		la.PutStr("user.id", "dev@example.com")
		la.PutStr("langfuse.trace.tags", js([]string{"claude-code", "cli"}))
		la.PutStr("langfuse.environment", "development")
		la.PutStr("langfuse.trace.input", js(msg{"prompt": "fix the flaky test"}))
		gen := genaiSpan(ss, trace, 22, 21, "Claude Code generation")
		ga := gen.Attributes()
		ga.PutStr("langfuse.observation.type", "generation")
		ga.PutStr("langfuse.observation.model.name", "claude-opus-5")
		ga.PutStr("langfuse.observation.model.parameters", js(msg{"temperature": 0, "max_tokens": 4096}))
		ga.PutStr("langfuse.observation.usage_details", js(msg{"input": 1500, "output": 250, "cache_read_input_tokens": 1200}))
		img := "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte(strings.Repeat("\x89PNG-bytes", 400)))
		ga.PutStr("langfuse.observation.input", js([]msg{{"role": "user", "content": []msg{{"type": "text", "text": "what is in this screenshot?"}, {"type": "image_url", "image_url": msg{"url": img}}}}}))
		ga.PutStr("langfuse.observation.output", js([]msg{{"role": "assistant", "content": "a failing test"}}))
		ga.PutStr("langfuse.observation.prompt.name", "fixer")
		ga.PutInt("langfuse.observation.prompt.version", 3)
		ga.PutStr("langfuse.observation.metadata.hook", "Stop")
		tc := genaiSpan(ss, trace, 23, 21, "Bash")
		tca := tc.Attributes()
		tca.PutStr("langfuse.observation.type", "tool")
		tca.PutStr("langfuse.observation.input", js(msg{"command": "go test ./..."}))
		tca.PutStr("langfuse.observation.output", strings.Repeat("ok  \tpkg\t0.1s\n", 300))
		opencode := genaiSpan(ss, trace, 24, 21, "opencode.generation")
		opa := opencode.Attributes()
		opa.PutStr("langfuse.observation.type", "generation")
		opa.PutStr("gen_ai.request.model", "anthropic/claude-sonnet-5")
		opa.PutStr("langfuse.observation.input", js([]msg{{"role": "user", "content": "refactor"}}))
		opa.PutStr("langfuse.observation.level", "WARNING")
		opa.PutStr("langfuse.observation.status_message", "rate limited once")

		// Hostile shapes.
		h := genaiSpan(ss, trace, 30, 0, "chat hostile")
		ha := h.Attributes()
		ha.PutStr("gen_ai.operation.name", "chat")
		ha.PutStr("gen_ai.input.messages", "not json [")
		ha.PutStr("gen_ai.output.messages", strings.Repeat("[", 100)+strings.Repeat("]", 100)) // past the split's depth bound: one payload
		ha.PutStr("gen_ai.system_instructions", "[\"\xff\xfe\", \"ok\"]")                      // invalid UTF-8 inside a message
		ha.PutStr("spoof", "[\"h:0123456789abcdef0123456789abcdef\"]")                         // a reference-looking value is data
		ha.PutStr("otel.payload.fake.bytes", "7")                                              // a producer key in the marker space
		ha.PutStr("big.unlisted", strings.Repeat("x", 5000))
		ha.PutInt("gen_ai.usage.input_tokens", 1)
		ha.PutEmptyBytes("gen_ai.prompt").FromRaw([]byte(strings.Repeat("\x00b", 1500))) // bytes, rendered then offloaded
	}
	return td
}

func genaiLogs() plog.Logs {
	ld := plog.NewLogs()
	for t, ns := range []string{"team-a", "team-b"} {
		rl := ld.ResourceLogs().AppendEmpty()
		genaiResource(rl.Resource(), ns, "evaluator")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("langfuse-evals")
		trace := byte(t + 1)
		for i, sc := range []struct {
			name, id, kind, label, supersedes string
			value                             float64
		}{
			{"helpfulness", "s1", "", "", "", 0.8},
			{"helpfulness", "s1b", "", "", "s1", 0.9}, // a correction
			{"toxicity", "s2", "", "none", "", 0},
			{"toxicity", "s2r", "retract", "", "s2", 0}, // a retraction
			{"groundedness", "s3", "", "", "", 0.42},
		} {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetEventName("gen_ai.evaluation.result")
			lr.SetTimestamp(pcommon.Timestamp(base + int64(trace)*1e9 + int64(i)*1e6))
			lr.SetTraceID(pcommon.TraceID{0x6a, trace, 1})
			lr.SetSpanID(pcommon.SpanID{0x6a, trace, 1})
			a := lr.Attributes()
			a.PutStr("gen_ai.evaluation.name", sc.name)
			if sc.label != "" {
				a.PutStr("gen_ai.evaluation.score.label", sc.label)
			} else {
				a.PutDouble("gen_ai.evaluation.score.value", sc.value)
			}
			a.PutStr("gen_ai.evaluation.explanation", strings.Repeat("because ", 10*i))
			a.PutStr("langfuse.score.id", fmt.Sprintf("%s-%d", sc.id, t))
			if sc.kind != "" {
				a.PutStr("langfuse.score.kind", sc.kind)
			}
			if sc.supersedes != "" {
				a.PutStr("langfuse.score.supersedes", fmt.Sprintf("%s-%d", sc.supersedes, t))
			}
			a.PutStr("langfuse.score.source", "EVAL")
		}
		// An evaluator's judgement text as the body, over the threshold.
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(base + int64(trace)*1e9 + 99))
		lr.Body().SetStr(strings.Repeat("judgement ", 400))
	}
	return ld
}
