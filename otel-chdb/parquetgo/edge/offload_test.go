package edge

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The payload part at the Go edge (../offload.go, DECISIONS.md D36): what
// an object carries is decided per slot from the lane's cache, marked only
// once it has committed, like resource announcements; references always
// resolve within the lane epoch; the request cap refuses as permanent.

type payRow struct {
	Attrs    map[string]string `parquet:"SpanAttributes"`
	Refs     []string          `parquet:"payload_refs,list"`
	Payloads map[string]string `parquet:"payloads"`
}

type payLogRow struct {
	Body     string            `parquet:"Body"`
	Attrs    map[string]string `parquet:"LogAttributes"`
	Refs     []string          `parquet:"payload_refs,list"`
	Payloads map[string]string `parquet:"payloads"`
}

func offloadEdge(t *testing.T, st commit.Store) *Edge {
	e := newEdge(t, st, "")
	o := parquetgo.DefaultOffloadOptions()
	o.MaxRequestBytes = 1 << 20
	o.MaxValue = 64 << 10
	e.cfg.Offload = o
	e.policy = parquetgo.NewOffloadPolicy(o)
	return e
}

const conv = `[{"role":"system","content":"You are terse."},{"role":"user","content":"%s"}]`

func genaiTraces(ns, question string, seed int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "agent")
	rs.Resource().Attributes().PutStr("k8s.namespace.name", ns)
	s := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetName(fmt.Sprintf("chat-%d", seed))
	s.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + int64(seed)))
	s.Attributes().PutStr("gen_ai.request.model", "m")
	s.Attributes().PutStr("gen_ai.input.messages", fmt.Sprintf(conv, question))
	return td
}

func payRows(t *testing.T, st *commit.MemStore, key string) ([]payRow, map[string]string) {
	t.Helper()
	o, ok := st.Get(key)
	if !ok {
		t.Fatalf("%s missing", key)
	}
	rows, err := parquet.Read[payRow](bytes.NewReader(o.Body), int64(len(o.Body)))
	if err != nil {
		t.Fatal(err)
	}
	return rows, o.Meta
}

func TestPayloadPartFollowsTheLaneCache(t *testing.T) {
	st := commit.NewMemStore()
	e := offloadEdge(t, st)
	ctx := context.Background()
	if err := e.PushTraces(ctx, genaiTraces("team-a", "q1", 1)); err != nil {
		t.Fatal(err)
	}
	keys := st.Keys("root/c1/p1/traces/")
	rows, meta := payRows(t, st, keys[0])
	if meta[commit.MetaPayloads] != "2" || meta[commit.MetaPayloadRefs] != "2" || len(rows[0].Refs) != 2 || len(rows[0].Payloads) != 2 {
		t.Fatalf("first object: %v %+v", meta, rows)
	}
	var doc []string
	if err := json.Unmarshal([]byte(rows[0].Attrs["gen_ai.input.messages"]), &doc); err != nil || len(doc) != 2 {
		t.Fatalf("reference document %q", rows[0].Attrs["gen_ai.input.messages"])
	}
	for i, r := range doc {
		if r != "h:"+rows[0].Refs[i] {
			t.Fatalf("doc %v refs %v", doc, rows[0].Refs)
		}
	}
	if rows[0].Payloads[rows[0].Refs[1]] != `{"role":"user","content":"q1"}` {
		t.Fatalf("payloads %v", rows[0].Payloads)
	}
	if rows[0].Attrs["otel.payload.gen_ai.input.messages.elements"] != "2" || rows[0].Attrs["gen_ai.request.model"] != "m" {
		t.Fatalf("markers %v", rows[0].Attrs)
	}
	// the same system prompt again: referenced, not carried
	if err := e.PushTraces(ctx, genaiTraces("team-a", "q2", 2)); err != nil {
		t.Fatal(err)
	}
	keys = st.Keys("root/c1/p1/traces/")
	rows2, meta2 := payRows(t, st, keys[1])
	if meta2[commit.MetaPayloads] != "1" || meta2[commit.MetaPayloadRefs] != "2" || rows2[0].Refs[0] != rows[0].Refs[0] {
		t.Fatalf("second object: %v %+v", meta2, rows2)
	}
	if _, ok := rows2[0].Payloads[rows[0].Refs[0]]; ok {
		t.Fatal("the system prompt carried twice in one epoch")
	}
	// another tenant: other hashes, carried
	if err := e.PushTraces(ctx, genaiTraces("team-b", "q1", 3)); err != nil {
		t.Fatal(err)
	}
	keys = st.Keys("root/c1/p1/traces/")
	rows3, meta3 := payRows(t, st, keys[2])
	if meta3[commit.MetaPayloads] != "2" || rows3[0].Refs[0] == rows[0].Refs[0] {
		t.Fatalf("another namespace: %v %+v", meta3, rows3)
	}
	// A lost object's payloads are not marked: carried again.
	st.Inject(commit.Drop)
	st.Inject(commit.HeadFail)
	st.Inject(commit.HeadFail)
	st.Inject(commit.HeadFail)
	_ = e.PushTraces(ctx, genaiTraces("team-a", "q-lost", 4))
	st.ClearFaults()
	if err := e.PushTraces(ctx, genaiTraces("team-a", "q-lost", 5)); err != nil {
		t.Fatal(err)
	}
	keys = st.Keys("root/c1/p1/traces/")
	last, _ := payRows(t, st, keys[len(keys)-1])
	if len(last[0].Payloads) == 0 {
		t.Fatalf("after a lost object: %+v", last)
	}
	// Every reference of the epoch resolves in an object of the epoch.
	carried := map[string]bool{}
	var refs []string
	for _, k := range keys {
		rs, _ := payRows(t, st, k)
		for _, r := range rs {
			for h := range r.Payloads {
				carried[h] = true
			}
			refs = append(refs, r.Refs...)
		}
	}
	for _, h := range refs {
		if !carried[h] {
			t.Fatalf("dangling reference %s", h)
		}
	}
	s := e.OffloadStats()
	if s.Offloaded != 4 || s.Split != 4 || s.Carried != 6 || s.Dedup != 2 {
		t.Fatalf("stats %+v", s)
	}
}

func TestPayloadLogBodyAndCap(t *testing.T) {
	st := commit.NewMemStore()
	e := offloadEdge(t, st)
	ctx := context.Background()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "team-a")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(1_700_000_000_000_000_000)
	big := strings.Repeat("é", 2000) // 4000 bytes
	lr.Body().SetStr(big)
	lr.Attributes().PutStr("k", "v")
	if err := e.PushLogs(ctx, ld); err != nil {
		t.Fatal(err)
	}
	keys := st.Keys("root/c1/p1/logs/")
	o, _ := st.Get(keys[0])
	rows, err := parquet.Read[payLogRow](bytes.NewReader(o.Body), int64(len(o.Body)))
	if err != nil {
		t.Fatal(err)
	}
	r := rows[0]
	if len(r.Refs) != 1 || r.Body != `["h:`+r.Refs[0]+`"]` || r.Payloads[r.Refs[0]] != big {
		t.Fatalf("body %q refs %v", r.Body, r.Refs)
	}
	if r.Attrs["otel.payload.@body.bytes"] != "4000" || r.Attrs["k"] != "v" {
		t.Fatalf("attrs %v", r.Attrs)
	}
	h, _ := hex.DecodeString(r.Refs[0])
	k := parquetgo.TenantKey("c1", []byte("team-a"), 1_790_000_000_123_456_789)
	if p := parquetgo.PayloadHash(&k, []byte(big)); !bytes.Equal(p[:], h) {
		t.Fatal("the hash is not the tenant's")
	}
	// the request cap: permanent, counted
	huge := plog.NewLogs()
	huge.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("x", 2<<20))
	if err := e.PushLogs(ctx, huge); !IsPermanent(err) {
		t.Fatalf("over the cap: %v", err)
	}
	if e.OffloadStats().Refused != 1 || len(st.Keys("root/c1/p1/logs/")) != 1 {
		t.Fatal("refused request published or not counted")
	}
}
