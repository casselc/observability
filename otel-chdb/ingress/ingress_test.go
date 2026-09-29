package ingress

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/casselc/observability/otel-chdb/ingress/entratest"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
)

const (
	tenantA   = "11111111-1111-4111-8111-111111111111" // our organisation
	tenantB   = "22222222-2222-4222-8222-222222222222" // another organisation (not accepted)
	apiID     = "33333333-3333-4333-8333-333333333333" // the ingress API app registration
	forwarder = "44444444-4444-4444-8444-444444444444" // the device forwarder (public client)
	ciApp     = "55555555-5555-4555-8555-555555555555" // a CI workload app (federated credential)
	alice     = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	bob       = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	carol     = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	groupOps  = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	graphAPI  = "00000003-0000-0000-c000-000000000000"
)

type rig struct {
	is    *entratest.Issuer
	store *commit.MemStore
	edge  *edge.Edge
	srv   *Server
	http  *httptest.Server
	logs  []string
}

func newRig(t testing.TB, producer string, is *entratest.Issuer, st *commit.MemStore, lim *Limits) *rig {
	t.Helper()
	if is == nil {
		is = entratest.New()
		t.Cleanup(is.Close)
	}
	if st == nil {
		st = commit.NewMemStore()
	}
	var n atomic.Int64
	e, err := edge.New(edge.Config{Store: st, Prefix: "root", Cluster: "devtools", ProducerID: producer,
		PutTimeout: time.Second, HeadTimeout: 200 * time.Millisecond,
		NewEpoch: func() string { return fmt.Sprintf("20260929T000000.000Z-%08x", n.Add(1)) }})
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewEntraVerifier(EntraConfig{Authority: is.URL, Tenants: []string{tenantA}, Audiences: []string{apiID, "api://otel-ingress"},
		ClientApps: []string{forwarder, ciApp}, UserScope: "Telemetry.Write", AppRole: "Telemetry.Write.App"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pol := &Policy{Cluster: "devtools", NamespacePrefix: "dev-", Rules: []Rule{
		{Tenant: tenantA, Role: "Team.Payments", Namespace: "dev-payments"},
		{Tenant: tenantA, Role: "Team.Search", Namespace: "dev-search"},
		{Tenant: tenantA, Group: groupOps, Namespace: "dev-ops"},
		{Tenant: tenantA, Role: "Telemetry.CI.Payments", Namespace: "dev-payments"},
	}}
	l := DefaultLimits()
	if lim != nil {
		l = *lim
	}
	srv, err := NewServer(v, pol, l, e, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{is: is, store: st, edge: e, srv: srv}
	srv.Logf = func(f string, a ...any) { r.logs = append(r.logs, fmt.Sprintf(f, a...)) }
	r.http = httptest.NewServer(srv.Handler())
	t.Cleanup(r.http.Close)
	return r
}

func userToken(is *entratest.Issuer, oid string, roles ...string) string {
	return is.Mint(entratest.Token{Tenant: tenantA, OID: oid, Audience: apiID, ClientApp: forwarder,
		Scope: "Telemetry.Write", Roles: roles})
}

// agentTraces is what a Langfuse integration sends: its own claims about
// who and where, which the ingress must not believe.
func agentTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ra := rs.Resource().Attributes()
	ra.PutStr("service.name", "claude-code")
	ra.PutStr("k8s.namespace.name", "prod-billing") // a claim into another team's tenant
	ra.PutStr("k8s.cluster.name", "prod-eu-1")
	ra.PutStr("k8s.pod.name", "billing-7f9c")
	ra.PutStr("user.id", "mallory@example.com")
	ra.PutStr("oscope.ingress.auth", "forged")
	s := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetName("generation")
	s.SetTraceID(pcommon.TraceID{1, 2, 3})
	s.SetSpanID(pcommon.SpanID{4, 5, 6})
	now := time.Now()
	s.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Second)))
	s.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	s.Attributes().PutStr("langfuse.user.id", "someone-else")
	s.Attributes().PutStr("gen_ai.request.model", "claude")
	return td
}

func protoBody(t *testing.T, td ptrace.Traces) []byte {
	b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (r *rig) post(t *testing.T, path, auth string, body []byte, hdr map[string]string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, r.http.URL+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

func expect(t *testing.T, resp *http.Response, code int, what string) {
	t.Helper()
	if resp.StatusCode != code {
		t.Fatalf("%s: HTTP %d, want %d", what, resp.StatusCode, code)
	}
}

func (r *rig) objects(t *testing.T, signal string) []commit.MemObject {
	t.Helper()
	var out []commit.MemObject
	for _, k := range r.store.Keys("root/devtools/") {
		if !strings.Contains(k, "/"+signal+"/") {
			continue
		}
		o, _ := r.store.Get(k)
		if o.Meta[commit.MetaKind] == "" || o.Meta[commit.MetaKind] == "data" {
			if len(o.Body) > 0 {
				out = append(out, o)
			}
		}
	}
	return out
}

type traceRow struct {
	Res  map[string]string `parquet:"ResourceAttributes"`
	Span map[string]string `parquet:"SpanAttributes"`
}

func rows(t *testing.T, o commit.MemObject) []traceRow {
	t.Helper()
	rs, err := parquet.Read[traceRow](bytes.NewReader(o.Body), int64(len(o.Body)))
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

// The whole path: a valid token, a producer that claims another tenant,
// another cluster, a pod and another user; the committed object carries
// only what the ingress asserts, the claims kept as labels.
func TestAcceptedRequestIsStampedAndCommitted(t *testing.T) {
	r := newRig(t, "ingress-0", nil, nil, nil)
	resp := r.post(t, "/api/public/otel/v1/traces", "Bearer "+userToken(r.is, alice, "Team.Payments"), protoBody(t, agentTraces()), nil)
	expect(t, resp, 200, "valid token, one team")
	objs := r.objects(t, "traces")
	if len(objs) != 1 {
		t.Fatalf("%d objects", len(objs))
	}
	rs := rows(t, objs[0])
	if len(rs) != 1 {
		t.Fatalf("%d rows", len(rs))
	}
	res, span := rs[0].Res, rs[0].Span
	want := map[string]string{
		"k8s.cluster.name": "devtools", "k8s.namespace.name": "dev-payments", "user.id": alice,
		"oscope.ingress.auth": "entra", "oscope.ingress.entra_tenant": tenantA, "oscope.ingress.principal_type": "user",
		"oscope.ingress.client_app": forwarder, "service.name": "claude-code",
		"oscope.ingress.claimed.k8s.namespace.name": "prod-billing", "oscope.ingress.claimed.k8s.cluster.name": "prod-eu-1",
		"oscope.ingress.claimed.k8s.pod.name": "billing-7f9c", "oscope.ingress.claimed.user.id": "mallory@example.com",
	}
	for k, v := range want {
		if res[k] != v {
			t.Errorf("resource %s = %q, want %q", k, res[k], v)
		}
	}
	if _, ok := res["k8s.pod.name"]; ok {
		t.Error("a claimed pod survived")
	}
	if len(res) != len(want) {
		t.Errorf("resource has extra keys: %v", res)
	}
	if span["langfuse.user.id"] != "" || span["oscope.ingress.claimed.langfuse.user.id"] != "someone-else" || span["gen_ai.request.model"] != "claude" {
		t.Errorf("span attributes %v", span)
	}
	if objs[0].Meta[commit.MetaCluster] != "" && objs[0].Meta[commit.MetaCluster] != "devtools" {
		t.Errorf("meta cluster %q", objs[0].Meta[commit.MetaCluster])
	}
	if c := r.srv.Counts(); c["ok"] != 1 {
		t.Errorf("counts %v", c)
	}
}

// Every token the ingress must refuse, and nothing committed for any.
func TestTokensRefused(t *testing.T) {
	r := newRig(t, "ingress-0", nil, nil, nil)
	is := r.is
	forger, _ := rsa.GenerateKey(rand.Reader, 2048)
	valid := entratest.Token{Tenant: tenantA, OID: alice, Audience: apiID, ClientApp: forwarder, Scope: "Telemetry.Write", Roles: []string{"Team.Payments"}}
	with := func(f func(*entratest.Token)) string { t2 := valid; f(&t2); return is.Mint(t2) }
	is.AddKey("bound-b", tenantB)
	goodClaims := func() jwt.MapClaims {
		return jwt.MapClaims{"iss": is.URL + "/" + tenantA + "/v2.0", "tid": tenantA, "oid": alice, "aud": apiID, "azp": forwarder,
			"ver": "2.0", "scp": "Telemetry.Write", "roles": []any{"Team.Payments"}, "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix()}
	}
	pubPEMish := forger.PublicKey.N.Bytes() // HS256 "keyed" with public material: alg confusion
	cases := []struct{ name, auth string }{
		{"no token", ""},
		{"langfuse basic auth", "Basic cGstbGYtMTIzOnNrLWxmLTQ1Ng=="},
		{"garbage", "Bearer not.a.jwt"},
		{"forged signature, real kid", "Bearer " + entratest.Sign(forger, "k1", jwt.SigningMethodRS256, goodClaims())},
		{"unknown kid", "Bearer " + entratest.Sign(forger, "k9", jwt.SigningMethodRS256, goodClaims())},
		{"alg none", "Bearer " + func() string {
			s, _ := jwt.NewWithClaims(jwt.SigningMethodNone, goodClaims()).SignedString(jwt.UnsafeAllowNoneSignatureType)
			return s
		}()},
		{"HS256 with public material", "Bearer " + entratest.Sign(pubPEMish, "k1", jwt.SigningMethodHS256, goodClaims())},
		{"wrong audience (a Graph token)", "Bearer " + with(func(t *entratest.Token) { t.Audience = graphAPI })},
		{"tenant not accepted", "Bearer " + with(func(t *entratest.Token) { t.Tenant = tenantB })},
		{"issuer and tid disagree", "Bearer " + with(func(t *entratest.Token) { t.Extra = jwt.MapClaims{"iss": is.URL + "/" + tenantB + "/v2.0"} })},
		{"tid not a GUID", "Bearer " + with(func(t *entratest.Token) { t.Tenant = "common" })},
		{"key bound to another tenant", "Bearer " + with(func(t *entratest.Token) { t.Kid = "bound-b" })},
		{"expired", "Bearer " + with(func(t *entratest.Token) {
			t.Extra = jwt.MapClaims{"exp": time.Now().Add(-5 * time.Minute).Unix(), "iat": time.Now().Add(-80 * time.Minute).Unix(), "nbf": time.Now().Add(-80 * time.Minute).Unix()}
		})},
		{"no exp", "Bearer " + with(func(t *entratest.Token) { t.Extra = jwt.MapClaims{"exp": nil} })},
		{"not yet valid", "Bearer " + with(func(t *entratest.Token) { t.Extra = jwt.MapClaims{"nbf": time.Now().Add(10 * time.Minute).Unix()} })},
		{"v1.0 token", "Bearer " + with(func(t *entratest.Token) { t.Extra = jwt.MapClaims{"ver": "1.0"} })},
		{"another client app", "Bearer " + with(func(t *entratest.Token) { t.ClientApp = "66666666-6666-4666-8666-666666666666" })},
		{"user token without the scope", "Bearer " + with(func(t *entratest.Token) { t.Scope = "User.Read" })},
		{"app token without the app role", "Bearer " + with(func(t *entratest.Token) {
			t.Scope = ""
			t.ClientApp = ciApp
			t.Roles = []string{"Telemetry.CI.Payments"}
		})},
		{"no oid", "Bearer " + with(func(t *entratest.Token) { t.Extra = jwt.MapClaims{"oid": nil} })},
	}
	body := protoBody(t, agentTraces())
	for _, c := range cases {
		resp := r.post(t, "/v1/traces", c.auth, body, nil)
		expect(t, resp, 401, c.name)
		if !strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Bearer") {
			t.Errorf("%s: no WWW-Authenticate", c.name)
		}
	}
	if n := len(r.objects(t, "traces")); n != 0 {
		t.Fatalf("%d objects committed from refused tokens", n)
	}
	if c := r.srv.Counts(); c["unauthenticated"] != int64(len(cases)) {
		t.Fatalf("counts %v", c)
	}
}

// Authenticated but not allowed: no mapped role or group, a groups
// overage, several teams without a choice, a choice outside the grants.
func TestTenantMapping(t *testing.T) {
	r := newRig(t, "ingress-0", nil, nil, nil)
	body := protoBody(t, agentTraces())
	expect(t, r.post(t, "/v1/traces", "Bearer "+userToken(r.is, carol), body, nil), 403, "no role, no group")
	expect(t, r.post(t, "/v1/traces", "Bearer "+userToken(r.is, carol, "Some.Other.Role"), body, nil), 403, "unmapped role")
	overage := r.is.Mint(entratest.Token{Tenant: tenantA, OID: carol, Audience: apiID, ClientApp: forwarder, Scope: "Telemetry.Write",
		Extra: jwt.MapClaims{"_claim_names": map[string]any{"groups": "src1"}}})
	expect(t, r.post(t, "/v1/traces", "Bearer "+overage, body, nil), 403, "groups overage")
	both := userToken(r.is, bob, "Team.Payments", "Team.Search")
	expect(t, r.post(t, "/v1/traces", "Bearer "+both, body, nil), 403, "two teams, no choice")
	expect(t, r.post(t, "/v1/traces", "Bearer "+both, body, map[string]string{NamespaceHeader: "prod-billing"}), 403, "a choice outside the grants")
	expect(t, r.post(t, "/v1/traces", "Bearer "+both, body, map[string]string{NamespaceHeader: "dev-ops"}), 403, "another team's namespace")
	if n := len(r.objects(t, "traces")); n != 0 {
		t.Fatalf("%d objects committed", n)
	}
	expect(t, r.post(t, "/v1/traces", "Bearer "+both, body, map[string]string{NamespaceHeader: "dev-search"}), 200, "a choice among the grants")
	grp := r.is.Mint(entratest.Token{Tenant: tenantA, OID: carol, Audience: apiID, ClientApp: forwarder, Scope: "Telemetry.Write", Groups: []string{groupOps}})
	expect(t, r.post(t, "/v1/traces", "Bearer "+grp, body, nil), 200, "a mapped group")
	// A CI workload: app-only token, app role, no user.
	ci := r.is.Mint(entratest.Token{Tenant: tenantA, OID: "77777777-7777-4777-8777-777777777777", Audience: apiID, ClientApp: ciApp,
		Roles: []string{"Telemetry.Write.App", "Telemetry.CI.Payments"}})
	expect(t, r.post(t, "/v1/traces", "Bearer "+ci, body, nil), 200, "a CI app token")
	got := map[string]string{}
	for _, o := range r.objects(t, "traces") {
		res := rows(t, o)[0].Res
		got[res["k8s.namespace.name"]] = res["oscope.ingress.principal_type"] + ":" + res["user.id"]
	}
	want := map[string]string{"dev-search": "user:" + bob, "dev-ops": "user:" + carol, "dev-payments": "app:77777777-7777-4777-8777-777777777777"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("tenants %v, want %v", got, want)
	}
}

// A sender's retry (same bytes, a refreshed token, another replica) must
// produce the same content key, so the consumer skips it as a copy.
func TestRetryIsACopy(t *testing.T) {
	is := entratest.New()
	defer is.Close()
	st := commit.NewMemStore()
	r0 := newRig(t, "ingress-0", is, st, nil)
	r1 := newRig(t, "ingress-1", is, st, nil)
	body := protoBody(t, agentTraces())
	expect(t, r0.post(t, "/v1/traces", "Bearer "+userToken(is, alice, "Team.Payments"), body, nil), 200, "first")
	time.Sleep(1100 * time.Millisecond) // a new iat/exp/uti
	expect(t, r1.post(t, "/v1/traces", "Bearer "+userToken(is, alice, "Team.Payments"), body, nil), 200, "retry elsewhere")
	objs := r0.objects(t, "traces")
	if len(objs) != 2 {
		t.Fatalf("%d objects", len(objs))
	}
	if a, b := objs[0].Meta[commit.MetaContent], objs[1].Meta[commit.MetaContent]; a != b || a == "" {
		t.Fatalf("content keys differ: %s %s", a, b)
	}
	// Another user sending the same bytes is not a copy.
	expect(t, r0.post(t, "/v1/traces", "Bearer "+userToken(is, carol, "Team.Payments"), body, nil), 200, "carol")
	keys := map[string]bool{}
	for _, o := range r0.objects(t, "traces") {
		keys[o.Meta[commit.MetaContent]] = true
	}
	if len(keys) != 2 {
		t.Fatalf("%d distinct content keys, want 2", len(keys))
	}
}

// 200 means committed; an unresolved commit is 503 with Retry-After, and
// the retry of the same bytes resolves to the committed object.
func TestUnresolvedCommitIsRetryable(t *testing.T) {
	r := newRig(t, "ingress-0", nil, nil, nil)
	body := protoBody(t, agentTraces())
	tok := "Bearer " + userToken(r.is, alice, "Team.Payments")
	// Every PUT lost and every HEAD failing: the append gives up (CAST 39's bound).
	for range 64 {
		r.store.Inject(commit.Drop)
		r.store.Inject(commit.HeadFail)
	}
	resp := r.post(t, "/v1/traces", tok, body, nil)
	expect(t, resp, 503, "unresolved")
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("no Retry-After")
	}
	r2 := newRig(t, "ingress-0b", r.is, commit.NewMemStore(), nil)
	expect(t, r2.post(t, "/v1/traces", tok, body, nil), 200, "retry to a healthy replica")
}

func TestCapsAndRates(t *testing.T) {
	lim := DefaultLimits()
	lim.MaxBodyBytes, lim.MaxDecodedBytes, lim.DecodedBytesPerMinute = 4<<10, 64<<10, 64<<10
	lim.RequestsPerMinute = 3
	r := newRig(t, "ingress-0", nil, nil, &lim)
	tok := "Bearer " + userToken(r.is, alice, "Team.Payments")
	big := agentTraces()
	big.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutStr("gen_ai.input.messages", strings.Repeat("x", 8<<10))
	expect(t, r.post(t, "/v1/traces", tok, protoBody(t, big), nil), 413, "body over the cap")
	// A gzip bomb: 1 MiB of zeros compresses to about 1 KiB.
	var z bytes.Buffer
	zw := gzip.NewWriter(&z)
	_, _ = zw.Write(make([]byte, 1<<20))
	_ = zw.Close()
	expect(t, r.post(t, "/v1/traces", tok, z.Bytes(), map[string]string{"Content-Encoding": "gzip"}), 413, "gzip bomb")
	// Rate: 3 per minute for alice (two already taken above).
	expect(t, r.post(t, "/v1/traces", tok, protoBody(t, agentTraces()), nil), 200, "third")
	resp := r.post(t, "/v1/traces", tok, protoBody(t, agentTraces()), nil)
	expect(t, resp, 429, "fourth")
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("no Retry-After")
	}
	// Bob is not throttled by alice (per principal, not per team).
	expect(t, r.post(t, "/v1/traces", "Bearer "+userToken(r.is, bob, "Team.Payments"), protoBody(t, agentTraces()), nil), 200, "bob")
	// Content type: a browser's simple request (text/plain) is refused.
	req, _ := http.NewRequest(http.MethodPost, r.http.URL+"/v1/traces", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Authorization", "Bearer "+userToken(r.is, carol, "Team.Search"))
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	expect(t, resp2, 415, "text/plain")
}

func TestLogsAreStamped(t *testing.T) {
	r := newRig(t, "ingress-0", nil, nil, nil)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "dev-search")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.SetEventName("gen_ai.evaluation.result")
	lr.Attributes().PutStr("user.id", "reviewer-claimed")
	lr.Attributes().PutDouble("gen_ai.evaluation.score.value", 0.9)
	b, _ := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	req, _ := http.NewRequest(http.MethodPost, r.http.URL+"/v1/logs", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+userToken(r.is, alice, "Team.Payments"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	expect(t, resp, 200, "logs")
	objs := r.objects(t, "logs")
	if len(objs) != 1 {
		t.Fatalf("%d objects", len(objs))
	}
	lrows, err := parquet.Read[struct {
		Res  map[string]string `parquet:"ResourceAttributes"`
		Attr map[string]string `parquet:"LogAttributes"`
	}](bytes.NewReader(objs[0].Body), int64(len(objs[0].Body)))
	if err != nil || len(lrows) != 1 {
		t.Fatal(err, len(lrows))
	}
	if lrows[0].Res["k8s.namespace.name"] != "dev-payments" || lrows[0].Res["user.id"] != alice ||
		lrows[0].Attr["user.id"] != "" || lrows[0].Attr["oscope.ingress.claimed.user.id"] != "reviewer-claimed" {
		t.Fatalf("%v %v", lrows[0].Res, lrows[0].Attr)
	}
}

func TestPolicyValidation(t *testing.T) {
	bad := []Policy{
		{Cluster: "Dev Tools", Rules: []Rule{{Tenant: tenantA, Role: "r", Namespace: "dev-a"}}},
		{Cluster: "devtools"},
		{Cluster: "devtools", Rules: []Rule{{Tenant: "contoso.com", Role: "r", Namespace: "dev-a"}}},
		{Cluster: "devtools", Rules: []Rule{{Tenant: tenantA, Role: "r", Group: groupOps, Namespace: "dev-a"}}},
		{Cluster: "devtools", Rules: []Rule{{Tenant: tenantA, Group: "Payments Team", Namespace: "dev-a"}}},
		{Cluster: "devtools", NamespacePrefix: "dev-", Rules: []Rule{{Tenant: tenantA, Role: "r", Namespace: "payments"}}},
	}
	for i, p := range bad {
		if p.Validate() == nil {
			t.Errorf("policy %d accepted", i)
		}
	}
	if _, err := NewEntraVerifier(EntraConfig{Authority: "https://login.microsoftonline.com", Tenants: []string{"common"},
		Audiences: []string{apiID}, ClientApps: []string{forwarder}, UserScope: "s"}, nil); err == nil {
		t.Error("tenant 'common' accepted")
	}
	if _, err := NewEntraVerifier(EntraConfig{Authority: "https://login.microsoftonline.com", Tenants: []string{tenantA},
		Audiences: []string{apiID}, UserScope: "s"}, nil); err == nil {
		t.Error("an empty client allow-list accepted")
	}
	l := DefaultLimits()
	l.DecodedBytesPerMinute = float64(l.MaxDecodedBytes) - 1
	if l.Validate() == nil {
		t.Error("a byte rate below one full request accepted")
	}
}

// The key set is read once, not per request, and a rotation is picked up.
func TestKeyRotation(t *testing.T) {
	r := newRig(t, "ingress-0", nil, nil, nil)
	body := protoBody(t, agentTraces())
	for range 3 {
		expect(t, r.post(t, "/v1/traces", "Bearer "+userToken(r.is, alice, "Team.Payments"), body, nil), 200, "k1")
	}
	if r.is.JWKSHits != 1 {
		t.Fatalf("%d key-set reads", r.is.JWKSHits)
	}
	r.is.AddKey("k2", "")
	r.srv.Verifier.SetClock(func() time.Time { return time.Now().Add(time.Minute) }) // past the min refresh gap
	tok := r.is.Mint(entratest.Token{Tenant: tenantA, OID: alice, Audience: apiID, ClientApp: forwarder, Scope: "Telemetry.Write",
		Roles: []string{"Team.Payments"}, Kid: "k2"})
	expect(t, r.post(t, "/v1/traces", "Bearer "+tok, body, nil), 200, "k2 after rotation")
	if r.is.JWKSHits != 2 {
		t.Fatalf("%d key-set reads", r.is.JWKSHits)
	}
}

// The D35 close: a drain refuses new requests, waits for the running ones,
// then closes every lane that has an epoch; the close slots are in the
// store after the data.
func TestDrainClosesLanes(t *testing.T) {
	r := newRig(t, "ingress-0", nil, nil, nil)
	body := protoBody(t, agentTraces())
	tok := "Bearer " + userToken(r.is, alice, "Team.Payments")
	expect(t, r.post(t, "/v1/traces", tok, body, nil), 200, "before the drain")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	beatCtx, stopBeats := context.WithCancel(context.Background())
	beatsDone := make(chan struct{})
	go func() { Heartbeats(beatCtx, r.edge, time.Hour, r.srv.Logf); close(beatsDone) }()
	n, err := r.srv.Drain(ctx, r.edge, func() { stopBeats(); <-beatsDone })
	if err != nil || n == 0 {
		t.Fatalf("drain: %d lanes closed, %v", n, err)
	}
	resp := r.post(t, "/v1/traces", tok, body, nil)
	expect(t, resp, 503, "after the drain")
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("no Retry-After")
	}
	closes := 0
	for _, k := range r.store.Keys("root/devtools/") {
		if o, _ := r.store.Get(k); o.Meta[commit.MetaKind] == commit.KindClose {
			closes++
		}
	}
	if closes != n {
		t.Fatalf("%d close slots in the store, %d reported", closes, n)
	}
	if c := r.srv.Counts(); c["draining"] != 1 || c["ok"] != 1 {
		t.Fatalf("counts %v", c)
	}
}

// A drain whose deadline passes while a request runs writes no close.
func TestDrainPastDeadlineWritesNoClose(t *testing.T) {
	r := newRig(t, "ingress-0", nil, nil, nil)
	if !r.srv.gate.enter() { // a handler that never finishes
		t.Fatal("gate closed")
	}
	defer r.srv.gate.leave()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if n, err := r.srv.Drain(ctx, r.edge, nil); err == nil || n != 0 {
		t.Fatalf("drain past the deadline: %d, %v", n, err)
	}
	for _, k := range r.store.Keys("root/devtools/") {
		if o, _ := r.store.Get(k); o.Meta[commit.MetaKind] == commit.KindClose {
			t.Fatal("a close was written with custody left")
		}
	}
}
