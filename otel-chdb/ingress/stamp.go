package ingress

import (
	"sort"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/casselc/observability/otel-chdb/parquetgo"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Attribution is what the ingress asserts about a request: the tenant scope
// and who sent it. Every field comes from the verified token and the
// policy; none from the request.
type Attribution struct {
	Cluster     string        // k8s.cluster.name: the policy's cluster
	Namespace   string        // k8s.namespace.name: the resolved namespace
	UserID      string        // user.id: the principal's Entra object id (oid)
	EntraTenant string        // oscope.ingress.entra_tenant: tid
	Type        PrincipalType // oscope.ingress.principal_type
	ClientApp   string        // oscope.ingress.client_app: azp
}

// Attribute names the ingress owns. The tenant keys are the ones the whole
// pipeline scopes by (the query service's additional_table_filters read
// ResourceAttributes['k8s.cluster.name'] and ['k8s.namespace.name']).
const (
	AttrCluster       = "k8s.cluster.name"
	AttrNamespace     = "k8s.namespace.name"
	AttrUserID        = "user.id"
	AttrAuth          = "oscope.ingress.auth"
	AttrEntraTenant   = "oscope.ingress.entra_tenant"
	AttrPrincipalType = "oscope.ingress.principal_type"
	AttrClientApp     = "oscope.ingress.client_app"
	// ClaimedPrefix keeps what the producer claimed, as a label only.
	ClaimedPrefix = "oscope.ingress.claimed."
)

// reservedResource reports whether a producer may not set resource key k:
// the tenant and identity keys, every k8s.* key (no pod, node or cluster
// exists for this producer; a claim would join it to a real workload's
// entity), and the ingress's own namespace.
func reservedResource(k string) bool {
	return strings.HasPrefix(k, "k8s.") || strings.HasPrefix(k, "oscope.") ||
		k == AttrUserID || k == "enduser.id" || k == "enduser.pseudo.id"
}

// identityKeys are span and log-record attributes by which a producer
// claims who the user is (OTel user.id, the deprecated enduser.*, Langfuse's
// langfuse.user.id). They stay, renamed, as claims; views read the
// resource's user.id.
func identityKey(k string) bool {
	return k == AttrUserID || k == "enduser.id" || k == "enduser.pseudo.id" || k == "langfuse.user.id" ||
		strings.HasPrefix(k, "oscope.")
}

// stampResource rewrites one resource's attributes. It is a pure function
// of (attributes, attribution): claims are moved in key order, stamps are
// appended in a fixed order, and nothing time- or token-dependent (exp, a
// token id, the receive time) is written, so a sender's retry of the same
// bytes, even with a refreshed token and to another replica, produces the
// same request bytes and the same content key, which the consumer skips as
// a copy (DECISIONS D11).
func stampResource(m pcommon.Map, a Attribution) {
	claimed := moveClaims(m, reservedResource)
	for _, kv := range claimed {
		m.PutStr(ClaimedPrefix+kv[0], kv[1])
	}
	m.PutStr(AttrCluster, a.Cluster)
	m.PutStr(AttrNamespace, a.Namespace)
	m.PutStr(AttrUserID, a.UserID)
	m.PutStr(AttrAuth, "entra")
	m.PutStr(AttrEntraTenant, a.EntraTenant)
	m.PutStr(AttrPrincipalType, string(a.Type))
	m.PutStr(AttrClientApp, a.ClientApp)
}

// moveClaims removes every key reserved(k) and returns the removed pairs
// (value as a string), sorted by key. Claims under oscope.* are dropped,
// not kept: a producer cannot pre-seed a "claimed" label either.
func moveClaims(m pcommon.Map, reserved func(string) bool) [][2]string {
	var out [][2]string
	m.RemoveIf(func(k string, v pcommon.Value) bool {
		if !reserved(k) {
			return false
		}
		if !strings.HasPrefix(k, "oscope.") {
			out = append(out, [2]string{k, parquetgo.AttrString(v)})
		}
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

func stampAttrs(m pcommon.Map) {
	for _, kv := range moveClaims(m, identityKey) {
		m.PutStr(ClaimedPrefix+kv[0], kv[1])
	}
}

// StampTraces applies a to every resource and demotes identity claims on
// spans and span events.
func StampTraces(td ptrace.Traces, a Attribution) {
	rss := td.ResourceSpans()
	for i := range rss.Len() {
		rs := rss.At(i)
		stampResource(rs.Resource().Attributes(), a)
		sss := rs.ScopeSpans()
		for j := range sss.Len() {
			spans := sss.At(j).Spans()
			for k := range spans.Len() {
				s := spans.At(k)
				stampAttrs(s.Attributes())
				evs := s.Events()
				for e := range evs.Len() {
					stampAttrs(evs.At(e).Attributes())
				}
			}
		}
	}
}

// StampLogs applies a to every resource and demotes identity claims on log
// records (scores are gen_ai.evaluation.result records: their author is the
// resource's user.id, never a claimed one, research/langfuse.md SEC-L6).
func StampLogs(ld plog.Logs, a Attribution) {
	rls := ld.ResourceLogs()
	for i := range rls.Len() {
		rl := rls.At(i)
		stampResource(rl.Resource().Attributes(), a)
		sls := rl.ScopeLogs()
		for j := range sls.Len() {
			lrs := sls.At(j).LogRecords()
			for k := range lrs.Len() {
				stampAttrs(lrs.At(k).Attributes())
			}
		}
	}
}
