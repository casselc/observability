package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
)

const sec = int64(time.Second)

func testRule(t testing.TB, mod func(*rule.Rule)) *rule.Rule {
	t.Helper()
	r := &rule.Rule{Name: "errors", SQL: "SELECT ServiceName AS service, count() AS value FROM otel_logs GROUP BY service",
		Window: rule.Duration(time.Minute), Condition: rule.Condition{Op: ">", Threshold: 2},
		Annotations: map[string]string{"summary": "{{labels.service}}: {{value}} errors"}}
	if mod != nil {
		mod(r)
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func rows(kv ...any) []rule.Row {
	var out []rule.Row
	for i := 0; i < len(kv); i += 2 {
		out = append(out, rule.Row{Labels: map[string]string{"service": kv[i].(string)}, Value: float64(kv[i+1].(int))})
	}
	return out
}

func complete(rs []rule.Row) Result {
	return Result{Outcome: Complete, Rows: rs, WatermarkStatus: "ok"}
}

var cfg = func() Config {
	c := Config{HoldResolved: 30 * time.Second, Refresh: time.Minute}
	c.Defaults()
	return c
}()

func TestNewStartsAtTheLatestEndedWindow(t *testing.T) {
	r := testRule(t, nil)
	st := New(r, 1000*sec+17*sec)
	if st.NextEndNs != 960*sec { // 16 min: aligned to every = 1m
		t.Fatalf("next end %d", st.NextEndNs/sec)
	}
}

func TestOnlyCompleteWindowsChangeGroups(t *testing.T) {
	r := testRule(t, nil)
	st := New(r, 600*sec)
	if !Apply(r, st, complete(rows("a", 5)), 0) {
		t.Fatal("complete must advance")
	}
	if len(st.Notices) != 1 || st.Groups[rule.GroupKey(map[string]string{"service": "a"})].Phase != Firing {
		t.Fatalf("a fires: %+v", st.Groups)
	}
	next := st.NextEndNs
	// partial / unknown / failures with no rows: the firing group is NOT resolved
	for _, o := range []Outcome{Partial, Unknown, Failed, Timeout, Refused, BadResult} {
		if Apply(r, st, Result{Outcome: o}, 0) {
			t.Fatalf("%s advanced", o)
		}
		if st.NextEndNs != next || len(st.Groups) != 1 {
			t.Fatalf("%s changed groups or the window", o)
		}
		for _, n := range st.Notices {
			if n.Want != Firing {
				t.Fatalf("%s resolved a firing alert: no rows in an incomplete window read as OK", o)
			}
		}
	}
	if st.Attempt == nil || st.Attempt.Tries != 6 || st.Attempt.Failures != 4 {
		t.Fatalf("attempt %+v", st.Attempt) // waits reset the failure count; the 4 failures after them count
	}
	// a complete window with no rows resolves it
	Apply(r, st, complete(nil), 0)
	for _, n := range st.Notices {
		if n.Want != Resolved || n.EndsNs != next {
			t.Fatalf("not resolved at the window's end: %+v", n)
		}
	}
	if len(st.Groups) != 0 || st.Attempt != nil {
		t.Fatal("groups or attempt left")
	}
}

func TestForHoldsAcrossWindows(t *testing.T) {
	r := testRule(t, func(r *rule.Rule) { r.For = rule.Duration(2 * time.Minute) })
	st := New(r, 600*sec)
	for i := 0; i < 2; i++ {
		Apply(r, st, complete(rows("a", 5)), 0)
		if len(st.Notices) != 0 {
			t.Fatalf("fired after %d windows", i+1)
		}
	}
	Apply(r, st, complete(rows("a", 5)), 0)
	if len(st.Notices) != 1 {
		t.Fatal("did not fire after 2m of holding")
	}
	// a pending group that stops holding is dropped silently
	st2 := New(r, 600*sec)
	Apply(r, st2, complete(rows("b", 5)), 0)
	Apply(r, st2, complete(rows("b", 1)), 0)
	if len(st2.Groups) != 0 || len(st2.Notices) != 0 {
		t.Fatal("pending group kept or notified")
	}
}

func TestOnNoRowsFire(t *testing.T) {
	r := testRule(t, func(r *rule.Rule) { r.OnNoRows = "fire" })
	st := New(r, 600*sec)
	Apply(r, st, Result{Outcome: Unknown}, 0)
	if len(st.Notices) != 0 {
		t.Fatal("absence alert on an unknown window")
	}
	Apply(r, st, complete(nil), 0)
	if len(st.Notices) != 1 {
		t.Fatal("absence alert did not fire on a complete window with no rows")
	}
	Apply(r, st, complete(rows("a", 0)), 0)
	for _, n := range st.Notices {
		if n.Want != Resolved {
			t.Fatal("absence alert not resolved when a row came back")
		}
	}
}

func TestDedupKeyIsDeterministic(t *testing.T) {
	r := testRule(t, nil)
	a, b := New(r, 600*sec), New(r, 600*sec)
	Apply(r, a, complete(rows("x", 9, "y", 9)), 1)
	Apply(r, b, complete(rows("y", 9, "x", 9)), 99999)
	if len(a.Notices) != 2 {
		t.Fatal(a.Notices)
	}
	for k := range a.Notices {
		if b.Notices[k] == nil {
			t.Fatalf("replicas disagree on key %s", k)
		}
	}
}

func TestCannotEvaluate(t *testing.T) {
	r := testRule(t, nil)
	c := cfg
	c.CannotEvaluateAfter = 5 * time.Minute
	st := New(r, 600*sec)
	end := st.NextEndNs
	res := Result{Outcome: Partial, WatermarkStatus: "ok", CompleteThroughNs: end - 30*sec,
		Holding: []Lane{{"prod-b/pub-0/logs", 330}, {"prod-b/pub-1/logs", 320}}, Stale: []Lane{{"prod-c/pub-0/logs", 900}}}
	Apply(r, st, res, 0)
	if UpdateMeta(r, st, end+4*60*sec, 0, c) || st.Meta.Firing {
		t.Fatal("paged before the bound")
	}
	if !UpdateMeta(r, st, end+6*60*sec, 0, c) || !st.Meta.Firing {
		t.Fatal("no page past the bound")
	}
	n := st.Notices[st.Meta.Key]
	if n.Labels["alertname"] != "AlertCannotEvaluate" || n.Labels["alert_rule"] != "errors" {
		t.Fatalf("labels %v", n.Labels)
	}
	reason := n.Annotations["reason"]
	for _, want := range []string{"past complete_through", "prod-b/pub-0/logs (330s behind)", "stale lanes: prod-c/pub-0/logs", "clusters: prod-b, prod-c"} {
		if !strings.Contains(reason, want) {
			t.Errorf("reason %q lacks %q", reason, want)
		}
	}
	// the reason follows the attempts: now the watermark is stale
	Apply(r, st, Result{Outcome: Unknown, WatermarkStatus: "stale", WatermarkAgeS: 400}, 0)
	if !UpdateMeta(r, st, end+7*60*sec, 0, c) || !strings.Contains(st.Notices[st.Meta.Key].Annotations["reason"], "watermark stale (published 400s ago)") {
		t.Fatalf("reason not updated: %v", st.Notices[st.Meta.Key].Annotations["reason"])
	}
	key := st.Meta.Key
	// catching up resolves it
	Apply(r, st, complete(nil), 0)
	if !UpdateMeta(r, st, end+8*60*sec, 0, c) || st.Meta.Firing || st.Notices[key].Want != Resolved {
		t.Fatal("not resolved after the window was evaluated")
	}
}

func TestFailuresPage(t *testing.T) {
	r := testRule(t, nil)
	c := cfg
	st := New(r, 600*sec)
	now := st.NextEndNs + sec
	for i := 0; i < c.FailuresToPage-1; i++ {
		Apply(r, st, Result{Outcome: Timeout, Err: "deadline"}, 0)
		UpdateMeta(r, st, now, 0, c)
		if st.Meta.Firing {
			t.Fatalf("paged after %d failures", i+1)
		}
	}
	Apply(r, st, Result{Outcome: Failed, Err: "HTTP 502 central_error"}, 0)
	UpdateMeta(r, st, now, 0, c)
	if !st.Meta.Firing || !strings.Contains(st.Notices[st.Meta.Key].Annotations["reason"], "central_error") {
		t.Fatal("repeated failures did not page")
	}
	// a refusal pages at once
	st2 := New(r, 600*sec)
	Apply(r, st2, Result{Outcome: Refused, Err: "HTTP 403 table_not_allowed"}, 0)
	UpdateMeta(r, st2, st2.NextEndNs+sec, 0, c)
	if !st2.Meta.Firing {
		t.Fatal("a refusal did not page")
	}
}

func TestLedger(t *testing.T) {
	r := testRule(t, nil)
	c := cfg
	st := New(r, 600*sec)
	Apply(r, st, complete(rows("a", 5)), 0)
	var key string
	for k := range st.Notices {
		key = k
	}
	now := int64(1_000_000)
	s := DueSends(st, now, c)
	if len(s) != 1 || s[0].Phase != Firing || s[0].Notice.Key != key {
		t.Fatalf("due %v", s)
	}
	// no answer: not delivered; retried after a backoff with the same key
	Delivered(st, s, false, "ambiguous", now, c)
	if len(DueSends(st, now+1, c)) != 0 {
		t.Fatal("no backoff")
	}
	s = DueSends(st, now+c.BackoffBase.Milliseconds(), c)
	if len(s) != 1 || s[0].Notice.Key != key || s[0].Phase != Firing {
		t.Fatal("not retried with the same key")
	}
	// resolved before the firing was ever acknowledged: firing goes first
	Apply(r, st, complete(nil), 0)
	s = DueSends(st, now+c.BackoffBase.Milliseconds(), c)
	if len(s) != 1 || s[0].Phase != Firing {
		t.Fatalf("resolution sent before the firing was acknowledged: %v", s)
	}
	t0 := now + 10_000
	Delivered(st, s, true, "acked", t0, c)
	if got := DueSends(st, t0+c.HoldResolved.Milliseconds()-1, c); len(got) != 0 {
		t.Fatal("resolution sent within the hold")
	}
	s = DueSends(st, t0+c.HoldResolved.Milliseconds(), c)
	if len(s) != 1 || s[0].Phase != Resolved {
		t.Fatalf("resolution not due: %v", s)
	}
	Delivered(st, s, false, "5xx", t0+c.HoldResolved.Milliseconds(), c)
	if st.Notices[key] == nil {
		t.Fatal("a 5xx removed the notice")
	}
	s = DueSends(st, t0+c.HoldResolved.Milliseconds()+c.BackoffMax.Milliseconds(), c)
	Delivered(st, s, true, "acked", 0, c)
	if st.Notices[key] != nil {
		t.Fatal("an acknowledged resolution stays in the ledger")
	}
}

func TestRefresh(t *testing.T) {
	r := testRule(t, nil)
	c := cfg
	st := New(r, 600*sec)
	Apply(r, st, complete(rows("a", 5)), 0)
	s := DueSends(st, 0, c)
	Delivered(st, s, true, "acked", 1000, c)
	if len(DueSends(st, 1000+c.Refresh.Milliseconds()-1, c)) != 0 {
		t.Fatal("refreshed early")
	}
	if s := DueSends(st, 1000+c.Refresh.Milliseconds(), c); len(s) != 1 || s[0].Phase != Firing {
		t.Fatal("firing alert not refreshed")
	}
	c.Refresh = 0
	if len(DueSends(st, 1e12, c)) != 0 {
		t.Fatal("refresh 0 re-sent an acknowledged firing")
	}
}

func TestSkipBacklog(t *testing.T) {
	r := testRule(t, nil)
	c := cfg
	c.MaxBacklog = 10 * time.Minute
	st := New(r, 600*sec)
	if n := SkipBacklog(r, st, st.NextEndNs+9*60*sec, c); n != 0 {
		t.Fatal("skipped within the bound")
	}
	n := SkipBacklog(r, st, st.NextEndNs+60*60*sec, c)
	if n != 50 || st.Skipped != 50 || st.NextEndNs != 600*sec+50*60*sec {
		t.Fatalf("skipped %d, next %d", n, st.NextEndNs/sec)
	}
}

func TestReconcileResolvesOnSpecChange(t *testing.T) {
	r := testRule(t, nil)
	st := New(r, 600*sec)
	Apply(r, st, complete(rows("a", 5)), 0)
	r2 := testRule(t, func(r *rule.Rule) { r.Condition.Threshold = 100 })
	if !Reconcile(r2, st, 0, 0) || len(st.Groups) != 0 {
		t.Fatal("spec change kept groups")
	}
	for _, n := range st.Notices {
		if n.Want != Resolved || n.Annotations["resolved_because"] == "" {
			t.Fatal("firing alert of the old spec not resolved")
		}
	}
	if Reconcile(r2, st, 0, 0) {
		t.Fatal("reconcile not idempotent")
	}
}
