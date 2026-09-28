// Package rule is an alert rule as data: a statement the query service runs
// over an event-time window, the condition a row must meet, how long it must
// hold, and the labels a page carries. Rules are loaded from a file
// (rules.example.yaml); nothing about a rule lives in code.
package rule

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration written as "5m", "30s".
type Duration time.Duration

// UnmarshalYAML reads "5m" or a number of seconds.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		*d = Duration(v * float64(time.Second))
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration %q: %v", s, err)
	}
	*d = Duration(v)
	return nil
}

// D is the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Condition compares one numeric column of each row with a threshold.
type Condition struct {
	Column    string  `yaml:"column" json:"column"` // default "value"
	Op        string  `yaml:"op" json:"op"`         // > >= < <= == !=
	Threshold float64 `yaml:"threshold" json:"threshold"`
}

// Holds says whether v meets the condition.
func (c Condition) Holds(v float64) bool {
	switch c.Op {
	case ">":
		return v > c.Threshold
	case ">=":
		return v >= c.Threshold
	case "<":
		return v < c.Threshold
	case "<=":
		return v <= c.Threshold
	case "==":
		return v == c.Threshold
	case "!=":
		return v != c.Threshold
	}
	return false
}

// Rule is one alert rule.
type Rule struct {
	Name string `yaml:"name"`
	// SQL runs through the query service's /v1/query with the window as
	// its `window`: the service restricts every table's time column to it.
	// Each row is one group: the condition's column is the value, every
	// other column is a label.
	SQL string `yaml:"sql"`
	// Window is the event-time length each evaluation reads; Every the step
	// between window ends (default: Window). Window ends are aligned to
	// multiples of Every since the Unix epoch, so every replica computes the
	// same windows.
	Window    Duration  `yaml:"window"`
	Every     Duration  `yaml:"every"`
	For       Duration  `yaml:"for"`
	Condition Condition `yaml:"condition"`
	// OnNoRows: "ok" (default: no row means no group holds) or "fire" (an
	// absence alert: no row is itself the condition). Either way it applies
	// only to a window the query service labels complete.
	OnNoRows    string            `yaml:"on_no_rows"`
	Severity    string            `yaml:"severity"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
	// Identity names the service identity (config `identities`) the
	// statement runs as: its token's scope is the rule's scope.
	Identity string `yaml:"identity"`
	// CannotEvaluateAfter: a window whose end is further behind now than
	// this and still cannot be evaluated pages as "cannot evaluate"
	// (default: the config's).
	CannotEvaluateAfter Duration `yaml:"cannot_evaluate_after"`
	// FailuresToPage: consecutive failed attempts (error, timeout) on one
	// window that page before CannotEvaluateAfter (default: the config's).
	// A refusal (4xx from the query service) pages at once.
	FailuresToPage int `yaml:"failures_to_page"`
	// Lateness: a margin on top of the query service's max_lateness (the
	// fleet's policy for how long after its event time a row may still be
	// received; the edge stamps received_at, the event time is the
	// sender's). complete_through is a bound on received_at, so a window is
	// evaluated only once complete_through ≥ its end + max_lateness +
	// Lateness (D26). Rows that arrive later than that are not in the
	// evaluation; the service counts them (AMBIGUITY.md X5, X12).
	// Default: the config's evaluation.lateness_s.
	Lateness Duration `yaml:"lateness"`
	// MaxGroups bounds the rows one evaluation may return (default 1000);
	// more is a failed evaluation, not a truncated one.
	MaxGroups int `yaml:"max_groups"`
	// Clusters narrows the rule to some of its identity's clusters (D29):
	// the query service filters rows to them and labels the result with
	// their complete_through only, so another cluster's stalled lane does
	// not hold this rule. Default: the identity's whole scope.
	Clusters []string `yaml:"clusters"`
}

// File is a rules file.
type File struct {
	Rules []*Rule `yaml:"rules"`
}

var nameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.-]{0,127}$`)
var labelRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// clusterRe is FORMAT.md §1's cluster name.
var clusterRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?$`)

// Reserved labels the evaluator sets itself.
var Reserved = map[string]bool{"alertname": true, "severity": true, "alert_rule": true, "alert_episode": true, "alert_kind": true}

// Validate fills defaults and checks the rule.
func (r *Rule) Validate() error {
	if !nameRe.MatchString(r.Name) {
		return fmt.Errorf("rule name %q: must match %s", r.Name, nameRe)
	}
	if strings.TrimSpace(r.SQL) == "" {
		return fmt.Errorf("rule %s: no sql", r.Name)
	}
	if r.Window <= 0 {
		return fmt.Errorf("rule %s: window must be > 0", r.Name)
	}
	if r.Every == 0 {
		r.Every = r.Window
	}
	if r.Every < Duration(time.Second) || time.Duration(r.Every)%time.Second != 0 {
		return fmt.Errorf("rule %s: every must be a whole number of seconds ≥ 1s", r.Name)
	}
	if r.For < 0 {
		return fmt.Errorf("rule %s: for must be ≥ 0", r.Name)
	}
	if r.Condition.Column == "" {
		r.Condition.Column = "value"
	}
	switch r.Condition.Op {
	case ">", ">=", "<", "<=", "==", "!=":
	default:
		return fmt.Errorf("rule %s: condition op %q", r.Name, r.Condition.Op)
	}
	if math.IsNaN(r.Condition.Threshold) || math.IsInf(r.Condition.Threshold, 0) {
		return fmt.Errorf("rule %s: threshold must be finite", r.Name)
	}
	switch r.OnNoRows {
	case "":
		r.OnNoRows = "ok"
	case "ok", "fire":
	default:
		return fmt.Errorf("rule %s: on_no_rows %q (ok or fire)", r.Name, r.OnNoRows)
	}
	if r.Severity == "" {
		r.Severity = "page"
	}
	for k := range r.Labels {
		if !labelRe.MatchString(k) || Reserved[k] {
			return fmt.Errorf("rule %s: label %q is invalid or reserved", r.Name, k)
		}
	}
	if r.Identity == "" {
		r.Identity = "default"
	}
	for _, c := range r.Clusters {
		if !clusterRe.MatchString(c) {
			return fmt.Errorf("rule %s: cluster %q is not a cluster name", r.Name, c)
		}
	}
	if r.Lateness < 0 {
		return fmt.Errorf("rule %s: lateness must be ≥ 0", r.Name)
	}
	if r.MaxGroups == 0 {
		r.MaxGroups = 1000
	}
	return nil
}

// Spec is a hash of what decides the windows and their outcome; a changed
// spec restarts a rule's groups (their history was about another rule).
func (r *Rule) Spec() string {
	b, _ := json.Marshal([]any{r.SQL, r.Window, r.Every, r.For, r.Condition, r.OnNoRows})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8])
}

// Load reads and validates a rules file (YAML or JSON).
func Load(path string) ([]*Rule, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Parse reads and validates rules.
func Parse(b []byte) ([]*Rule, error) {
	var f File
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, r := range f.Rules {
		if err := r.Validate(); err != nil {
			return nil, err
		}
		if seen[r.Name] {
			return nil, fmt.Errorf("rule %s defined twice", r.Name)
		}
		seen[r.Name] = true
	}
	if len(f.Rules) == 0 {
		return nil, errors.New("no rules")
	}
	return f.Rules, nil
}

// ---- windows ------------------------------------------------------------

// Window is a half-open event-time range [FromNs, ToNs).
type Window struct {
	FromNs int64 `json:"from_ns"`
	ToNs   int64 `json:"to_ns"`
}

// WindowEnding is the window whose end is end.
func (r *Rule) WindowEnding(end int64) Window {
	return Window{FromNs: end - int64(r.Window), ToNs: end}
}

// AlignDown is the latest window end ≤ t.
func (r *Rule) AlignDown(t int64) int64 {
	e := int64(r.Every)
	q := t / e
	if t < 0 && t%e != 0 {
		q--
	}
	return q * e
}

// ---- groups -------------------------------------------------------------

// GroupKey is a canonical, order-independent key for a label set.
func GroupKey(labels map[string]string) string {
	ks := make([]string, 0, len(labels))
	for k := range labels {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	var sb strings.Builder
	for _, k := range ks {
		sb.WriteString(strconv.Quote(k))
		sb.WriteByte('=')
		sb.WriteString(strconv.Quote(labels[k]))
		sb.WriteByte(',')
	}
	h := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(h[:8])
}

// Row is one result row as a group and its value.
type Row struct {
	Labels map[string]string
	Value  float64
}

// Rows turns ClickHouse FORMAT JSON data (a list of objects) into rows:
// the condition's column is the value, every other column a label. A value
// that is missing, null, not a number or not finite is an error: the
// evaluation failed; it is never read as "does not hold".
func (r *Rule) Rows(data []map[string]any) ([]Row, error) {
	if len(data) > r.MaxGroups {
		return nil, fmt.Errorf("%d rows, more than max_groups %d", len(data), r.MaxGroups)
	}
	out := make([]Row, 0, len(data))
	seen := map[string]bool{}
	for i, d := range data {
		raw, ok := d[r.Condition.Column]
		if !ok {
			return nil, fmt.Errorf("row %d: no column %q", i, r.Condition.Column)
		}
		v, err := number(raw)
		if err != nil {
			return nil, fmt.Errorf("row %d: %s: %v", i, r.Condition.Column, err)
		}
		lb := map[string]string{}
		for k, x := range d {
			if k == r.Condition.Column {
				continue
			}
			lb[k] = str(x)
		}
		k := GroupKey(lb)
		if seen[k] {
			return nil, fmt.Errorf("row %d: two rows for the same group %v", i, lb)
		}
		seen[k] = true
		out = append(out, Row{Labels: lb, Value: v})
	}
	return out, nil
}

func number(x any) (float64, error) {
	var v float64
	switch t := x.(type) {
	case float64:
		v = t
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return 0, err
		}
		v = f
	case string: // ClickHouse quotes 64-bit integers
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, fmt.Errorf("not a number: %q", t)
		}
		v = f
	case nil:
		return 0, errors.New("null")
	default:
		return 0, fmt.Errorf("not a number: %T", x)
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("not finite: %v", v)
	}
	return v, nil
}

func str(x any) string {
	switch t := x.(type) {
	case string:
		return t
	case nil:
		return ""
	case json.Number:
		return t.String()
	}
	b, _ := json.Marshal(x)
	return string(b)
}
