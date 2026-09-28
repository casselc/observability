// Package metrics is a small Prometheus text-format registry: counters and
// gauges with labels, nothing else.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type family struct {
	name, help, kind string
	labels           []string
	vals             map[string]float64 // joined label values → value
}

// Registry holds families.
type Registry struct {
	mu  sync.Mutex
	fam map[string]*family
}

// New is an empty registry.
func New() *Registry { return &Registry{fam: map[string]*family{}} }

// Counter declares a counter.
func (r *Registry) Counter(name, help string, labels ...string) {
	r.declare(name, help, "counter", labels)
}

// Gauge declares a gauge.
func (r *Registry) Gauge(name, help string, labels ...string) { r.declare(name, help, "gauge", labels) }

func (r *Registry) declare(name, help, kind string, labels []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fam[name] = &family{name: name, help: help, kind: kind, labels: labels, vals: map[string]float64{}}
}

const sep = "\xff"

func (r *Registry) get(name string, lv []string) (*family, string) {
	f := r.fam[name]
	if f == nil {
		panic("metrics: undeclared " + name)
	}
	if len(lv) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s wants %d labels", name, len(f.labels)))
	}
	return f, strings.Join(lv, sep)
}

// Add adds v to a counter or gauge.
func (r *Registry) Add(name string, v float64, lv ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, k := r.get(name, lv)
	f.vals[k] += v
}

// Inc adds 1.
func (r *Registry) Inc(name string, lv ...string) { r.Add(name, 1, lv...) }

// Set sets a gauge.
func (r *Registry) Set(name string, v float64, lv ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, k := r.get(name, lv)
	f.vals[k] = v
}

// Value reads one series (tests).
func (r *Registry) Value(name string, lv ...string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, k := r.get(name, lv)
	return f.vals[k]
}

// Write renders the text format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.fam))
	for n := range r.fam {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f := r.fam[n]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", n, f.help, n, f.kind)
		keys := make([]string, 0, len(f.vals))
		for k := range f.vals {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "%s%s %s\n", n, labelStr(f.labels, k), num(f.vals[k]))
		}
	}
}

func labelStr(names []string, joined string) string {
	if len(names) == 0 {
		return ""
	}
	vals := strings.Split(joined, sep)
	var sb strings.Builder
	sb.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			sb.WriteByte(',')
		}
		v := strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(vals[i])
		sb.WriteString(n + `="` + v + `"`)
	}
	sb.WriteByte('}')
	return sb.String()
}

func num(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
