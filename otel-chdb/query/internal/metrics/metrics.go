// Package metrics is a small Prometheus text-format registry: counters with
// labels, and gauges read at scrape time.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
)

// Registry holds the service's metrics.
type Registry struct {
	mu       sync.Mutex
	counters map[string]*counter
	gauges   map[string]*gauge
}

type counter struct {
	help   string
	labels []string
	vals   map[string]float64
}

type gauge struct {
	help string
	fn   func() []Sample
}

// Sample is one gauge value with its label values.
type Sample struct {
	Labels []string // alternating name, value
	Value  float64
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{counters: map[string]*counter{}, gauges: map[string]*gauge{}}
}

// Counter declares a counter.
func (r *Registry) Counter(name, help string, labels ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters[name] = &counter{help: help, labels: labels, vals: map[string]float64{}}
}

// Add adds v to the counter with these label values (in declared order).
func (r *Registry) Add(name string, v float64, values ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counters[name]
	if !ok || len(values) != len(c.labels) {
		panic(fmt.Sprintf("metrics: %s: undeclared or wrong label count", name))
	}
	c.vals[strings.Join(values, "\x00")] += v
}

// Inc adds 1.
func (r *Registry) Inc(name string, values ...string) { r.Add(name, 1, values...) }

// Get returns a counter's value (tests).
func (r *Registry) Get(name string, values ...string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c.vals[strings.Join(values, "\x00")]
	}
	return 0
}

// Gauge declares a gauge computed at scrape time.
func (r *Registry) Gauge(name, help string, fn func() []Sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = &gauge{help: help, fn: fn}
}

// Write renders the text format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	names := make([]string, 0, len(r.counters)+len(r.gauges))
	for n := range r.counters {
		names = append(names, n)
	}
	for n := range r.gauges {
		names = append(names, n)
	}
	sort.Strings(names)
	type snap struct {
		help, kind string
		lines      []string
	}
	var out []snap
	var gauges []string
	for _, n := range names {
		if c, ok := r.counters[n]; ok {
			s := snap{help: fmt.Sprintf("# HELP %s %s\n", n, c.help), kind: fmt.Sprintf("# TYPE %s counter\n", n)}
			keys := make([]string, 0, len(c.vals))
			for k := range c.vals {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				var lv []string
				if len(c.labels) > 0 {
					vals := strings.Split(k, "\x00")
					for i, l := range c.labels {
						lv = append(lv, l, vals[i])
					}
				}
				s.lines = append(s.lines, n+labels(lv)+" "+num(c.vals[k])+"\n")
			}
			out = append(out, s)
		} else {
			gauges = append(gauges, n)
		}
	}
	gs := map[string]*gauge{}
	for _, n := range gauges {
		gs[n] = r.gauges[n]
	}
	r.mu.Unlock()
	for _, s := range out {
		io.WriteString(w, s.help+s.kind+strings.Join(s.lines, ""))
	}
	for _, n := range gauges {
		g := gs[n]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", n, g.help, n)
		for _, s := range g.fn() {
			fmt.Fprintf(w, "%s%s %s\n", n, labels(s.Labels), num(s.Value))
		}
	}
}

func labels(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		v := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(kv[i+1])
		fmt.Fprintf(&b, `%s="%s"`, kv[i], v)
	}
	b.WriteByte('}')
	return b.String()
}

func num(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return fmt.Sprintf("%g", v)
}
