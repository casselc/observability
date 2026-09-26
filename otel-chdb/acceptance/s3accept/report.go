package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

type Status string

const (
	PASS Status = "PASS"
	FAIL Status = "FAIL"
	WARN Status = "WARN"
	SKIP Status = "SKIP"
	INFO Status = "INFO"
)

// Level says how much a check matters to the design.
type Level string

const (
	Required    Level = "required"    // a FAIL rejects a capability (see capabilities)
	Recommended Level = "recommended" // a FAIL costs something (a workaround, IAM, config)
	Info        Level = "info"        // measured, never gates
)

// Result is one check's outcome. Meaning is filled on FAIL and WARN: what
// the result means for the design, and what to do.
type Result struct {
	ID         string         `json:"id"`
	Title      string         `json:"title"`
	Level      Level          `json:"level"`
	Needs      []string       `json:"capabilities,omitempty"`
	Status     Status         `json:"status"`
	Summary    string         `json:"summary"`
	Meaning    string         `json:"meaning,omitempty"`
	Evidence   []string       `json:"evidence,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
	DurationMS int64          `json:"duration_ms"`
}

func (r *Result) Log(format string, a ...any) {
	r.Evidence = append(r.Evidence, fmt.Sprintf(format, a...))
}

func (r *Result) Set(k string, v any) {
	if r.Data == nil {
		r.Data = map[string]any{}
	}
	r.Data[k] = v
}

// Worsen moves the status towards FAIL (never back), keeping the first
// meaning of the worst status.
func (r *Result) Worsen(s Status, summary, meaning string) {
	rank := map[Status]int{"": -1, PASS: 0, INFO: 0, SKIP: 1, WARN: 2, FAIL: 3}
	if rank[s] > rank[r.Status] || (rank[s] == rank[r.Status] && r.Meaning == "" && meaning != "") {
		r.Status = s
		if summary != "" {
			r.Summary = summary
		}
		if meaning != "" {
			r.Meaning = meaning
		}
	}
}

// Check is one probe. Plan says, for --dry-run, which requests it sends.
type Check struct {
	ID    string
	Title string
	Level Level
	Needs []string
	Plan  func(p Params) string
	Run   func(ctx context.Context, e *Env, r *Result)
}

// Capability is something the design builds on a store property. It holds
// when every check it needs passes (WARN counts as a pass with a caveat).
type Capability struct {
	ID       string   `json:"id"`
	Title    string   `json:"title"`
	IfFail   string   `json:"if_fail"`
	Verdict  string   `json:"verdict"`
	Failing  []string `json:"failing,omitempty"`
	Unproven []string `json:"unproven,omitempty"`
}

var capabilities = []Capability{
	{ID: "control-plane", Title: "S3-native control plane: create-only log slots, epoch fence, CAS'd leases and checkpoints",
		IfFail: "Do not run the manifest-less / S3-native commit on this store. Put the control plane (log slots, leases, " +
			"checkpoint, gc.json) behind the Coordinator interface on ClickHouse Keeper (model/S3NATIVE.md section 9, " +
			"fallback (b)); data objects can stay on this store with plain PUTs."},
	{ID: "inline-consumer", Title: "otap-rs consumer: HEAD slots with metadata, LIST StartAfter discovery, 404 on free slots, DELETE for GC",
		IfFail: "The consumer as built (otap-rs README, Consumer) cannot run unchanged: see each failing check for the workaround."},
	{ID: "exporter-data", Title: "Exporter data path: plain and create-only PUTs of 100 KB-1 MB objects with the SDKs' default checksums",
		IfFail: "Exporters need configuration (IAM, bucket policy, or AWS_REQUEST_CHECKSUM_CALCULATION=when_required) before they can publish."},
}

func verdicts(results []*Result) []Capability {
	out := make([]Capability, 0, len(capabilities))
	for _, c := range capabilities {
		c := c
		ran := map[string]bool{}
		for _, r := range results {
			ran[r.ID] = true
		}
		for _, ch := range checks() {
			for _, n := range ch.Needs {
				if n == c.ID && !ran[ch.ID] {
					c.Unproven = append(c.Unproven, ch.ID)
				}
			}
		}
		for _, r := range results {
			for _, n := range r.Needs {
				if n != c.ID {
					continue
				}
				switch r.Status {
				case FAIL:
					c.Failing = append(c.Failing, r.ID)
				case SKIP:
					c.Unproven = append(c.Unproven, r.ID)
				}
			}
		}
		switch {
		case len(c.Failing) > 0:
			c.Verdict = "REJECTED"
		case len(c.Unproven) > 0:
			c.Verdict = "UNPROVEN"
		default:
			c.Verdict = "ACCEPTED"
		}
		out = append(out, c)
	}
	return out
}

// Report is the JSON document.
type Report struct {
	Tool         string         `json:"tool"`
	Mode         string         `json:"mode"`
	Started      time.Time      `json:"started"`
	Finished     time.Time      `json:"finished"`
	Store        string         `json:"store"`
	Target       map[string]any `json:"target"`
	Params       Params         `json:"params"`
	Results      []*Result      `json:"results"`
	Capabilities []Capability   `json:"capabilities,omitempty"`
	Requests     map[string]int `json:"requests"`
}

func writeJSON(path string, rep *Report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func wrap(s string, width int, indent string) string {
	var lines []string
	for _, para := range strings.Split(s, "\n") {
		words := strings.Fields(para)
		line := ""
		for _, w := range words {
			if line != "" && len(line)+1+len(w) > width {
				lines = append(lines, line)
				line = w
			} else if line == "" {
				line = w
			} else {
				line += " " + w
			}
		}
		lines = append(lines, line)
	}
	return indent + strings.Join(lines, "\n"+indent)
}

func printTable(w io.Writer, rep *Report, verbose bool) {
	fmt.Fprintf(w, "\n%-5s  %-11s  %-18s  %s\n", "", "LEVEL", "CHECK", "RESULT")
	fmt.Fprintln(w, strings.Repeat("-", 100))
	for _, r := range rep.Results {
		fmt.Fprintf(w, "%-5s  %-11s  %-18s  %s\n", r.Status, r.Level, r.ID, r.Summary)
		if r.Meaning != "" && (r.Status == FAIL || r.Status == WARN) {
			fmt.Fprintln(w, wrap("→ "+r.Meaning, 92, strings.Repeat(" ", 7)))
		}
		if verbose {
			for _, ev := range r.Evidence {
				fmt.Fprintf(w, "         · %s\n", ev)
			}
		}
	}
	if len(rep.Capabilities) > 0 {
		fmt.Fprintln(w, "\nWhat the design can rely on here:")
		for _, c := range rep.Capabilities {
			fmt.Fprintf(w, "  %-9s %s\n", c.Verdict, c.Title)
			if len(c.Failing) > 0 {
				fmt.Fprintf(w, "            failing: %s\n", strings.Join(c.Failing, ", "))
				fmt.Fprintln(w, wrap(c.IfFail, 88, strings.Repeat(" ", 12)))
			}
			if len(c.Unproven) > 0 {
				fmt.Fprintf(w, "            not run or skipped: %s\n", strings.Join(c.Unproven, ", "))
			}
		}
	}
	keys := make([]string, 0, len(rep.Requests))
	total := 0
	for k, v := range rep.Requests {
		keys = append(keys, k)
		total += v
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, rep.Requests[k]))
	}
	fmt.Fprintf(w, "\nHTTP requests sent: %d (%s)\n", total, strings.Join(parts, " "))
}
