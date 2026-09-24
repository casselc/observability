package qtrace

import (
	"strings"
	"testing"
	"time"
)

func st(action string, seq uint64, thread string, order int64) Step {
	s := Step{Action: action, Seq: seq, Recorder: "r", Actor: "a", Process: "p", Thread: thread,
		Time: time.Unix(0, int64(seq)), Args: map[string]any{}}
	if order > 0 {
		s.Order = map[string]int64{"batch": order}
	}
	return s
}

func actions(t *Trace) string {
	var out []string
	for _, s := range t.Steps {
		out = append(out, s.Action)
	}
	return strings.Join(out, " ")
}

// B started second (batch 2) but recorded its start first; B's table write
// is recorded before A's start. The domain clock puts A's start first, and
// program order keeps B's write after B's start.
func TestDomainClockReorders(t *testing.T) {
	steps := []Step{
		st("startB", 1, "B", 2),
		st("tableB", 2, "B", 0),
		st("startA", 3, "A", 1),
		st("tableA", 4, "A", 0),
	}
	traces, issues := Reconstruct(steps)
	if len(issues) != 0 || len(traces) != 1 || len(traces[0].Issues) != 0 {
		t.Fatalf("issues %v %v", issues, traces[0].Issues)
	}
	if got := actions(traces[0]); got != "startA startB tableB tableA" {
		t.Fatalf("order: %s", got)
	}
}

// Without thread keys, seq order is only a preference and the same input
// would put tableB before startB: program order matters.
func TestWithoutThreadsSeqIsOnlyPreferred(t *testing.T) {
	steps := []Step{st("startB", 1, "", 2), st("tableB", 2, "", 0), st("startA", 3, "", 1)}
	traces, _ := Reconstruct(steps)
	if got := actions(traces[0]); got != "tableB startA startB" {
		t.Fatalf("order: %s", got)
	}
}

func TestGapsAndDuplicates(t *testing.T) {
	steps := []Step{st("x", 1, "", 0), st("x", 1, "", 0), st("y", 3, "", 0)}
	traces, issues := Reconstruct(steps)
	if len(issues) != 1 || issues[0].Kind != "duplicate" {
		t.Fatalf("issues: %v", issues)
	}
	if len(traces[0].Issues) != 1 || traces[0].Issues[0].Kind != "gap" || !strings.Contains(traces[0].Issues[0].Msg, "[2]") {
		t.Fatalf("trace issues: %v", traces[0].Issues)
	}
}

func TestProcessesAndOverlap(t *testing.T) {
	a, b, c := st("x", 1, "", 0), st("y", 2, "", 0), st("z", 3, "", 0)
	a.Process, b.Process, c.Process = "p1", "p2", "p1"
	traces, _ := Reconstruct([]Step{a, b, c})
	tr := traces[0]
	if strings.Join(tr.Processes, ",") != "p1,p2" || len(tr.Issues) != 1 || tr.Issues[0].Kind != "overlap" {
		t.Fatalf("processes %v issues %v", tr.Processes, tr.Issues)
	}
}

func TestContradictoryKeysReportCycle(t *testing.T) {
	// program order says 1 before 2, the domain clock says 2 before 1
	steps := []Step{st("a", 1, "T", 2), st("b", 2, "T", 1)}
	traces, _ := Reconstruct(steps)
	if len(traces[0].Issues) != 1 || traces[0].Issues[0].Kind != "cycle" || len(traces[0].Steps) != 2 {
		t.Fatalf("issues %v", traces[0].Issues)
	}
}
