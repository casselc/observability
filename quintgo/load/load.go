// Package load reads any of quintgo's trace inputs, detecting the format:
// OTLP/JSON (spans carrying quint.step events), the native step log (JSONL
// of qtrace steps), or a model-level ITF trace.
package load

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/chucklehead-dev/oscope/spike/quintgo/itf"
	"github.com/chucklehead-dev/oscope/spike/quintgo/otelio"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
)

// Format of a trace input.
type Format string

const (
	OTLP    Format = "otlp"  // OTLP/JSON TracesData lines
	StepLog Format = "steps" // native step log (qobs.JSONLSink)
	ITF     Format = "itf"   // model-level ITF (quintgo, or quint run --mbt)
	Unknown Format = ""
)

// Sniff detects the format from the first JSON value.
func Sniff(data []byte) Format {
	var first map[string]json.RawMessage
	if json.NewDecoder(bytes.NewReader(data)).Decode(&first) != nil {
		return Unknown
	}
	switch {
	case first["resourceSpans"] != nil:
		return OTLP
	case first["states"] != nil:
		return ITF
	case first[qtrace.KeyAction] != nil:
		return StepLog
	}
	return Unknown
}

// Input is one file's contents: steps (OTLP, step log) or an ITF trace.
type Input struct {
	Path   string
	Format Format
	Steps  []qtrace.Step
	ITF    *itf.Trace
}

// File reads path. format may be "" or "auto" to detect it.
func File(path string, format Format) (*Input, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if format == "" || format == "auto" {
		format = Sniff(data)
	}
	in := &Input{Path: path, Format: format}
	switch format {
	case OTLP:
		in.Steps, err = otelio.ReadSteps(bytes.NewReader(data))
	case StepLog:
		in.Steps, err = qtrace.ReadJSONL(bytes.NewReader(data))
	case ITF:
		in.ITF, err = itf.Parse(data)
	default:
		if strings.TrimSpace(string(data)) == "" {
			return in, nil // an empty file holds no steps
		}
		return nil, fmt.Errorf("%s: not OTLP/JSON, a step log or ITF", path)
	}
	if err != nil {
		return nil, fmt.Errorf("%s (%s): %w", path, format, err)
	}
	return in, nil
}
