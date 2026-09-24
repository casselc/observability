package qtrace

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// KeyTime is the step log's timestamp field (Unix nanoseconds). It is the
// only key of the native step log that is not an OTel attribute key.
const KeyTime = "quint.time_unix_nano"

// Attributes flattens a step into the schema's attribute keys: exactly the
// attributes an OTel span event carries, plus KeyTime. The native step log
// is one such object per line.
func (s *Step) Attributes() map[string]any {
	m := map[string]any{KeyAction: s.Action, KeySeq: int64(s.Seq), KeyRecorder: s.Recorder, KeyOutcome: s.Outcome,
		KeyTime: s.Time.UnixNano()}
	for k, v := range map[string]string{KeyActor: s.Actor, KeyProcess: s.Process, KeyThread: s.Thread, KeyError: s.Error} {
		if v != "" {
			m[k] = v
		}
	}
	for k, v := range s.Args {
		m[PrefixArg+k] = v
	}
	for k, v := range s.Obs {
		m[PrefixObs+k] = v
	}
	for k, v := range s.Order {
		m[PrefixOrder+k] = v
	}
	return m
}

// MarshalLine encodes a step as one step-log line (with the newline).
func (s *Step) MarshalLine() ([]byte, error) {
	b, err := json.Marshal(s.Attributes())
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// WriteJSONL writes steps as a native step log.
func WriteJSONL(w io.Writer, steps []Step) error {
	for i := range steps {
		b, err := steps[i].MarshalLine()
		if err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

// ReadJSONL reads a native step log: one flat JSON object of schema keys
// per line. Blank lines are skipped.
func ReadJSONL(r io.Reader) ([]Step, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var out []Step
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(line))
		dec.UseNumber()
		var raw map[string]any
		if err := dec.Decode(&raw); err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		attrs := map[string]any{}
		for k, v := range raw {
			if num, ok := v.(json.Number); ok {
				if i, err := num.Int64(); err == nil {
					attrs[k] = i
				} else if f, err := num.Float64(); err == nil {
					attrs[k] = f
				}
				continue
			}
			attrs[k] = v
		}
		var ts time.Time
		if ns, ok := attrs[KeyTime].(int64); ok {
			ts = time.Unix(0, ns)
		}
		delete(attrs, KeyTime)
		s, err := FromAttributes(ts, attrs)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		s.Source = fmt.Sprintf("line %d", n)
		out = append(out, s)
	}
	return out, sc.Err()
}
