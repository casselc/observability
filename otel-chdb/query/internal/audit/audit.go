// Package audit records every access decision (STPA R-S8): one JSON line per
// decision, appended, and an interface for shipping the lines elsewhere.
package audit

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Record is one decision, or the outcome of an allowed request.
type Record struct {
	Time      time.Time `json:"ts"`
	RequestID string    `json:"request_id"`
	// Event: "decision" (written before anything runs; if it cannot be
	// written, nothing runs) or "outcome" (after).
	Event    string `json:"event"`
	Action   string `json:"action"`   // query | plan
	Decision string `json:"decision"` // allow | deny
	Reason   string `json:"reason,omitempty"`
	Detail   string `json:"detail,omitempty"`

	Subject    string   `json:"sub,omitempty"`
	Groups     []string `json:"groups,omitempty"`
	Roles      []string `json:"roles,omitempty"`
	Clusters   []string `json:"clusters,omitempty"` // the scope applied; ["*"] for all
	Namespaces []string `json:"namespaces,omitempty"`
	ClientIP   string   `json:"client_ip,omitempty"`

	// query
	QueryHash string   `json:"query_hash,omitempty"` // sha256 of the rebuilt SQL and its filters
	InputHash string   `json:"input_hash,omitempty"` // sha256 of the SQL as sent
	SQL       string   `json:"sql,omitempty"`        // the rebuilt SQL, truncated
	Tables    []string `json:"tables,omitempty"`
	Filters   int      `json:"filters,omitempty"`

	// plan
	Signal      string   `json:"signal,omitempty"`
	Objects     int      `json:"objects,omitempty"`
	Bytes       int64    `json:"bytes,omitempty"`
	ObjectsHash string   `json:"objects_hash,omitempty"` // sha256 of the planned keys, in order
	Keys        []string `json:"keys,omitempty"`         // the first keys planned
	ExpiresAt   string   `json:"expires_at,omitempty"`

	// outcome
	Status    int     `json:"status,omitempty"`
	Rows      int64   `json:"rows,omitempty"`
	RowsRead  int64   `json:"rows_read,omitempty"`
	BytesRead int64   `json:"bytes_read,omitempty"`
	ElapsedMs float64 `json:"elapsed_ms,omitempty"`
	Error     string  `json:"error,omitempty"`
}

// Sink takes records. Write must not return before the record is durable to
// the sink's standard; an error means the record is not recorded.
type Sink interface {
	Write(Record) error
	Close() error
}

// Shipper sends records to another store (a SIEM, an object store, a
// separate audit service). It is fed from the file, never instead of it.
type Shipper interface {
	Ship(ctx context.Context, batch []Record) error
}

// File appends JSON lines to a file opened O_APPEND, one write per record,
// and fsyncs each (Sync: true) or leaves it to the OS.
type File struct {
	mu   sync.Mutex
	f    *os.File
	sync bool
	ship chan Record
}

// OpenFile opens (creating, 0600) path for appending.
func OpenFile(path string, sync bool) (*File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}
	return &File{f: f, sync: sync}, nil
}

// Write appends one line.
func (a *File) Write(r Record) error {
	if r.Time.IsZero() {
		r.Time = time.Now().UTC()
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f == nil {
		return errors.New("audit: closed")
	}
	if _, err := a.f.Write(b); err != nil {
		return fmt.Errorf("audit: %w", err)
	}
	if a.sync {
		if err := a.f.Sync(); err != nil {
			return fmt.Errorf("audit: %w", err)
		}
	}
	if a.ship != nil {
		select {
		case a.ship <- r:
		default: // the shipper is behind; the file has it (ReadFrom can replay)
		}
	}
	return nil
}

// Close closes the file.
func (a *File) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f == nil {
		return nil
	}
	err := a.f.Close()
	a.f = nil
	if a.ship != nil {
		close(a.ship)
		a.ship = nil
	}
	return err
}

// StartShipping forwards written records to s in batches, retrying a failed
// batch with backoff. The file stays the record of truth: a shipper that
// falls behind drops from its queue, not from the file (ReadFrom replays).
// onErr is called for each failed attempt.
func (a *File) StartShipping(ctx context.Context, s Shipper, batch int, every time.Duration, onErr func(error)) {
	ch := make(chan Record, 10*batch)
	a.mu.Lock()
	a.ship = ch
	a.mu.Unlock()
	go func() {
		var buf []Record
		t := time.NewTicker(every)
		defer t.Stop()
		flush := func() {
			for backoff := time.Second; len(buf) > 0; backoff *= 2 {
				if err := s.Ship(ctx, buf); err == nil {
					buf = buf[:0]
					return
				} else if onErr != nil {
					onErr(err)
				}
				if backoff > time.Minute || ctx.Err() != nil {
					return // keep buf; the next flush tries again
				}
				time.Sleep(backoff)
			}
		}
		for {
			select {
			case r, ok := <-ch:
				if !ok {
					flush()
					return
				}
				buf = append(buf, r)
				if len(buf) >= batch {
					flush()
				}
			case <-t.C:
				flush()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// ReadFrom reads records back (tests, replay to a shipper).
func ReadFrom(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return out, err
		}
		out = append(out, r)
	}
	return out, sc.Err()
}

// Memory is a Sink for tests; Fail makes writes fail.
type Memory struct {
	mu      sync.Mutex
	Records []Record
	Fail    error
}

// Write records r.
func (m *Memory) Write(r Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail != nil {
		return m.Fail
	}
	m.Records = append(m.Records, r)
	return nil
}

// Close does nothing.
func (m *Memory) Close() error { return nil }

// Snapshot copies the records.
func (m *Memory) Snapshot() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Record(nil), m.Records...)
}
