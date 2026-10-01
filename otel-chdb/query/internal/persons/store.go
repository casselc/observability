package persons

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/central"
)

// Config names the person events table (D32 `person`; written by the
// aggregator from the person controller's lane and by the steward).
type Config struct {
	Database string `json:"database"`
	Table    string `json:"table"`  // default "person_events"
	Tenant   string `json:"tenant"` // the Entra tenant (single tenant, O-E1)
	MaxOIDs  int    `json:"max_oids"`
}

// Querier runs a statement (central.Client).
type Querier interface {
	Query(ctx context.Context, sql string, settings url.Values) ([]byte, central.Summary, error)
}

// Store reads and writes the person events table.
type Store struct {
	cfg Config
	q   Querier
}

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// NewStore checks cfg; nil, nil when no database is configured.
func NewStore(cfg Config, q Querier) (*Store, error) {
	if cfg.Database == "" {
		return nil, nil
	}
	if cfg.Table == "" {
		cfg.Table = "person_events"
	}
	if cfg.MaxOIDs <= 0 {
		cfg.MaxOIDs = 100
	}
	if !identRE.MatchString(cfg.Database) || !identRE.MatchString(cfg.Table) {
		return nil, fmt.Errorf("persons: database and table must be plain identifiers")
	}
	if _, err := NormaliseOID(cfg.Tenant); err != nil {
		return nil, fmt.Errorf("persons: tenant: %w", err)
	}
	cfg.Tenant = strings.ToLower(cfg.Tenant)
	return &Store{cfg: cfg, q: q}, nil
}

// Config is the store's configuration with defaults applied.
func (s *Store) Config() Config { return s.cfg }

func (s *Store) fqn() string { return s.cfg.Database + "." + s.cfg.Table }

// Schema is the table's DDL. system_from is the store's arrival time (D32:
// the aggregator's put_at), never the writer's clock, so arrival order is
// system_from order and the first correction stays first. The
// deduplication window makes a retried insert with the same
// insert_deduplication_token one row.
func (s *Store) Schema() string {
	return "CREATE TABLE IF NOT EXISTS " + s.fqn() + ` (
  tenant LowCardinality(String),
  oid String,
  cluster LowCardinality(String) DEFAULT '',
  namespace LowCardinality(String) DEFAULT '',
  valid_from Int64,
  valid_to Int64 DEFAULT 9223372036854775807,
  system_from Int64 DEFAULT toUnixTimestamp64Milli(now64(3)),
  seq UInt64,
  source LowCardinality(String),
  kind LowCardinality(String),
  name String DEFAULT '',
  reason String DEFAULT ''
) ENGINE = MergeTree ORDER BY (tenant, oid, system_from, seq)
SETTINGS non_replicated_deduplication_window = 10000`
}

func settings() url.Values {
	v := url.Values{}
	v.Set("default_format", "JSONCompact")
	v.Set("max_execution_time", "20")
	v.Set("wait_end_of_query", "1")
	return v
}

func lit(s string) string { return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'" }

// Events reads every event of oids (normalised GUIDs) in the store's tenant.
func (s *Store) Events(ctx context.Context, oids []string) ([]Event, error) {
	if len(oids) == 0 {
		return nil, nil
	}
	if len(oids) > s.cfg.MaxOIDs {
		return nil, fmt.Errorf("persons: at most %d oids per request", s.cfg.MaxOIDs)
	}
	in := make([]string, len(oids))
	for i, o := range oids {
		n, err := NormaliseOID(o)
		if err != nil {
			return nil, err
		}
		in[i] = lit(n)
	}
	sql := "SELECT tenant, oid, cluster, namespace, valid_from, valid_to, system_from, seq, source, kind, name FROM " + s.fqn() +
		" WHERE tenant = " + lit(s.cfg.Tenant) + " AND oid IN (" + strings.Join(in, ", ") + ") ORDER BY system_from, seq"
	body, _, err := s.q.Query(ctx, sql, settings())
	if err != nil {
		return nil, fmt.Errorf("persons: %w", err)
	}
	var out struct {
		Data [][]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("persons: %w", err)
	}
	evs := make([]Event, 0, len(out.Data))
	for _, r := range out.Data {
		if len(r) != 11 {
			return nil, fmt.Errorf("persons: row of %d columns", len(r))
		}
		e := Event{Tenant: str(r[0]), OID: str(r[1]), Cluster: str(r[2]), Namespace: str(r[3]),
			ValidFrom: num(r[4]), ValidTo: num(r[5]), SystemFrom: num(r[6]), Seq: uint64(num(r[7])),
			Source: str(r[8]), Kind: str(r[9]), Name: str(r[10])}
		evs = append(evs, e)
	}
	return evs, nil
}

func str(r json.RawMessage) string {
	var s string
	_ = json.Unmarshal(r, &s)
	return s
}

func num(r json.RawMessage) int64 {
	n, _ := strconv.ParseInt(strings.Trim(string(r), `"`), 10, 64)
	return n
}

// DedupToken is the one insert_deduplication_token of a departure signal:
// the same signal, retried, is one row.
func DedupToken(tenant, oid string) string {
	return "pseudonymise/" + strings.ToLower(tenant) + "/" + strings.ToLower(oid)
}

// insertCorrection writes the correction (one row; system_from by the store).
func (s *Store) insertCorrection(ctx context.Context, oid, pseudonym, reason string) error {
	h := fnv.New64a()
	h.Write([]byte(DedupToken(s.cfg.Tenant, oid)))
	sql := "INSERT INTO " + s.fqn() + " (tenant, oid, valid_from, valid_to, seq, source, kind, name, reason) VALUES (" +
		strings.Join([]string{lit(s.cfg.Tenant), lit(oid), "-9223372036854775808", strconv.FormatInt(Open, 10),
			strconv.FormatUint(h.Sum64(), 10), lit(SrcSteward), lit(KindPseudonymise), lit(pseudonym), lit(reason)}, ", ") + ")"
	v := url.Values{}
	v.Set("insert_deduplication_token", DedupToken(s.cfg.Tenant, oid))
	v.Set("insert_deduplicate", "1")
	v.Set("wait_end_of_query", "1")
	_, _, err := s.q.Query(ctx, sql, v)
	return err
}

// CreateTable creates the table if it is missing (tests, `personctl init`).
func (s *Store) CreateTable(ctx context.Context) error {
	_, _, err := s.q.Query(ctx, s.Schema(), url.Values{})
	return err
}

// Outcome of a departure signal.
type Outcome struct {
	OID           string `json:"oid"`
	Pseudonym     string `json:"pseudonym"`        // the stored first correction's: what every answer shows
	At            int64  `json:"pseudonymised_at"` // its system time (ms)
	Already       bool   `json:"already"`          // the oid was pseudonymised before this run
	HiddenName    string `json:"-"`                // the name it hides (for the operator's confirmation only)
	Attempts      int    `json:"attempts"`
	KeyedDiffered bool   `json:"keyed_differed,omitempty"` // a different pseudonym was stored first (another key)
}

// ErrUnknown: the correction could not be confirmed stored; running the
// same signal again is safe.
var ErrUnknown = errors.New("persons: the correction is not confirmed stored (no answer and not read back); run the same command again")

// Pseudonymise records the departure of oid (O-G9; AMBIGUITY G6): read
// first, and stop if a correction is stored; else insert the correction
// with the signal's dedup token and read back. A failed or unanswered
// insert is never taken as "not applied" (CAST 50, 74): it is re-read, and
// the same insert is retried (the token makes it one row; a duplicate that
// lands anyway changes no answer, the first correction wins). key derives
// the pseudonym; reason is required (the offboarding ticket).
func (s *Store) Pseudonymise(ctx context.Context, oid string, key []byte, reason string, attempts int, backoff time.Duration) (Outcome, error) {
	oid, err := NormaliseOID(oid)
	if err != nil {
		return Outcome{}, err
	}
	if strings.TrimSpace(reason) == "" {
		return Outcome{}, errors.New("persons: a reason (the offboarding ticket) is required")
	}
	if len(key) < 16 {
		return Outcome{}, errors.New("persons: the pseudonym key must be at least 16 bytes")
	}
	if attempts <= 0 {
		attempts = 3
	}
	want := Pseudonym(key, s.cfg.Tenant, oid)
	out := Outcome{OID: oid}
	stored := func() (bool, error) {
		evs, err := s.Events(ctx, []string{oid})
		if err != nil {
			return false, err
		}
		var first *Event
		for i := range evs {
			e := &evs[i]
			if e.Source == SrcSteward && e.Kind == KindPseudonymise && e.Name != "" {
				if first == nil || e.SystemFrom < first.SystemFrom || (e.SystemFrom == first.SystemFrom && e.Seq < first.Seq) {
					first = e
				}
			} else if e.Source == SrcController && e.Kind == KindAssert && e.Name != "" {
				out.HiddenName = e.Name // the latest stored name (arrival order)
			}
		}
		if first == nil {
			return false, nil
		}
		out.Pseudonym, out.At, out.KeyedDiffered = first.Name, first.SystemFrom, first.Name != want
		return true, nil
	}
	ok, err := stored()
	if err != nil {
		return out, err
	}
	if ok {
		out.Already = true
		return out, nil
	}
	var last error
	for out.Attempts < attempts {
		out.Attempts++
		last = s.insertCorrection(ctx, oid, want, reason)
		// whatever the insert said, the store decides
		ok, err := stored()
		if err == nil && ok {
			return out, nil
		}
		if err != nil {
			last = errors.Join(last, err)
		}
		if out.Attempts < attempts && backoff > 0 {
			select {
			case <-ctx.Done():
				return out, errors.Join(ErrUnknown, ctx.Err())
			case <-time.After(backoff):
			}
		}
	}
	return out, errors.Join(ErrUnknown, last)
}
