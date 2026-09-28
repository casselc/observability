// Package catalog reads the entity catalog (entities/README.md): the
// resource ids of a caller's clusters and namespaces, for tables scoped by
// resource_id, and the catalog's lag per cluster from the aggregator's
// ingest_log (STPA R-S5).
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
)

// Config names the catalog's tables.
type Config struct {
	Database      string `json:"database"`
	Resources     string `json:"resources"`  // default "resources"
	IngestLog     string `json:"ingest_log"` // default "ingest_log"
	ClusterAttr   string `json:"cluster_attr"`
	NamespaceAttr string `json:"namespace_attr"`
	RefreshS      int    `json:"refresh_s"`
	// LagMaxS: a cluster whose newest ingested entity object is older than
	// this is "lagging" (the controller syncs every 10 min by default).
	LagMaxS int `json:"lag_max_s"`
	// MaxResourceIDs bounds one caller's id set.
	MaxResourceIDs int `json:"max_resource_ids"`
}

// Querier runs internal statements.
type Querier interface {
	Query(ctx context.Context, sql string, settings url.Values) ([]byte, central.Summary, error)
}

// Catalog caches the answers.
type Catalog struct {
	cfg    Config
	q      Querier
	now    func() time.Time
	policy *sqlscope.Policy

	mu  sync.Mutex
	ids map[string]idsEntry
	lag *lagEntry
}

type idsEntry struct {
	ids []uint64
	at  time.Time
}

type lagEntry struct {
	at       time.Time
	err      string
	clusters map[string]ClusterLag
}

// New returns a Catalog, or nil when cfg names no database.
func New(cfg Config, q Querier) (*Catalog, error) {
	if cfg.Database == "" {
		return nil, nil
	}
	if cfg.Resources == "" {
		cfg.Resources = "resources"
	}
	if cfg.IngestLog == "" {
		cfg.IngestLog = "ingest_log"
	}
	if cfg.ClusterAttr == "" {
		cfg.ClusterAttr = "k8s.cluster.name"
	}
	if cfg.NamespaceAttr == "" {
		cfg.NamespaceAttr = "k8s.namespace.name"
	}
	if cfg.RefreshS <= 0 {
		cfg.RefreshS = 30
	}
	if cfg.LagMaxS <= 0 {
		cfg.LagMaxS = 900
	}
	if cfg.MaxResourceIDs <= 0 {
		cfg.MaxResourceIDs = 200_000
	}
	// the catalog's own read goes through the same predicate builder
	p, err := sqlscope.NewPolicy(cfg.Database, []*sqlscope.Table{{
		Name: cfg.Resources, Scope: "columns",
		Cluster:   "attrs[" + sqlscope.Quote(cfg.ClusterAttr) + "]",
		Namespace: "attrs[" + sqlscope.Quote(cfg.NamespaceAttr) + "]",
	}}, 0)
	if err != nil {
		return nil, err
	}
	return &Catalog{cfg: cfg, q: q, now: time.Now, policy: p, ids: map[string]idsEntry{}}, nil
}

func internalSettings(filters string) url.Values {
	v := url.Values{}
	v.Set("default_format", "JSONCompact")
	v.Set("max_execution_time", "20")
	v.Set("wait_end_of_query", "1")
	for _, m := range central.OverflowModes {
		v.Set(m, "throw")
	}
	if filters != "" {
		v.Set("additional_table_filters", filters)
	}
	return v
}

// ResourceIDs are the ids of every resource in the caller's scope that the
// catalog knows (any lifetime).
func (c *Catalog) ResourceIDs(ctx context.Context, s sqlscope.Scope) ([]uint64, error) {
	key := fmt.Sprintf("%v|%v|%v|%v", s.AllClusters, s.Clusters, s.AllNamespaces, s.Namespaces)
	c.mu.Lock()
	if e, ok := c.ids[key]; ok && c.now().Sub(e.at) < time.Duration(c.cfg.RefreshS)*time.Second {
		c.mu.Unlock()
		return e.ids, nil
	}
	c.mu.Unlock()
	pr, err := c.policy.Prepare("SELECT DISTINCT resource_id FROM " + c.cfg.Resources + " LIMIT " + strconv.Itoa(c.cfg.MaxResourceIDs+1))
	if err != nil {
		return nil, err
	}
	s.ResourceIDs, s.Window = nil, nil
	res, err := pr.Finish(s)
	if err != nil {
		return nil, err
	}
	body, _, err := c.q.Query(ctx, res.SQL, internalSettings(res.FiltersSetting()))
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	var out struct {
		Data [][]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	if len(out.Data) > c.cfg.MaxResourceIDs {
		return nil, fmt.Errorf("catalog: more than %d resources in scope", c.cfg.MaxResourceIDs)
	}
	ids := make([]uint64, 0, len(out.Data))
	for _, row := range out.Data {
		var s string
		if err := json.Unmarshal(row[0], &s); err != nil {
			var n uint64
			if err := json.Unmarshal(row[0], &n); err != nil {
				return nil, fmt.Errorf("catalog: resource_id %s", row[0])
			}
			ids = append(ids, n)
			continue
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("catalog: resource_id %q", s)
		}
		ids = append(ids, n)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	c.mu.Lock()
	c.ids[key] = idsEntry{ids: ids, at: c.now()}
	c.mu.Unlock()
	return ids, nil
}

// ClusterLag is one cluster's catalog freshness.
type ClusterLag struct {
	LastPut      string  `json:"last_put"`      // the newest entity object ingested (its PUT time)
	LastIngested string  `json:"last_ingested"` // when the aggregator last ingested one
	LagS         float64 `json:"lag_s"`         // now − last_put
	Status       string  `json:"status"`        // ok | lagging
}

// Info is the catalog part of a result's label (R-S5).
type Info struct {
	Status   string                `json:"status"` // ok | lagging | missing | unavailable | not_configured
	Clusters map[string]ClusterLag `json:"clusters,omitempty"`
	AsOf     string                `json:"as_of,omitempty"`
	Error    string                `json:"error,omitempty"`
	Note     string                `json:"note,omitempty"`
}

// Lag reports the catalog's freshness for the clusters the caller may see.
func (c *Catalog) Lag(ctx context.Context, may func(string) bool, want []string) Info {
	if c == nil {
		return Info{Status: "not_configured", Note: "no entity catalog is configured: catalog lag (R-S5) is not reported"}
	}
	c.mu.Lock()
	e := c.lag
	fresh := e != nil && c.now().Sub(e.at) < time.Duration(c.cfg.RefreshS)*time.Second
	c.mu.Unlock()
	if !fresh {
		e = c.readLag(ctx)
		c.mu.Lock()
		c.lag = e
		c.mu.Unlock()
	}
	info := Info{AsOf: e.at.UTC().Format(time.RFC3339Nano)}
	if e.err != "" {
		info.Status, info.Error = "unavailable", e.err
		info.Note = "the catalog's lag could not be read: rows may lack entity attributes, and catalog-scoped reads may miss new resources"
		return info
	}
	info.Status = "ok"
	info.Clusters = map[string]ClusterLag{}
	for _, cl := range want {
		if !may(cl) {
			continue
		}
		l, ok := e.clusters[cl]
		if !ok {
			info.Status = "missing"
			info.Clusters[cl] = ClusterLag{Status: "missing"}
			continue
		}
		if l.Status != "ok" && info.Status == "ok" {
			info.Status = "lagging"
		}
		info.Clusters[cl] = l
	}
	if want == nil { // every cluster the caller may see
		for cl, l := range e.clusters {
			if may(cl) {
				info.Clusters[cl] = l
				if l.Status != "ok" && info.Status == "ok" {
					info.Status = "lagging"
				}
			}
		}
	}
	if info.Status != "ok" {
		info.Note = "the entity catalog is behind for some clusters: new resources may be missing from filters and group-bys"
	}
	return info
}

func (c *Catalog) readLag(ctx context.Context) *lagEntry {
	now := c.now()
	e := &lagEntry{at: now, clusters: map[string]ClusterLag{}}
	sql := "SELECT cluster, toUnixTimestamp64Milli(max(put_at)), toUnixTimestamp64Milli(max(ingested_at)) FROM " +
		c.cfg.Database + "." + c.cfg.IngestLog + " GROUP BY cluster"
	body, _, err := c.q.Query(ctx, sql, internalSettings(""))
	if err != nil {
		e.err = err.Error()
		return e
	}
	var out struct {
		Data [][]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		e.err = err.Error()
		return e
	}
	for _, row := range out.Data {
		var cl string
		_ = json.Unmarshal(row[0], &cl)
		put, ing := ms(row[1]), ms(row[2])
		lag := now.Sub(time.UnixMilli(put)).Seconds()
		st := "ok"
		if lag > float64(c.cfg.LagMaxS) {
			st = "lagging"
		}
		e.clusters[cl] = ClusterLag{LastPut: time.UnixMilli(put).UTC().Format(time.RFC3339Nano),
			LastIngested: time.UnixMilli(ing).UTC().Format(time.RFC3339Nano), LagS: lag, Status: st}
	}
	return e
}

func ms(r json.RawMessage) int64 {
	s := strings.Trim(string(r), `"`)
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// SetClock replaces the clock (tests).
func (c *Catalog) SetClock(now func() time.Time) { c.now = now }
