package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

// Config is the proxy's configuration (JSON). scripts/config.py writes it
// from the catalog.
type Config struct {
	Listen   string `json:"listen"`   // host:port the proxy serves
	Upstream string `json:"upstream"` // ClickHouse's HTTP interface, e.g. http://127.0.0.1:18123

	// Mode picks the filter rewrite:
	//   exact    if(mapContains(Residual, k), Residual[k] OP v, resource_id IN (catalog lookup)):
	//            the ALIAS column's own semantics, whatever the catalog knows
	//   catalog  resource_id IN (catalog lookup) alone for keys the catalog
	//            covers: a primary-key condition on the variant-c sort key,
	//            exact once every resource the rows name is in the catalog
	Mode string `json:"mode"`

	// Tables whose ResourceAttributes is the catalog ALIAS ("db.table").
	Tables []string `json:"tables"`

	Column     string `json:"column"`      // the ALIAS map column: ResourceAttributes
	Residual   string `json:"residual"`    // ResourceResidual
	ResourceID string `json:"resource_id"` // resource_id
	KV         string `json:"kv"`          // the catalog's (Key, Value, resource_id) table: rw_cat.resource_kv

	// Values: per covered key, the catalog's value for one resource, with
	// {rid} for the resource id column and '' when the catalog doesn't know
	// the resource or the key. A covered key without an entry is left to the
	// ALIAS column (correct, slow).
	Values map[string]string `json:"values"`

	// Covered: every key the catalog can hold (the edge's covered key list).
	// A key outside it lives only in the residual. RefreshCoveredSQL, if
	// set, re-reads the list from ClickHouse every RefreshSeconds.
	Covered           []string `json:"covered"`
	RefreshCoveredSQL string   `json:"refresh_covered_sql"`
	RefreshSeconds    int      `json:"refresh_seconds"`
	RefreshUser       string   `json:"refresh_user"`
	RefreshPassword   string   `json:"refresh_password"`

	// Materialized maps column names that stand for a resource key (e.g.
	// "__hdx_materialized_k8s.pod.name": "k8s.pod.name"). Variant c has none;
	// a table that kept them would be rewritten through them too.
	Materialized map[string]string `json:"materialized"`

	// Fallback: when a rewritten statement fails before any byte of the
	// response is sent, re-send the original statement.
	Fallback bool `json:"fallback"`

	Log string `json:"log"` // JSON lines, one per statement; "" = none

	tables  map[string]bool
	mu      sync.RWMutex
	covered map[string]bool
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &Config{}
	if err := json.Unmarshal(b, c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return c, c.init()
}

func (c *Config) init() error {
	if c.Column == "" {
		c.Column = "ResourceAttributes"
	}
	if c.Residual == "" {
		c.Residual = "ResourceResidual"
	}
	if c.ResourceID == "" {
		c.ResourceID = "resource_id"
	}
	if c.Mode == "" {
		c.Mode = "exact"
	}
	if c.Mode != "exact" && c.Mode != "catalog" {
		return fmt.Errorf("mode %q: exact or catalog", c.Mode)
	}
	if c.KV == "" {
		return fmt.Errorf("kv: the catalog's key-value table is required")
	}
	c.tables = map[string]bool{}
	for _, t := range c.Tables {
		if !strings.Contains(t, ".") {
			return fmt.Errorf("table %q: want db.table", t)
		}
		c.tables[t] = true
	}
	c.setCovered(c.Covered)
	return nil
}

func (c *Config) setCovered(keys []string) {
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		m[k] = true
	}
	c.mu.Lock()
	c.covered = m
	c.mu.Unlock()
}

func (c *Config) isCovered(k string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.covered[k]
}
