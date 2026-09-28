// Command kvrollupmigrate gives an existing central otel_logs / otel_traces
// key-value rollup its cluster column (internal/rollupmig, DECISIONS.md
// D33), so the query service can serve it to cluster-restricted callers.
//
//	kvrollupmigrate -ddl otap-rs/sql/otel_logs.sql -table otel.otel_logs -url http://clickhouse:8123 [-user u] (password in CH_PASSWORD)
//
// Stop every consumer replica first; start them again afterwards.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/rollupmig"
)

type conn struct {
	url, user, password string
	hc                  *http.Client
}

func (c *conn) do(ctx context.Context, sql string, settings map[string]string) ([]byte, error) {
	q := url.Values{}
	for k, v := range settings {
		q.Set(k, v)
	}
	q.Set("wait_end_of_query", "1")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/?"+q.Encode(), strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	if c.user != "" {
		req.Header.Set("X-ClickHouse-User", c.user)
		req.Header.Set("X-ClickHouse-Key", c.password)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", strings.SplitN(sql, "\n", 2)[0], bytes.TrimSpace(b))
	}
	return b, nil
}

func (c *conn) Exec(ctx context.Context, sql string, settings map[string]string) error {
	_, err := c.do(ctx, sql, settings)
	return err
}

func (c *conn) Rows(ctx context.Context, sql string) ([]string, error) {
	b, err := c.do(ctx, sql+" FORMAT JSONCompactColumns", nil)
	if err != nil {
		return nil, err
	}
	var cols [][]string
	if err := json.Unmarshal(b, &cols); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, nil
	}
	return cols[0], nil
}

func main() {
	ddl := flag.String("ddl", "", "the table's DDL file (otap-rs/sql/otel_logs.sql or otel_traces.sql)")
	table := flag.String("table", "", "the table, database.table (otel.otel_logs)")
	u := flag.String("url", "http://127.0.0.1:8123", "ClickHouse's HTTP interface")
	user := flag.String("user", "", "ClickHouse user (password in CH_PASSWORD)")
	flag.Parse()
	p, err := rollupmig.NewPlan(*ddl, *table)
	if err != nil {
		log.Fatal(err)
	}
	c := &conn{url: strings.TrimRight(*u, "/"), user: *user, password: os.Getenv("CH_PASSWORD"), hc: &http.Client{Timeout: time.Hour}}
	r, err := rollupmig.Migrate(context.Background(), c, p)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: altered %v; rebuilt %d days %v; kept without cluster %v\n", p.Rollup, r.Altered, len(r.Days), r.Days, r.Kept)
}
