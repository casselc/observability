// Package ch is a minimal ClickHouse HTTP client.
package ch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Client struct {
	URL  string // http://localhost:18123
	HTTP *http.Client
}

func New(u string) *Client {
	return &Client{URL: strings.TrimRight(u, "/"), HTTP: &http.Client{Timeout: 5 * time.Minute}}
}

// Summary is X-ClickHouse-Summary.
type Summary struct {
	WrittenRows string `json:"written_rows"`
	ReadRows    string `json:"read_rows"`
}

// Exec runs query with body (may be nil) and settings; returns the response body.
func (c *Client) Exec(query string, body io.Reader, settings map[string]string, header map[string]string) ([]byte, Summary, error) {
	q := url.Values{}
	q.Set("query", query)
	for k, v := range settings {
		q.Set(k, v)
	}
	if body == nil {
		body = http.NoBody
	}
	req, err := http.NewRequest("POST", c.URL+"/?"+q.Encode(), body)
	if err != nil {
		return nil, Summary{}, err
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	if u := os.Getenv("CH_USER"); u != "" {
		req.Header.Set("X-ClickHouse-User", u)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, Summary{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var s Summary
	_ = json.Unmarshal([]byte(resp.Header.Get("X-ClickHouse-Summary")), &s)
	if resp.StatusCode != 200 {
		return nil, s, fmt.Errorf("clickhouse %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
	return b, s, nil
}

// Query returns TSV rows split into fields.
func (c *Client) Query(query string) ([][]string, error) {
	b, _, err := c.Exec(query+" FORMAT TSV", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	var out [][]string
	for _, l := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		if l == "" {
			continue
		}
		out = append(out, strings.Split(l, "\t"))
	}
	return out, nil
}

// Script runs ';\n'-separated statements with {db} replaced, skipping comment lines.
func (c *Client) Script(sql, db string) error {
	var keep []string
	for _, l := range strings.Split(sql, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "--") {
			keep = append(keep, l)
		}
	}
	for _, st := range strings.Split(strings.ReplaceAll(strings.Join(keep, "\n"), "{db}", db), ";\n") {
		st = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(st), ";"))
		if st == "" {
			continue
		}
		if _, _, err := c.Exec(st, nil, nil, nil); err != nil {
			return fmt.Errorf("%s: %w", st[:min(60, len(st))], err)
		}
	}
	return nil
}
