package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/ch"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"

	"github.com/casselc/observability/otel-chdb/testgate"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// An entity controller may write any key under its own cluster's prefix
// (D18: create-only PUT on {entities}/C/*). The aggregator used to splice
// the key into its ingest_log and lane_progress INSERTs, so a key with a
// quote in it (1) forged an ingest_log row for another cluster (its catalog
// lag then reads as fresh: STPA R-S5, H-5) and (2) failed the lane_progress
// INSERT, which stopped the pass at that object on every run: no cluster
// after it was ingested again (H-3 for the whole fleet, from one cluster's
// credentials, SEC-1). The key is data: it lands verbatim, and cluster c2's
// lane is ingested whatever c1's holds. Runs against ClickHouse and the
// local S3 like TestGapMarksVersionsUncertain.
func TestHostileKeysAreData(t *testing.T) {
	tracetag.Covers(t, "PH,IT", "CAST-24", "H-6", "H-3", "H-5", "R-S5", "R-S7", "SEC-1", "UCA-8")
	chURL := env("ENT_CH", "http://127.0.0.1:18123")
	s3URL := env("ENT_S3", "http://127.0.0.1:18333")
	c := ch.New(chURL)
	if _, err := c.Query("SELECT 1"); err != nil {
		testgate.Skip(t, "clickhouse", "no ClickHouse at %s: %v", chURL, err)
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "otel")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "otelsecret")
	}
	ctx := context.Background()
	s3c, err := lane.NewS3(ctx, s3URL, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("%08x", rand.Uint32())
	db := "agg_inj_" + id
	sql, err := os.ReadFile("../../sql/aggregator.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Exec("CREATE DATABASE "+db, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _, _ = c.Exec("DROP DATABASE "+db+" SYNC", nil, nil, nil) }()
	if err := c.Script(string(sql), db); err != nil {
		t.Fatal(err)
	}
	prefix := "fix2-entities/inj-" + id
	t0 := lane.Time(time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC).UnixMilli())
	rec := func(level string, key, cluster uint64, name string) lane.Record {
		return lane.Record{Level: level, Key: key, Entity: key, ClusterKey: cluster, Name: name,
			Attrs: map[string]string{"k8s.cluster.name": name}, ValidFrom: t0, ObservedAt: t0, EventAt: t0, Writer: "w"}
	}
	body := func(recs ...lane.Record) []byte {
		var raw bytes.Buffer
		zw := gzip.NewWriter(&raw)
		enc := json.NewEncoder(zw)
		for i := range recs {
			_ = enc.Encode(&recs[i])
		}
		_ = zw.Close()
		return raw.Bytes()
	}
	put := func(key string, b []byte) {
		fctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		if _, err := s3c.PutObject(fctx, &s3.PutObjectInput{Bucket: aws.String("otel"), Key: aws.String(key), Body: bytes.NewReader(b)}); err != nil {
			testgate.Skip(t, "s3", "S3 at %s does not take writes: %v", s3URL, err)
		}
	}
	// c1's controller: a well-formed object under a hostile key
	hostile := prefix + "/c1/1-i/000000000001.delta.ndjson.gz','c2','delta',0,0,'2100-01-01 00:00:00.000',0),('x"
	put(hostile, body(rec(lane.LCluster, 107, 7, "c1"), rec(lane.LPod, 1, 7, "p1")))
	// c2's controller, honest, listed after c1
	put(prefix+"/c2/1-i/000000000001.delta.ndjson.gz", body(rec(lane.LCluster, 108, 8, "c2"), rec(lane.LPod, 2, 8, "p2")))

	a := &agg{c: c, s3: s3c, db: db, bucket: "otel", prefix: prefix, margin: time.Minute, progress: map[string]string{}}
	defer cleanup(ctx, t, a, prefix)
	n, err := a.pass(ctx)
	if err != nil || n != 2 {
		t.Errorf("pass: %d objects, %v", n, err)
	}
	rows, err := c.Query("SELECT lower(hex(object)), cluster, toUnixTimestamp64Milli(put_at) > 4000000000000 FROM " + db + ".ingest_log ORDER BY cluster, object")
	if err != nil {
		t.Fatal(err)
	}
	hx := func(s string) string { return hex.EncodeToString([]byte(s)) }
	want := [][]string{{hx(hostile), "c1", "0"}, {hx(prefix + "/c2/1-i/000000000001.delta.ndjson.gz"), "c2", "0"}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Errorf("ingest_log:\n got %q\nwant %q", rows, want)
	}
	rows, err = c.Query("SELECT key FROM " + db + ".versions_final WHERE level = 'pod' ORDER BY key")
	if err != nil || fmt.Sprint(rows) != "[[1] [2]]" {
		t.Errorf("both clusters' pods must be ingested: %v %v", rows, err)
	}
	// a second pass resumes after the hostile object (its progress landed)
	if n, err := a.pass(ctx); err != nil || n != 0 {
		t.Errorf("second pass: %d objects, %v", n, err)
	}
}

// A gap record's times are read as times, never as SQL: one that does not
// parse is skipped, not spliced.
func TestGapTimesAreParsed(t *testing.T) {
	tracetag.Covers(t, "PH", "CAST-24", "H-6", "H-5", "R-S5")
	for _, s := range []string{"2026-09-27 12:00:00.000", "2026-09-27 12:00:00.000') OR 1 OR ('", "", "x"} {
		_, err := gapTime(s)
		if (err == nil) != (s == "2026-09-27 12:00:00.000") {
			t.Errorf("gapTime(%q): %v", s, err)
		}
	}
}
