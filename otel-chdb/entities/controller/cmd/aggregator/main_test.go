package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/ch"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
)

// A gap record marks what opened or closed inside the gap as uncertain
// (AMBIGUITY.md X1), in its cluster only, without changing the merge: a
// relist found pod A's deletion (closed at the relist time, inside the gap)
// and pod C born in the gap; B closed before the gap, D is still open, and E
// is another cluster's close in the same window. Runs against ClickHouse
// (ENT_CH, default :18123) and the local S3 (ENT_S3, default :18333, bucket
// otel), skipped when either is down.
func TestGapMarksVersionsUncertain(t *testing.T) {
	chURL := env("ENT_CH", "http://127.0.0.1:18123")
	s3URL := env("ENT_S3", "http://127.0.0.1:18333")
	c := ch.New(chURL)
	if _, err := c.Query("SELECT 1"); err != nil {
		t.Skipf("no ClickHouse at %s: %v", chURL, err)
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
	db := "agg_gap_" + id
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
	prefix := "fix2-entities/" + id
	w := &lane.Writer{S3: s3c, Bucket: "otel", Lane: prefix + "/c1/1-i", PutTimeout: 5 * time.Second}

	t0 := lane.Time(time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC).UnixMilli())
	mn := lane.Time(60_000)
	gapFrom, gapTo := t0, t0+5*mn
	pod := func(cluster, key uint64, from, closed lane.Time) lane.Record {
		obs := from
		if closed > obs {
			obs = closed
		}
		return lane.Record{Level: lane.LPod, Key: key, Entity: key, ClusterKey: cluster, Name: fmt.Sprintf("p%d", key),
			Attrs: map[string]string{"k8s.pod.name": fmt.Sprintf("p%d", key)}, ValidFrom: from, ClosedAt: closed,
			ObservedAt: obs, EventAt: from, Writer: "w"}
	}
	w.Add(
		pod(7, 1, t0-60*mn, gapTo-mn), // A: its deletion found by the relist
		pod(7, 2, t0-60*mn, t0-30*mn), // B: closed before the gap
		pod(7, 3, t0+2*mn, 0),         // C: born in the gap
		pod(7, 4, t0-60*mn, 0),        // D: open throughout
		pod(8, 5, t0-60*mn, gapTo-mn), // E: another cluster
	)
	// The lane writer retries until it lands; a store that refuses writes
	// (SeaweedFS out of volumes) skips the test instead of hanging it.
	fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := w.Flush(fctx); err != nil {
		t.Skipf("S3 at %s does not take writes: %v", s3URL, err)
	}
	w.Add(lane.Record{Level: lane.LGap, Key: 99, Entity: 98, ClusterKey: 7, Kind: "pods", Name: "relist",
		Attrs: map[string]string{"k8s.resource": "pods"}, ValidFrom: gapFrom, ClosedAt: gapTo, ObservedAt: gapTo,
		EventAt: gapFrom + mn, Writer: "w"})
	if err := w.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	a := &agg{c: c, s3: s3c, db: db, bucket: "otel", prefix: prefix, margin: time.Minute, progress: map[string]string{}}
	n, err := a.pass(ctx)
	if err != nil || n != 2 {
		t.Fatalf("pass: %d objects, %v", n, err)
	}
	state := func() map[string]string {
		rows, err := c.Query("SELECT key, uncertain, toUnixTimestamp64Milli(valid_from), toUnixTimestamp64Milli(closed_at), toUnixTimestamp64Milli(last_observed) FROM " + db + ".versions_final WHERE level = 'pod' ORDER BY key")
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]string{}
		for _, r := range rows {
			m[r[0]] = fmt.Sprint(r[1:])
		}
		return m
	}
	got := state()
	want := map[string]string{
		"1": fmt.Sprint([]string{"1", fmt.Sprint(t0 - 60*mn), fmt.Sprint(gapTo - mn), fmt.Sprint(gapTo - mn)}),
		"2": fmt.Sprint([]string{"0", fmt.Sprint(t0 - 60*mn), fmt.Sprint(t0 - 30*mn), fmt.Sprint(t0 - 30*mn)}),
		"3": fmt.Sprint([]string{"1", fmt.Sprint(t0 + 2*mn), "0", fmt.Sprint(t0 + 2*mn)}),
		"4": fmt.Sprint([]string{"0", fmt.Sprint(t0 - 60*mn), "0", fmt.Sprint(t0 - 60*mn)}),
		"5": fmt.Sprint([]string{"0", fmt.Sprint(t0 - 60*mn), fmt.Sprint(gapTo - mn), fmt.Sprint(gapTo - mn)}),
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("pod %s: %s, want %s (uncertain, valid_from, closed_at, last_observed)", k, got[k], v)
		}
	}
	// the catalog views carry it; the gap is a view of its own
	rows, err := c.Query("SELECT pod_key FROM " + db + ".pods WHERE uncertain = 1 ORDER BY pod_key")
	if err != nil || fmt.Sprint(rows) != "[[1] [3]]" {
		t.Fatalf("pods view: %v %v", rows, err)
	}
	rows, err = c.Query("SELECT resource, reason, toUnixTimestamp64Milli(gap_from), toUnixTimestamp64Milli(gap_to) FROM " + db + ".gaps")
	if err != nil || fmt.Sprint(rows) != fmt.Sprint([][]string{{"pods", "relist", fmt.Sprint(gapFrom), fmt.Sprint(gapTo)}}) {
		t.Fatalf("gaps view: %v %v", rows, err)
	}
	// A replayed lane (the aggregator restarted without its progress):
	// nothing changes.
	a.progress = map[string]string{}
	if _, err := a.pass(ctx); err != nil {
		t.Fatal(err)
	}
	if again := state(); fmt.Sprint(again) != fmt.Sprint(got) {
		t.Fatalf("replay changed the versions:\n%v\n%v", again, got)
	}
	// A later reopen (the controller sees A again) still merges as before:
	// the latest observation decides, and the mark stays.
	late := pod(7, 1, t0-60*mn, 0)
	late.ObservedAt = gapTo + 10*mn
	w2 := &lane.Writer{S3: s3c, Bucket: "otel", Lane: prefix + "/c1/2-i", PutTimeout: 5 * time.Second}
	w2.Add(late)
	if err := w2.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.pass(ctx); err != nil {
		t.Fatal(err)
	}
	if got := state()["1"]; got != fmt.Sprint([]string{"1", fmt.Sprint(t0 - 60*mn), "0", fmt.Sprint(gapTo + 10*mn)}) {
		t.Fatalf("pod 1 after reopening: %s", got)
	}
	cleanup(ctx, t, a, prefix)
}

func cleanup(ctx context.Context, t *testing.T, a *agg, prefix string) {
	_, objs, err := a.list(ctx, prefix+"/", "", "")
	if err != nil {
		t.Log(err)
		return
	}
	for _, o := range objs {
		_, _ = a.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &a.bucket, Key: &o.key})
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
