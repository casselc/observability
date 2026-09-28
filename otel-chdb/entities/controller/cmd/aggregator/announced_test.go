package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/entities/controller/internal/ch"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/rid"
)

// The catalog's resources merged with the edges' announcements
// (sql/announced.sql): a resource the controller knows keeps the
// controller's row; one only an edge announced (a pod born and dead inside a
// controller outage) appears from its announcement, uncertain, with the
// controller's cluster key; the evidence view says which is which. Runs
// against ClickHouse (ENT_CH), skipped when it is down.
func TestAnnouncementsMergeIntoResources(t *testing.T) {
	c := ch.New(env("ENT_CH", "http://127.0.0.1:18123"))
	if _, err := c.Query("SELECT 1"); err != nil {
		t.Skipf("no ClickHouse: %v", err)
	}
	id := fmt.Sprintf("%08x", rand.Uint32())
	db, cons := "agg_ann_"+id, "cons_ann_"+id
	for _, d := range []string{db, cons} {
		if _, _, err := c.Exec("CREATE DATABASE "+d, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		defer func() { _, _, _ = c.Exec("DROP DATABASE "+d+" SYNC", nil, nil, nil) }()
	}
	agg, err := os.ReadFile("../../sql/aggregator.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Script(string(agg), db); err != nil {
		t.Fatal(err)
	}
	// The consumer's table, from its own DDL.
	res, err := os.ReadFile("../../../../otap-rs/sql/otel_resources.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Script(strings.ReplaceAll(string(res), "{table}", cons+".otel_resources"), cons); err != nil {
		t.Fatal(err)
	}
	if err := applyAnnounced(c, "../../sql/announced.sql", db, cons+".otel_resources"); err != nil {
		t.Fatal(err)
	}
	covered := func(pod string) map[string]string {
		return map[string]string{"k8s.cluster.uid": "cl-uid", "k8s.cluster.name": "c1", "k8s.namespace.name": "shop",
			"k8s.pod.name": pod, "k8s.pod.uid": "uid-" + pod, "k8s.container.name": "app"}
	}
	known, orphan := covered("known"), covered("orphan")
	kid, oid := rid.ID(known), rid.ID(orphan)
	clusterKey := rid.Key("cluster", "cl-uid")
	t0 := lane.Time(time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC).UnixMilli())
	rec := lane.Record{Level: lane.LResource, Key: kid, Entity: 7, ClusterKey: clusterKey, PodUID: "uid-known", Container: "app",
		Attrs: known, ValidFrom: t0, ObservedAt: t0, EventAt: t0, Writer: "w"}
	b, _ := json.Marshal(rec)
	if _, _, err := c.Exec("INSERT INTO "+db+".records FORMAT JSONEachRow", strings.NewReader(string(b)), nil, nil); err != nil {
		t.Fatal(err)
	}
	ann := func(rid uint64, attrs map[string]string, epoch string, seen string) string {
		a, _ := json.Marshal(attrs)
		return fmt.Sprintf(`{"resource_id":%d,"ResourceAttributes":%s,"signal":"logs","producer_id":"p1","producer_epoch":"%s","batch_id":0,"seen_at":"%s"}`, rid, a, epoch, seen)
	}
	rows := strings.Join([]string{
		ann(kid, known, "E1", "2026-09-28 10:00:05.000000000"),
		ann(oid, orphan, "E1", "2026-09-28 10:01:00.000000000"),
		ann(oid, orphan, "E1", "2026-09-28 10:01:00.000000000"), // a retried statement
		ann(oid, orphan, "E2", "2026-09-28 11:01:00.000000000"), // the next window
	}, "\n")
	if _, _, err := c.Exec("INSERT INTO "+cons+".otel_resources FORMAT JSONEachRow", strings.NewReader(rows), nil, nil); err != nil {
		t.Fatal(err)
	}
	got, err := c.Query("SELECT resource_id, source, cluster_key, pod_uid, container, uncertain, toString(valid_from), toString(valid_to), length(attrs) FROM " + db + ".resources ORDER BY source")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("resources %v", got)
	}
	want := [][]string{
		{fmt.Sprint(oid), "announce", fmt.Sprint(clusterKey), "uid-orphan", "app", "1", "2026-09-28 10:01:00.000", "2026-09-28 13:01:00.000", "6"},
		{fmt.Sprint(kid), "controller", fmt.Sprint(clusterKey), "uid-known", "app", "0", "2026-09-28 10:00:00.000", "2100-01-01 00:00:00.000", "6"},
	}
	for i := range want {
		if strings.Join(got[i], "|") != strings.Join(want[i], "|") {
			t.Errorf("row %d: %v, want %v", i, got[i], want[i])
		}
	}
	ev, err := c.Query("SELECT resource_id, producers, in_controller FROM " + db + ".resource_evidence ORDER BY in_controller")
	if err != nil || len(ev) != 2 || ev[0][0] != fmt.Sprint(oid) || ev[0][2] != "0" || ev[1][2] != "1" {
		t.Fatalf("evidence %v %v", ev, err)
	}
	n, err := c.Query("SELECT announcements FROM " + cons + ".otel_resources_announced WHERE resource_id = " + fmt.Sprint(oid))
	if err != nil || n[0][0] != "2" {
		t.Fatalf("announcements of the orphan %v %v (the retried statement counts once)", n, err)
	}
}
