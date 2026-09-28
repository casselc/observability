package lane

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// PreviousEnd finds the latest earlier lane of the cluster (not our own, not
// a later one) and the time its last object landed.
func TestPreviousEnd(t *testing.T) {
	t1 := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(5 * time.Minute)
	lanes := map[string][]time.Time{ // lane -> its objects' LastModified
		"1000-a": {t1.Add(-time.Hour)},
		"2000-b": {t1, t2, t1.Add(time.Minute)},
		"2500-e": {},                      // no object yet
		"3000-c": {t2.Add(time.Hour)},     // ours
		"4000-d": {t2.Add(2 * time.Hour)}, // a later incarnation (clock skew, a second instance): not a predecessor
		"junk":   {t2.Add(3 * time.Hour)},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		prefix := q.Get("prefix")
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>b</Name><IsTruncated>false</IsTruncated>`)
		if q.Get("delimiter") == "/" {
			for l := range lanes {
				fmt.Fprintf(&b, "<CommonPrefixes><Prefix>%s%s/</Prefix></CommonPrefixes>", prefix, l)
			}
		} else {
			l := strings.TrimSuffix(strings.TrimPrefix(prefix, "lanes/c1/"), "/")
			for i, m := range lanes[l] {
				fmt.Fprintf(&b, "<Contents><Key>%s%012d.delta.ndjson.gz</Key><LastModified>%s</LastModified><Size>1</Size></Contents>", prefix, i+1, m.Format(time.RFC3339))
			}
		}
		b.WriteString(`</ListBucketResult>`)
		rw.Header().Set("Content-Type", "application/xml")
		_, _ = rw.Write([]byte(b.String()))
	}))
	defer srv.Close()
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	c, err := NewS3(context.Background(), srv.URL, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := PreviousEnd(context.Background(), c, "b", "lanes/c1", "3000-c")
	if err != nil {
		t.Fatal(err)
	}
	// 2500-e is the latest predecessor, and has no object: its start
	if got != 2500 {
		t.Fatalf("got %d, want 2500 (the empty lane's epoch)", got)
	}
	delete(lanes, "2500-e")
	if got, err = PreviousEnd(context.Background(), c, "b", "lanes/c1", "3000-c"); err != nil || got != FromTime(t2) {
		t.Fatalf("got %d, %v; want %d (2000-b's last object)", got, err, FromTime(t2))
	}
	if got, err = PreviousEnd(context.Background(), c, "b", "lanes/c1", "0500-z"); err != nil || got != 0 {
		t.Fatalf("first incarnation: got %d, %v; want 0", got, err)
	}
}
