package notify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
)

func TestSendAnswers(t *testing.T) {
	var mu sync.Mutex
	var got []Alert
	var st atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in []Alert
		_ = json.NewDecoder(r.Body).Decode(&in)
		mu.Lock()
		got = in
		mu.Unlock()
		status := int(st.Load())
		if status == -1 {
			time.Sleep(300 * time.Millisecond)
		}
		w.WriteHeader(max(status, 200))
	}))
	defer srv.Close()
	s := New(Config{URL: srv.URL})
	s.hc.Timeout = 100 * time.Millisecond
	n := &engine.Notice{Key: "r/g/1", Labels: map[string]string{"alertname": "r", "alert_episode": "1"}, Annotations: map[string]string{},
		StartsNs: 1e18, EndsNs: 1e18 + 60e9}
	now := time.Unix(1_000_000_000, 0)
	for code, want := range map[int]Answer{200: Acked, 202: Acked, 400: Rejected, 401: Rejected, 429: ServerErr, 500: ServerErr, 503: ServerErr, -1: NoAnswer} {
		st.Store(int32(code))
		if a, _ := s.Send(context.Background(), []engine.Send{{Notice: n, Phase: engine.Firing}}, now); a != want {
			t.Errorf("HTTP %d: %s want %s", code, a, want)
		}
	}
	st.Store(200)
	time.Sleep(300 * time.Millisecond) // the unanswered request's handler finishes
	s.Send(context.Background(), []engine.Send{{Notice: n, Phase: engine.Firing}}, now)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].Labels["alert_episode"] != "1" || got[0].EndsAt != now.Add(4*time.Minute).UTC().Format(time.RFC3339Nano) {
		t.Fatalf("firing payload %+v", got)
	}
	mu.Unlock()
	s.Send(context.Background(), []engine.Send{{Notice: n, Phase: engine.Resolved}}, now)
	mu.Lock()
	if got[0].EndsAt != time.Unix(0, 1e18+60e9).UTC().Format(time.RFC3339Nano) {
		t.Fatalf("resolved payload %+v", got)
	}
}
