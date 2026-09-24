package chdbpurego

import (
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSignalRepro is a deliberately adversarial stress test aimed at
// reproducing the crash from issue #30 locally. Skipped unless STRESS=1.
//
// Strategy: mimic GitHub Actions macos-14 conditions as closely as possible:
//   - run with `GOMAXPROCS=3` (the GHA runner has 3 cores allocated);
//   - launch many more goroutines than P (heavy oversubscription → Go runtime
//     fires SIGURG aggressively for async preemption);
//   - recurse with large stack frames so the runtime needs to grow stacks
//     (SIGSEGV on the guard page) frequently;
//   - drive a chdb query on every iteration so libchdb has live work in
//     progress whenever a Go runtime signal lands.
//
// Knobs (env):
//
//	STRESS_DURATION  = duration string (default 60s)
//	STRESS_WORKERS   = explicit worker count (default 16 * GOMAXPROCS)
//	STRESS_FRAME_KB  = per-frame allocation size in KB (default 8)
//	STRESS_DEPTH     = recursion depth before issuing the query (default 32)
func TestSignalRepro(t *testing.T) {
	if os.Getenv("STRESS") != "1" {
		t.Skip("set STRESS=1 to run the adversarial repro")
	}

	duration := envDuration("STRESS_DURATION", 60*time.Second)
	workers := envInt("STRESS_WORKERS", 16*runtime.GOMAXPROCS(0))
	frameKB := envInt("STRESS_FRAME_KB", 8)
	depth := envInt("STRESS_DEPTH", 32)

	t.Logf("GOMAXPROCS=%d workers=%d frame=%dKB depth=%d duration=%s",
		runtime.GOMAXPROCS(0), workers, frameKB, depth, duration)

	conn, err := NewConnectionFromConnString(":memory:")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	var (
		wg       sync.WaitGroup
		queries  atomic.Uint64
		failures atomic.Uint64
		stop     atomic.Bool
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			growAndQuery(conn, depth, frameKB, &queries, &failures, &stop)
		}(i)
	}

	time.Sleep(duration)
	stop.Store(true)
	wg.Wait()

	t.Logf("queries=%d failures=%d (%.0f qps)",
		queries.Load(), failures.Load(),
		float64(queries.Load())/duration.Seconds())
	if failures.Load() != 0 {
		t.Fatalf("%d query failures under repro stress", failures.Load())
	}
}

// growAndQuery recurses with a large stack frame to force Go stack growth via
// SIGSEGV on the guard page, then drives queries in a tight loop. Each leaf
// goroutine alternates work between Go and libchdb-side C++, maximising the
// chance that a runtime signal lands inside libchdb.
func growAndQuery(conn ChdbConn, depth, frameKB int, queries, failures *atomic.Uint64, stop *atomic.Bool) {
	if depth > 0 {
		// allocate a sizable local so the Go stack must grow at this frame
		frame := make([]byte, frameKB*1024)
		_ = frame[0]
		growAndQuery(conn, depth-1, frameKB, queries, failures, stop)
		return
	}
	for !stop.Load() {
		res, err := conn.Query("SELECT 1 + 1", "CSV")
		if err != nil {
			failures.Add(1)
			continue
		}
		if res != nil {
			res.Free()
		}
		queries.Add(1)
	}
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
