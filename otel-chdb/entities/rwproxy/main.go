// rwproxy: a SQL-rewriting proxy between HyperDX and ClickHouse for the
// entities spike's variant c (README.md).
//
//	rwproxy serve   -config rwproxy.json [-listen :18125] [-mode exact|catalog]
//	rwproxy rewrite -config rwproxy.json [-mode ...] < statements.jsonl > rewritten.jsonl
//	rwproxy bench   -config rwproxy.json [-mode ...] [-n 200] statements.jsonl
//
// Statement lines are {"query": ..., "params": {...}, "db": ...}.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: rwproxy serve|rewrite|bench -config FILE ...")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	cfgPath := fs.String("config", "rwproxy.json", "configuration (JSON)")
	listen := fs.String("listen", "", "override listen")
	mode := fs.String("mode", "", "override mode: exact | catalog")
	n := fs.Int("n", 200, "bench: rewrites per statement")
	fs.Parse(os.Args[2:])
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *mode != "" {
		cfg.Mode = *mode
		if err := cfg.init(); err != nil {
			log.Fatal(err)
		}
	}
	switch cmd {
	case "serve":
		if os.Getenv("GOGC") == "" {
			// the heap is a few MB: at the default 100 the collector runs every
			// few dozen requests and its sweeper dominates the proxy's CPU
			debug.SetGCPercent(800)
		}
		s, err := newServer(cfg)
		if err != nil {
			log.Fatal(err)
		}
		if cfg.RefreshCoveredSQL != "" && cfg.RefreshSeconds > 0 {
			if keys, err := s.readKeys(); err == nil {
				cfg.setCovered(keys)
			} else {
				log.Printf("covered keys: %v", err)
			}
			go s.refreshCovered()
		}
		log.Printf("rwproxy %s -> %s, mode %s, %d tables, %d value expressions", cfg.Listen, cfg.Upstream, cfg.Mode, len(cfg.Tables), len(cfg.Values))
		srv := &http.Server{Addr: cfg.Listen, Handler: s, ReadHeaderTimeout: 30 * time.Second}
		log.Fatal(srv.ListenAndServe())
	case "rewrite":
		each(os.Stdin, func(st stmt) {
			t := time.Now()
			r := cfg.Rewrite(st.Query, st.Params, st.DB)
			out := map[string]any{"id": st.ID, "rewritten": r.Rewritten, "reason": r.Reason, "rules": r.Rules, "left": r.Left,
				"err": r.Err, "sql": r.SQL, "us": float64(time.Since(t).Nanoseconds()) / 1e3}
			b, _ := json.Marshal(out)
			fmt.Println(string(b))
		})
	case "bench":
		f, err := os.Open(fs.Arg(0))
		if err != nil {
			log.Fatal(err)
		}
		var sts []stmt
		each(f, func(st stmt) { sts = append(sts, st) })
		bench(cfg, sts, *n)
	default:
		log.Fatalf("unknown command %q", cmd)
	}
}

type stmt struct {
	ID     any               `json:"id"`
	Query  string            `json:"query"`
	Params map[string]string `json:"params"`
	DB     string            `json:"db"`
}

func each(f *os.File, fn func(stmt)) {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var st stmt
		if err := json.Unmarshal(sc.Bytes(), &st); err != nil {
			log.Fatal(err)
		}
		fn(st)
	}
}

// bench: parse + rewrite time per statement, in process (what the proxy
// adds before forwarding), n rounds over the corpus, interleaved.
func bench(cfg *Config, sts []stmt, n int) {
	type acc struct{ ns []int64 }
	byKind := map[string]*acc{"all": {}, "rewritten": {}, "passthrough": {}}
	kind := make([]string, len(sts))
	for i, st := range sts {
		if cfg.Rewrite(st.Query, st.Params, st.DB).Rewritten {
			kind[i] = "rewritten"
		} else {
			kind[i] = "passthrough"
		}
	}
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	t0 := time.Now()
	for round := 0; round < n; round++ {
		for i, st := range sts {
			t := time.Now()
			cfg.Rewrite(st.Query, st.Params, st.DB)
			d := time.Since(t).Nanoseconds()
			byKind["all"].ns = append(byKind["all"].ns, d)
			byKind[kind[i]].ns = append(byKind[kind[i]].ns, d)
		}
	}
	wall := time.Since(t0)
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	out := map[string]any{"statements": len(sts), "rounds": n, "wall_s": wall.Seconds(),
		"alloc_bytes_per_rewrite": float64(m1.TotalAlloc-m0.TotalAlloc) / float64(n*len(sts)),
		"cpu":                     runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0)}
	for k, a := range byKind {
		sort.Slice(a.ns, func(i, j int) bool { return a.ns[i] < a.ns[j] })
		out[k] = percentiles(a.ns)
	}
	b, _ := json.MarshalIndent(out, "", " ")
	fmt.Println(string(b))
}
