// otlpsend posts OTLP/HTTP protobuf requests read from files, the way a
// collector's exporter with a persistent queue and retry_on_failure
// (max_elapsed_time: 0) behaves: each request is resent, with identical
// bytes, until it gets a 2xx; a non-retryable status (OTLP/HTTP: anything
// but 429, 502, 503, 504) drops it. It prints one JSON line per request (attempts,
// latency to the final 2xx) and a summary.
//
//	otlpsend -url http://127.0.0.1:4318 -signal traces -file traces-testgen-10000.pb -n 30 [-timeout 5s]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

type reqResult struct {
	Seq      int     `json:"seq"`
	File     string  `json:"file"`
	Attempts int     `json:"attempts"`
	SentNS   int64   `json:"sent_ns"`
	AckMS    float64 `json:"ack_ms"`
	LastErr  string  `json:"last_err,omitempty"`
	Dropped  bool    `json:"dropped,omitempty"`
}

func main() {
	url := flag.String("url", "http://127.0.0.1:4318", "OTLP/HTTP base URL")
	signal := flag.String("signal", "traces", "traces or logs")
	files := flag.String("file", "", "comma-separated .pb files, sent round-robin")
	n := flag.Int("n", 1, "requests to send")
	timeout := flag.Duration("timeout", 30*time.Second, "per-attempt HTTP timeout")
	backoff := flag.Duration("backoff", 200*time.Millisecond, "wait between attempts")
	interval := flag.Duration("interval", 0, "wait between requests")
	quiet := flag.Bool("quiet", false, "only the summary")
	flag.Parse()
	var bodies [][]byte
	var names []string
	for _, f := range strings.Split(*files, ",") {
		b, err := os.ReadFile(f)
		if err != nil {
			log.Fatal(err)
		}
		bodies = append(bodies, b)
		names = append(names, f)
	}
	cl := &http.Client{Timeout: *timeout}
	endpoint := strings.TrimRight(*url, "/") + "/v1/" + *signal
	var lat []float64
	attempts := 0
	start := time.Now()
	for i := 0; i < *n; i++ {
		b := bodies[i%len(bodies)]
		r := reqResult{Seq: i, File: names[i%len(names)], SentNS: time.Now().UnixNano()}
		t0 := time.Now()
		for {
			r.Attempts++
			resp, err := cl.Post(endpoint, "application/x-protobuf", bytes.NewReader(b))
			if err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode/100 == 2 {
					break
				}
				err = fmt.Errorf("HTTP %d: %.200s", resp.StatusCode, body)
				// OTLP/HTTP: only 429, 502, 503 and 504 are retryable.
				if c := resp.StatusCode; c != 429 && c != 502 && c != 503 && c != 504 {
					r.LastErr, r.Dropped = err.Error(), true
					log.Printf("request %d attempt %d: %v (permanent: dropped)", i, r.Attempts, err)
					break
				}
			}
			r.LastErr = err.Error()
			log.Printf("request %d attempt %d: %v", i, r.Attempts, err)
			time.Sleep(*backoff)
		}
		r.AckMS = float64(time.Since(t0).Microseconds()) / 1000
		lat = append(lat, r.AckMS)
		attempts += r.Attempts
		if !*quiet {
			j, _ := json.Marshal(r)
			fmt.Println(string(j))
		}
		time.Sleep(*interval)
	}
	sort.Float64s(lat)
	sum := map[string]any{"summary": true, "requests": *n, "attempts": attempts,
		"median_ack_ms": lat[len(lat)/2], "min_ack_ms": lat[0], "max_ack_ms": lat[len(lat)-1],
		"elapsed_s": time.Since(start).Seconds()}
	j, _ := json.Marshal(sum)
	fmt.Println(string(j))
}
