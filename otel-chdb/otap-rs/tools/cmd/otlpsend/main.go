// otlpsend posts OTLP/HTTP protobuf requests read from files, the way a
// collector's exporter with a persistent queue and retry_on_failure
// (max_elapsed_time: 0) behaves: each request is resent, with identical
// bytes, until it gets a 2xx; a non-retryable status (OTLP/HTTP: anything
// but 429, 502, 503, 504) drops it. It prints one JSON line per request (attempts,
// latency to the final 2xx) and a summary.
//
//	otlpsend -url http://127.0.0.1:4318 -signal traces -file traces-testgen-10000.pb -n 30 [-timeout 5s]
//	otlpsend -grpc 127.0.0.1:4317 ...   the same over OTLP/gRPC (unary Export); retryable:
//	                                    Unavailable, ResourceExhausted, Aborted, DeadlineExceeded
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
	"os"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// grpcSender sends request i as a gRPC Export call. The requests are
// unmarshalled once, up front, so the latency is the call's alone.
func grpcSender(addr, signal string, timeout time.Duration, bodies [][]byte) func(int) (bool, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(256<<20)))
	if err != nil {
		log.Fatal(err)
	}
	var calls []func(context.Context) error
	for _, b := range bodies {
		switch signal {
		case "traces":
			r := ptraceotlp.NewExportRequest()
			if err := r.UnmarshalProto(b); err != nil {
				log.Fatal(err)
			}
			c := ptraceotlp.NewGRPCClient(conn)
			calls = append(calls, func(ctx context.Context) error { _, e := c.Export(ctx, r); return e })
		case "logs":
			r := plogotlp.NewExportRequest()
			if err := r.UnmarshalProto(b); err != nil {
				log.Fatal(err)
			}
			c := plogotlp.NewGRPCClient(conn)
			calls = append(calls, func(ctx context.Context) error { _, e := c.Export(ctx, r); return e })
		default:
			r := pmetricotlp.NewExportRequest()
			if err := r.UnmarshalProto(b); err != nil {
				log.Fatal(err)
			}
			c := pmetricotlp.NewGRPCClient(conn)
			calls = append(calls, func(ctx context.Context) error { _, e := c.Export(ctx, r); return e })
		}
	}
	return func(i int) (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		err := calls[i](ctx)
		if err == nil {
			return false, nil
		}
		switch status.Code(err) {
		case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
			return true, err
		}
		return false, err
	}
}

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
	grpcAddr := flag.String("grpc", "", "send over OTLP/gRPC to this address instead of HTTP")
	flag.Parse()
	var viaGRPC func(int) (bool, error)
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
	if *grpcAddr != "" {
		viaGRPC = grpcSender(*grpcAddr, *signal, *timeout, bodies)
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
			if viaGRPC != nil {
				retry, err := viaGRPC(i % len(bodies))
				if err == nil {
					break
				}
				r.LastErr = err.Error()
				if !retry {
					r.Dropped = true
					log.Printf("request %d attempt %d: %v (permanent: dropped)", i, r.Attempts, err)
					break
				}
				log.Printf("request %d attempt %d: %v", i, r.Attempts, err)
				time.Sleep(*backoff)
				continue
			}
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
