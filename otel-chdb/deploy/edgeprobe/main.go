// edgeprobe is the publishers' readiness check for a wedged durable buffer
// (deploy/README.md §Durable buffer): a publisher whose buffer can't take
// writes must leave the Service's endpoints, so the agents send elsewhere
// and an alert on the pod's readiness fires. Neither collector can say so
// itself: the Rust engine's /api/v1/readyz knows memory pressure and the
// pipeline phase only, and the Go collector's health_check reports the
// pipeline up. So the kubelet runs this as an exec readiness probe in the
// publisher container (a static binary, stdlib only, copied into both
// images), in place of the httpGet probe it wraps (-ready: the engine's
// /api/v1/readyz, the collector's health_check). Not ready when any holds:
//
//   - -ready does not answer 2xx (what the probe checked before);
//   - the buffer's filesystem has less than -min-free bytes available
//     (statfs of -dir): the volume filled before the buffer's own cap, the
//     case that wedged a publisher on kind (a WAL write, a segment flush,
//     even the progress file that releases committed segments need room);
//   - a buffer is at its cap: max over the series of -used / -cap, read
//     from the publisher's Prometheus text endpoint (-metrics), is at least
//     -max-fill. Rust: the durable buffer's storage_bytes_used_bytes /
//     storage_bytes_cap_bytes; Go: otelcol_exporter_queue_size /
//     otelcol_exporter_queue_capacity. A failed scrape is not ready too.
//
// Both are states, not events, so the pod comes back by itself once S3
// drains the buffer (or the volume is grown), and a pod that gets no
// traffic while not ready does not stay not ready for want of a write.
//
//	edgeprobe -ready http://127.0.0.1:8080/api/v1/readyz -dir /var/lib/otap-s3pq/buffer -min-free 1Gi \
//	  -metrics http://127.0.0.1:8080/api/v1/metrics -used storage_bytes_used_bytes \
//	  -cap storage_bytes_cap_bytes -match otel_scope_name=processor.durable_buffer -max-fill 0.95
//
// Exit 0 ready, 1 not ready (the reason on stdout, which the kubelet puts
// in the pod's events), 2 bad usage.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type labels []string

func (l *labels) String() string     { return strings.Join(*l, ",") }
func (l *labels) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	os.Exit(run(os.Args[1:], os.Stdout))
}

func run(args []string, out io.Writer) int {
	fs := flag.NewFlagSet("edgeprobe", flag.ContinueOnError)
	dir := fs.String("dir", "", "the buffer's directory (statfs)")
	minFree := fs.String("min-free", "0", "not ready below this many bytes available (Ki/Mi/Gi suffixes)")
	url := fs.String("metrics", "", "the publisher's Prometheus text endpoint")
	used := fs.String("used", "", "metric: the buffer's current size")
	capName := fs.String("cap", "", "metric: the buffer's cap, with the same labels as -used")
	maxFill := fs.Float64("max-fill", 0.95, "not ready at or above this -used/-cap ratio")
	ready := fs.String("ready", "", "a readiness URL that must answer 2xx")
	timeout := fs.Duration("timeout", 2*time.Second, "timeout of each HTTP request")
	var match labels
	fs.Var(&match, "match", "only series with this label=value (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	free, err := parseBytes(*minFree)
	if err != nil || (*dir == "" && *url == "" && *ready == "") || (*url != "" && (*used == "" || *capName == "")) {
		fmt.Fprintln(out, "usage: edgeprobe [-ready URL] [-dir D [-min-free N]] [-metrics URL -used M -cap M [-match k=v] [-max-fill R]]")
		return 2
	}
	var notReady []string
	var info []string
	if *ready != "" {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		if err := get2xx(ctx, *ready); err != nil {
			notReady = append(notReady, err.Error())
		}
	}
	if *dir != "" {
		avail, err := available(*dir)
		switch {
		case err != nil:
			notReady = append(notReady, fmt.Sprintf("statfs %s: %v", *dir, err))
		case avail < free:
			notReady = append(notReady, fmt.Sprintf("buffer volume full: %d bytes available < %d", avail, free))
		default:
			info = append(info, fmt.Sprintf("available=%d", avail))
		}
	}
	if *url != "" {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		fill, err := scrapeFill(ctx, *url, *used, *capName, match)
		switch {
		case err != nil:
			notReady = append(notReady, err.Error())
		case fill >= *maxFill:
			notReady = append(notReady, fmt.Sprintf("buffer at its cap: %s/%s = %.3f >= %.3f", *used, *capName, fill, *maxFill))
		default:
			info = append(info, fmt.Sprintf("fill=%.3f", fill))
		}
	}
	if len(notReady) > 0 {
		fmt.Fprintln(out, "not ready: "+strings.Join(notReady, "; "))
		return 1
	}
	fmt.Fprintln(out, "ready: "+strings.Join(info, " "))
	return 0
}

func get2xx(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("readiness: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		return fmt.Errorf("readiness: %s %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

func available(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}

func parseBytes(s string) (uint64, error) {
	mult := uint64(1)
	for suf, m := range map[string]uint64{"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30} {
		if strings.HasSuffix(s, suf) {
			s, mult = strings.TrimSuffix(s, suf), m
			break
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n * mult, err
}

func scrapeFill(ctx context.Context, url, used, capName string, match []string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scrape: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("scrape: %s", resp.Status)
	}
	return fill(resp.Body, used, capName, match)
}

// fill is the highest used/cap over the series pairs with equal labels.
func fill(r io.Reader, used, capName string, match []string) (float64, error) {
	u, c := map[string]float64{}, map[string]float64{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, lbls, val, ok := parseLine(line)
		if !ok || (name != used && name != capName) || !matches(lbls, match) {
			continue
		}
		key := strings.Join(lbls, ",")
		if name == used {
			u[key] = val
		} else {
			c[key] = val
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	best, pairs := 0.0, 0
	for k, v := range u {
		cv, ok := c[k]
		if !ok || cv <= 0 {
			continue
		}
		pairs++
		if f := v / cv; f > best {
			best = f
		}
	}
	if pairs == 0 {
		return 0, errors.New("scrape: no " + used + "/" + capName + " series pair")
	}
	return best, nil
}

// parseLine splits `name{k="v",...} value [timestamp]`; the labels come
// back sorted as k="v" strings.
func parseLine(line string) (string, []string, float64, bool) {
	var name, rest string
	var lbls []string
	if i := strings.IndexByte(line, '{'); i >= 0 {
		j := strings.LastIndexByte(line, '}')
		if j < i {
			return "", nil, 0, false
		}
		name, rest = line[:i], strings.TrimSpace(line[j+1:])
		lbls = splitLabels(line[i+1 : j])
	} else {
		f := strings.Fields(line)
		if len(f) < 2 {
			return "", nil, 0, false
		}
		name, rest = f[0], strings.Join(f[1:], " ")
	}
	f := strings.Fields(rest)
	if len(f) == 0 {
		return "", nil, 0, false
	}
	v, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return "", nil, 0, false
	}
	sort.Strings(lbls)
	return name, lbls, v, true
}

func splitLabels(s string) []string {
	var out []string
	var cur strings.Builder
	inQ, esc := false, false
	for _, r := range s {
		switch {
		case esc:
			esc = false
		case r == '\\':
			esc = true
		case r == '"':
			inQ = !inQ
		case r == ',' && !inQ:
			if t := strings.TrimSpace(cur.String()); t != "" {
				out = append(out, t)
			}
			cur.Reset()
			continue
		}
		cur.WriteRune(r)
	}
	if t := strings.TrimSpace(cur.String()); t != "" {
		out = append(out, t)
	}
	return out
}

func matches(lbls, match []string) bool {
	for _, m := range match {
		k, v, _ := strings.Cut(m, "=")
		want := k + `="` + v + `"`
		found := false
		for _, l := range lbls {
			if l == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
