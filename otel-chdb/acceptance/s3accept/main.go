// s3accept checks whether an S3 store (AWS S3, Nutanix Objects, SeaweedFS,
// MinIO, ...) holds the assumptions the otel-chdb design makes of it:
// atomic create-only and If-Match writes, resolvable ambiguous outcomes,
// user metadata on HEAD, consistent reads and LIST StartAfter, 404 on free
// slots, the SDKs' default checksums, and request latency at the object
// sizes the exporters write. It also reports which credential source is in
// use (the `creds` mode exercises each deployment credential mode).
//
//	s3accept [run]  --url s3://bucket/prefix | --bucket B [--prefix P] [--endpoint URL] ...
//	s3accept creds  (same connection flags) [--hold 65m]
//	s3accept cleanup (same connection flags)   deletes everything under the prefix
//
// See ../RUNBOOK.md.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

type Params struct {
	RaceWriters   int           `json:"race_writers"`
	RaceRounds    int           `json:"race_rounds"`
	CASWriters    int           `json:"cas_writers"`
	CASIncrements int           `json:"cas_increments"`
	AmbiguousN    int           `json:"ambiguous_n"`
	Settle        time.Duration `json:"settle_ns"`
	MPUWriters    int           `json:"mpu_writers"`
	MPURounds     int           `json:"mpu_rounds"`
	ConsistencyN  int           `json:"consistency_n"`
	ListLagMax    time.Duration `json:"list_lag_max_ns"`
	PerfOps       int           `json:"perf_ops"`
	PerfConc      int           `json:"perf_concurrency"`
	PerfSizes     []int         `json:"perf_sizes"`
	PerfWarnP99   time.Duration `json:"perf_warn_p99_ns"`
}

var all = []string{"control-plane", "inline-consumer", "exporter-data"}

func checks() []Check {
	return []Check{
		{ID: "identity", Title: "credential source and caller identity", Level: Required, Needs: all, Run: checkIdentity,
			Plan: func(Params) string {
				return "resolve credentials through the default chain (no S3 request); STS GetCallerIdentity on AWS or with --sts-endpoint"
			}},
		{ID: "create-only", Title: "PUT If-None-Match:* (create-only), incl. identical retry", Level: Required, Needs: []string{"control-plane", "inline-consumer"}, Run: checkCreateOnly,
			Plan: func(Params) string {
				return "3 PUT If-None-Match:* on one key (new, other body, identical retry), 1 GET"
			}},
		{ID: "plain-put", Title: "unconditional PUT (bucket policy check)", Level: Recommended, Needs: []string{"exporter-data"}, Run: checkPlainPut,
			Plan: func(Params) string { return "2 unconditional PUTs (new key, overwrite)" }},
		{ID: "if-match", Title: "PUT If-Match CAS", Level: Required, Needs: []string{"control-plane", "inline-consumer"}, Run: checkIfMatch,
			Plan: func(Params) string {
				return "PUT; PUT If-Match current, stale, made-up, on a missing key, unquoted; 1 GET"
			}},
		{ID: "cond-delete", Title: "DELETE If-Match", Level: Recommended, Run: checkCondDelete,
			Plan: func(Params) string { return "2 PUT, DELETE If-Match stale/current/* on missing, 2 HEAD, 1 GET" }},
		{ID: "create-race", Title: "N concurrent create-only writers on one key", Level: Required, Needs: []string{"control-plane", "inline-consumer"}, Run: checkCreateRace,
			Plan: func(p Params) string {
				return fmt.Sprintf("%d rounds × %d concurrent PUT If-None-Match:* on one key per round (%d PUTs), 1 GET per round", p.RaceRounds, p.RaceWriters, p.RaceRounds*p.RaceWriters)
			}},
		{ID: "cas-race", Title: "concurrent read-CAS increments (lost-update detection)", Level: Required, Needs: []string{"control-plane", "inline-consumer"}, Run: checkCASRace,
			Plan: func(p Params) string {
				return fmt.Sprintf("%d writers × %d successful GET + PUT If-Match increments of one counter (plus refused attempts)", p.CASWriters, p.CASIncrements)
			}},
		{ID: "cas-vs-delete", Title: "CAS racing a conditional DELETE", Level: Info, Run: checkCASvsDelete,
			Plan: func(Params) string { return "20 rounds: PUT, then PUT If-Match ∥ DELETE If-Match on the same ETag" }},
		{ID: "ambiguous-create", Title: "create-only PUT cancelled mid-request, then resolved", Level: Required, Needs: []string{"control-plane"}, Run: checkAmbiguousCreate,
			Plan: func(p Params) string {
				return fmt.Sprintf("%d × (PUT If-None-Match:* cancelled once written, HEAD, wait %s, identical PUT, GET on 412)", p.AmbiguousN, p.Settle)
			}},
		{ID: "ambiguous-cas", Title: "If-Match PUT cancelled mid-request, then resolved", Level: Required, Needs: []string{"control-plane", "inline-consumer"}, Run: checkAmbiguousCAS,
			Plan: func(p Params) string {
				return fmt.Sprintf("%d × (PUT If-Match cancelled once written, wait %s, identical PUT If-Match, GET on 412)", p.AmbiguousN, p.Settle)
			}},
		{ID: "mpu-race", Title: "conditional CompleteMultipartUpload (not used by the design)", Level: Info, Run: checkMPU,
			Plan: func(p Params) string {
				return fmt.Sprintf("2 sequential single-part uploads completed If-None-Match:*; %d rounds × %d concurrent completions; AbortMultipartUpload for every loser", p.MPURounds, p.MPUWriters)
			}},
		{ID: "metadata", Title: "user metadata round trip on HEAD/GET", Level: Required, Needs: []string{"inline-consumer"}, Run: checkMetadata,
			Plan: func(Params) string {
				return "PUT If-None-Match:* with 9 x-amz-meta-* headers; a losing create; HEAD; GET"
			}},
		{ID: "read-after-write", Title: "read-after-create/overwrite/delete", Level: Required, Needs: []string{"control-plane", "inline-consumer"}, Run: checkRAW,
			Plan: func(p Params) string {
				return fmt.Sprintf("%d × (PUT If-None-Match:*, GET, HEAD, PUT overwrite, GET, DELETE, GET)", p.ConsistencyN)
			}},
		{ID: "list", Title: "LIST-after-write, StartAfter order, pagination, delimiter", Level: Required, Needs: []string{"inline-consumer"}, Run: checkList,
			Plan: func(p Params) string {
				return fmt.Sprintf("%d × (PUT, LIST until visible ≤ %s); 16 slot PUTs; 6 StartAfter LISTs; a MaxKeys=3 paginated LIST; a delimiter LIST", p.ConsistencyN, p.ListLagMax)
			}},
		{ID: "head-missing", Title: "HEAD/GET of a missing key: 404, not 403", Level: Required, Needs: []string{"control-plane", "inline-consumer"}, Run: checkHeadMissing,
			Plan: func(Params) string { return "HEAD and GET of a key never written" }},
		{ID: "checksum", Title: "SDK default CRC32 (header and trailer), when_required, DeleteObjects", Level: Recommended, Needs: []string{"exporter-data"}, Run: checkChecksum,
			Plan: func(Params) string {
				return "5 PUTs of 100 KB (default CRC32, streamed trailer, CRC32C, SHA256, when_required), 1 GET with validation, 2 DeleteObjects"
			}},
		{ID: "bucket-config", Title: "versioning, lifecycle, bucket policy condition keys", Level: Info, Run: checkBucketConfig,
			Plan: func(Params) string {
				return "GetBucketVersioning, GetBucketLifecycleConfiguration, GetBucketPolicy (403s are reported, not failures)"
			}},
		{ID: "clock", Title: "store clock vs local clock", Level: Info, Needs: []string{"exporter-data"}, Run: checkClock,
			Plan: func(Params) string { return "no extra request: the Date header of every response" }},
		{ID: "perf", Title: "latency and throughput at the exporters' object sizes", Level: Info, Run: checkPerf,
			Plan: func(p Params) string {
				var s []string
				for _, z := range p.PerfSizes {
					s = append(s, fmtSize(z))
				}
				total := 0
				for _, z := range p.PerfSizes {
					total += z * p.PerfOps
				}
				return fmt.Sprintf("per size %v: %d PUT If-None-Match:*, %d GET, %d HEAD at concurrency %d (%.0f MB written); "+
					"%d LIST; %d 200-byte slot PUTs, %d CAS, %d HEAD-404", s, p.PerfOps, p.PerfOps, p.PerfOps, p.PerfConc,
					float64(total)/1e6, max(10, p.PerfOps/4), p.PerfOps, p.PerfOps, p.PerfOps)
			}},
		{ID: "cleanup", Title: "delete this run's prefix", Level: Recommended, Run: nil,
			Plan: func(Params) string {
				return "LIST the run prefix, DeleteObjects (or DELETE one by one), ListMultipartUploads + Abort"
			}},
	}
}

func main() {
	mode := "run"
	args := os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		mode, args = args[0], args[1:]
	}
	switch mode {
	case "run", "creds", "cleanup":
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (run | creds | cleanup)\n", mode)
		os.Exit(2)
	}
	fs := flag.NewFlagSet("s3accept "+mode, flag.ExitOnError)
	o := &Opts{}
	fs.StringVar(&o.URL, "url", "", "s3://bucket/prefix or https://host/bucket/prefix (as the exporters' url); overrides --bucket/--prefix/--endpoint")
	fs.StringVar(&o.Bucket, "bucket", "", "bucket")
	fs.StringVar(&o.Prefix, "prefix", "otel-accept", "key prefix; each run writes under {prefix}/{run-id}/")
	fs.StringVar(&o.Endpoint, "endpoint", "", "S3-compatible endpoint (Nutanix Objects, SeaweedFS); empty = AWS (or AWS_ENDPOINT_URL_S3)")
	fs.StringVar(&o.Region, "region", "", "region (default: AWS_REGION / profile; us-east-1 for a custom endpoint)")
	fs.StringVar(&o.Profile, "profile", "", "shared-config profile (AWS_PROFILE also works): static keys, credential_process, role chains")
	fs.StringVar(&o.CABundle, "ca-bundle", "", "PEM file added to the system roots (AWS_CA_BUNDLE also works)")
	fs.StringVar(&o.AccessKey, "access-key-id", "", "static key (override; the default chain reads AWS_ACCESS_KEY_ID itself)")
	fs.StringVar(&o.SecretKey, "secret-access-key", "", "static secret")
	fs.StringVar(&o.RoleARN, "role-arn", "", "STS AssumeRole on top of the resolved credentials (parquetgo RoleARN, otap-rs role_arn)")
	fs.StringVar(&o.STSEndpoint, "sts-endpoint", "", "STS endpoint for GetCallerIdentity / AssumeRole (AWS_ENDPOINT_URL_STS also works)")
	fs.StringVar(&o.PathStyle, "path-style", "auto", "auto (path-style iff --endpoint) | true | false (virtual-hosted)")
	fs.StringVar(&o.Store, "store", "", "aws | nutanix | seaweedfs | other: label for the report (default from the endpoint)")
	fs.DurationVar(&o.Timeout, "timeout", 30*time.Second, "per-request timeout")
	raceEPs := fs.String("race-endpoints", "", "comma-separated extra endpoints of the SAME store for the race checks, as URL or URL=IP (connect to IP, keep URL's host for TLS and signing; e.g. one entry per Nutanix Objects client IP)")
	dry := fs.Bool("dry-run", false, "print what would be done and send nothing")
	out := fs.String("out", "", "JSON report path (default s3accept-{mode}-{store}-{run}.json)")
	verbose := fs.Bool("v", false, "print every check's evidence lines")
	only := fs.String("only", "", "comma-separated check ids to run (default all)")
	skip := fs.String("skip", "", "comma-separated check ids to skip")
	keep := fs.Bool("keep", false, "do not delete this run's objects")
	yes := fs.Bool("yes", false, "cleanup: do not ask (required when the prefix does not contain 'accept')")
	p := Params{}
	fs.IntVar(&p.RaceWriters, "race-writers", 16, "create-race: concurrent writers per round")
	fs.IntVar(&p.RaceRounds, "race-rounds", 20, "create-race: rounds")
	fs.IntVar(&p.CASWriters, "cas-writers", 8, "cas-race: writers")
	fs.IntVar(&p.CASIncrements, "cas-increments", 25, "cas-race: successful increments per writer")
	fs.IntVar(&p.AmbiguousN, "ambiguous", 20, "ambiguous-*: attempts")
	fs.DurationVar(&p.Settle, "settle", 200*time.Millisecond, "ambiguous-*: wait before the retry")
	fs.IntVar(&p.MPUWriters, "mpu-writers", 4, "mpu-race: concurrent completions per round")
	fs.IntVar(&p.MPURounds, "mpu-rounds", 3, "mpu-race: rounds")
	fs.IntVar(&p.ConsistencyN, "consistency", 30, "read-after-write / list: keys")
	fs.DurationVar(&p.ListLagMax, "list-lag-max", 10*time.Second, "list: give up waiting for a key after this")
	fs.IntVar(&p.PerfOps, "perf-ops", 200, "perf: operations per op type and size")
	fs.IntVar(&p.PerfConc, "perf-concurrency", 8, "perf: concurrent requests (an exporter uses 1-8)")
	sizes := fs.String("perf-sizes", "100KB,1MB", "perf: object sizes (KB = 1000, MB = 1e6, MiB = 2^20)")
	fs.DurationVar(&p.PerfWarnP99, "perf-warn-p99", 2*time.Second, "perf: WARN when an op's p99 exceeds this")
	// creds mode
	modes := fs.String("modes", "auto", "creds: comma-separated modes (static,profile,irsa,pod-identity,imds,chain) or auto")
	hold := fs.Duration("hold", 0, "creds: after the checks, keep making a request every --every for this long (spans a real refresh)")
	every := fs.Duration("every", 30*time.Second, "creds: interval for --hold")
	sample := fs.Bool("leave-sample", false, "creds: leave {prefix}/creds/sample.tsv for the ClickHouse keyless s3() check")
	fs.Parse(args)

	var err error
	if p.PerfSizes, err = parseSizes(*sizes); err != nil {
		fatal(err)
	}
	for _, ep := range strings.Split(*raceEPs, ",") {
		if ep = strings.TrimSpace(ep); ep != "" {
			o.RaceEndpoints = append(o.RaceEndpoints, ep)
		}
	}
	if err := o.resolve(); err != nil {
		fatal(err)
	}
	run := time.Now().UTC().Format("20060102T150405") + "-" + randHex(3)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *out == "" {
		*out = fmt.Sprintf("s3accept-%s-%s-%s.json", mode, o.Store, run)
	}

	switch mode {
	case "creds":
		os.Exit(runCreds(ctx, o, run, *dry, *modes, *hold, *every, *sample, *out, *verbose))
	case "cleanup":
		if !strings.Contains(o.Prefix, "accept") && !*yes {
			fatal(fmt.Errorf("refusing to delete everything under %q without --yes", o.Prefix))
		}
		if *dry {
			fmt.Printf("would delete every object and abort every upload under s3://%s/%s/\n", o.Bucket, o.Prefix)
			return
		}
		e, err := newEnv(ctx, o)
		if err != nil {
			fatal(err)
		}
		r := &Result{ID: "cleanup"}
		cleanupPrefix(ctx, e, o.Prefix+"/", r)
		fmt.Println(r.Status, r.Summary)
		for _, l := range r.Evidence {
			fmt.Println("  ", l)
		}
		return
	}

	sel := selectChecks(*only, *skip)
	if *dry {
		dryRun(o, p, run, sel, *keep, *out)
		return
	}
	e, err := newEnv(ctx, o)
	if err != nil {
		fatal(err)
	}
	e.Run, e.Root, e.Params, e.Verbose = run, o.Prefix+"/"+run, p, *verbose
	rep := &Report{Tool: "s3accept", Mode: "run", Started: time.Now().UTC(), Store: o.Store, Target: target(o, e.Root), Params: p}
	fmt.Printf("s3accept: store %s, s3://%s/%s/ (endpoint %s, %s), run %s\n", o.Store, o.Bucket, e.Root,
		orDefault(o.Endpoint, "AWS"), map[bool]string{true: "path-style", false: "virtual-hosted"}[o.pathStyle()], run)
	for _, c := range sel {
		if c.Run == nil {
			continue
		}
		if ctx.Err() != nil {
			break
		}
		r := &Result{ID: c.ID, Title: c.Title, Level: c.Level, Needs: c.Needs}
		fmt.Fprintf(os.Stderr, "  %-18s … ", c.ID)
		t0 := time.Now()
		func() {
			defer func() {
				if x := recover(); x != nil {
					r.Worsen(FAIL, fmt.Sprintf("panic: %v", x), "")
				}
			}()
			c.Run(ctx, e, r)
		}()
		if r.Status == "" {
			r.Status = PASS
		}
		r.DurationMS = time.Since(t0).Milliseconds()
		fmt.Fprintf(os.Stderr, "%s (%s)\n", r.Status, time.Since(t0).Round(10*time.Millisecond))
		rep.Results = append(rep.Results, r)
		if c.ID == "identity" && r.Status == FAIL {
			fmt.Fprintln(os.Stderr, "  no credentials: stopping")
			break
		}
	}
	for _, c := range sel {
		if c.ID == "cleanup" && !*keep {
			r := &Result{ID: "cleanup", Title: c.Title, Level: c.Level}
			cleanupPrefix(context.Background(), e, e.Root+"/", r)
			rep.Results = append(rep.Results, r)
		}
	}
	rep.Finished = time.Now().UTC()
	rep.Capabilities = verdicts(rep.Results)
	rep.Requests = e.Rec.Counts()
	printTable(os.Stdout, rep, *verbose)
	if err := writeJSON(*out, rep); err != nil {
		fatal(err)
	}
	fmt.Printf("JSON report: %s\n", *out)
	for _, c := range rep.Capabilities {
		if c.Verdict == "REJECTED" && c.ID == "control-plane" {
			os.Exit(1)
		}
	}
}

func selectChecks(only, skip string) []Check {
	set := func(s string) map[string]bool {
		m := map[string]bool{}
		for _, x := range strings.Split(s, ",") {
			if x = strings.TrimSpace(x); x != "" {
				m[x] = true
			}
		}
		return m
	}
	on, off := set(only), set(skip)
	var out []Check
	known := map[string]bool{}
	for _, c := range checks() {
		known[c.ID] = true
		if (len(on) == 0 || on[c.ID] || c.ID == "identity" || c.ID == "cleanup") && !off[c.ID] {
			out = append(out, c)
		}
	}
	for id := range on {
		if !known[id] {
			fatal(fmt.Errorf("unknown check %q", id))
		}
	}
	return out
}

func dryRun(o *Opts, p Params, run string, sel []Check, keep bool, out string) {
	fmt.Printf("DRY RUN: nothing is sent.\n\n")
	fmt.Printf("target      s3://%s/%s/%s/\n", o.Bucket, o.Prefix, run)
	fmt.Printf("store       %s\n", o.Store)
	fmt.Printf("endpoint    %s (%s)\n", orDefault(o.Endpoint, orDefault(os.Getenv("AWS_ENDPOINT_URL_S3"), "AWS, from the region")),
		map[bool]string{true: "path-style", false: "virtual-hosted"}[o.pathStyle()])
	fmt.Printf("region      %s\n", orDefault(o.Region, orDefault(os.Getenv("AWS_REGION"), orDefault(os.Getenv("AWS_DEFAULT_REGION"), "(from profile; us-east-1 if custom endpoint)"))))
	fmt.Printf("CA bundle   %s\n", orDefault(o.CABundle, orDefault(os.Getenv("AWS_CA_BUNDLE"), "system roots")))
	switch {
	case o.AccessKey != "":
		fmt.Printf("credentials static keys from flags\n")
	case o.Profile != "":
		fmt.Printf("credentials profile %s, then the default chain\n", o.Profile)
	default:
		fmt.Printf("credentials the default chain; environment seen:\n")
		for k, v := range credEnvHints() {
			fmt.Printf("              %s=%s\n", k, v)
		}
	}
	if o.RoleARN != "" {
		fmt.Printf("assume role %s\n", o.RoleARN)
	}
	if len(o.RaceEndpoints) > 0 {
		fmt.Printf("race endpoints %s (create-race and cas-race writers alternate over main + these)\n", strings.Join(o.RaceEndpoints, ", "))
	}
	fmt.Printf("\nchecks, in order (SDK retries off; per-request timeout %s):\n", o.Timeout)
	for i, c := range sel {
		if c.ID == "cleanup" && keep {
			fmt.Printf("%2d. %-18s [skipped: --keep]\n", i+1, c.ID)
			continue
		}
		fmt.Printf("%2d. %-18s %-11s %s\n", i+1, c.ID, c.Level, c.Title)
		fmt.Printf("    %-18s %s\n", "", c.Plan(p))
	}
	fmt.Printf("\nreport: table on stdout, JSON to %s\n", out)
}

func target(o *Opts, root string) map[string]any {
	return map[string]any{"bucket": o.Bucket, "root": root, "endpoint": orDefault(o.Endpoint, "aws"), "region": o.Region,
		"path_style": o.pathStyle(), "profile": o.Profile, "ca_bundle": o.CABundle, "role_arn": o.RoleARN,
		"static_keys_flag": o.AccessKey != "", "race_endpoints": o.RaceEndpoints}
}

func parseSizes(s string) ([]int, error) {
	var out []int
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(strings.ToUpper(f))
		mult := 1
		for _, u := range []struct {
			suf string
			m   int
		}{{"MIB", 1 << 20}, {"KIB", 1 << 10}, {"MB", 1000000}, {"KB", 1000}, {"B", 1}} {
			if strings.HasSuffix(f, u.suf) {
				mult, f = u.m, strings.TrimSuffix(f, u.suf)
				break
			}
		}
		n, err := strconv.Atoi(f)
		if err != nil || n <= 0 || n*mult < 32 {
			return nil, fmt.Errorf("bad size %q", f)
		}
		out = append(out, n*mult)
	}
	return out, nil
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "s3accept:", err)
	os.Exit(2)
}
