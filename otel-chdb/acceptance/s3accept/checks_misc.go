package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// ---- identity ----

// credEnvHints lists the credential-related environment the chain will see.
func credEnvHints() map[string]string {
	h := map[string]string{}
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE",
		"AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
		"AWS_EC2_METADATA_SERVICE_ENDPOINT", "AWS_EC2_METADATA_DISABLED", "AWS_REGION", "AWS_DEFAULT_REGION",
		"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL_STS", "AWS_CA_BUNDLE", "SSL_CERT_FILE",
		"AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_RESPONSE_CHECKSUM_VALIDATION", "HTTPS_PROXY", "NO_PROXY"} {
		if v := os.Getenv(k); v != "" {
			if k == "AWS_ACCESS_KEY_ID" {
				v = mask(v)
			}
			h[k] = v
		}
	}
	return h
}

func mask(s string) string {
	if len(s) <= 8 {
		return "****"
	}
	return s[:4] + "…" + s[len(s)-4:]
}

// sourceLabel turns the SDK's provider name into the deployment mode it means.
func sourceLabel(src string) string {
	switch {
	case strings.HasPrefix(src, "WebIdentityCredentials"):
		return "IRSA (web identity → STS AssumeRoleWithWebIdentity)"
	case strings.HasPrefix(src, "CredentialsEndpointProvider"):
		return "container credentials (EKS Pod Identity agent, or ECS)"
	case strings.HasPrefix(src, "EC2RoleProvider"):
		if os.Getenv("AWS_EC2_METADATA_SERVICE_ENDPOINT") != "" {
			return "IMDS at " + os.Getenv("AWS_EC2_METADATA_SERVICE_ENDPOINT") + " (aws_signing_helper serve, Roles Anywhere)"
		}
		return "IMDS (the NODE's instance role: pods should not normally get here)"
	case strings.HasPrefix(src, "ProcessProvider"):
		return "credential_process (e.g. aws_signing_helper credential-process, Roles Anywhere)"
	case strings.HasPrefix(src, "EnvConfigCredentials"):
		return "static keys from the environment"
	case strings.HasPrefix(src, "SharedConfigCredentials"):
		return "static keys from a shared credentials/config file"
	case strings.HasPrefix(src, "StaticCredentials"):
		return "static keys (flags)"
	case strings.HasPrefix(src, "AssumeRoleProvider"):
		return "STS AssumeRole on top of another source"
	case strings.HasPrefix(src, "SSOProvider"):
		return "AWS SSO / Identity Center (a human session: not a deployment mode)"
	}
	return src
}

func checkIdentity(ctx context.Context, e *Env, r *Result) {
	hints := credEnvHints()
	r.Set("env", hints)
	t0 := time.Now()
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	creds, err := e.AWS.Credentials.Retrieve(cctx)
	cancel()
	if err != nil {
		r.Worsen(FAIL, "no credentials: "+describe(err),
			"Nothing else can run. Check the pod's env (IRSA: AWS_ROLE_ARN + token file; Pod Identity: "+
				"AWS_CONTAINER_CREDENTIALS_FULL_URI) or pass keys / --profile.")
		return
	}
	r.Set("source", creds.Source)
	r.Set("mode", sourceLabel(creds.Source))
	r.Set("access_key", mask(creds.AccessKeyID))
	r.Set("session_token", creds.SessionToken != "")
	r.Set("retrieve_ms", time.Since(t0).Milliseconds())
	exp := "never"
	if creds.CanExpire {
		exp = creds.Expires.UTC().Format(time.RFC3339) + fmt.Sprintf(" (in %s)", time.Until(creds.Expires).Round(time.Second))
		r.Set("expires", creds.Expires.UTC())
	}
	r.Log("credential source: %s → %s", creds.Source, sourceLabel(creds.Source))
	r.Log("key %s, session token %v, expires %s, retrieved in %s", mask(creds.AccessKeyID), creds.SessionToken != "", exp, time.Since(t0).Round(time.Millisecond))
	summary := sourceLabel(creds.Source)

	// Caller identity through STS, where there is an STS to ask.
	stsWanted := e.O.Store == "aws" || e.O.STSEndpoint != "" || os.Getenv("AWS_ENDPOINT_URL_STS") != ""
	if !stsWanted {
		r.Log("STS GetCallerIdentity skipped: custom endpoint and no --sts-endpoint (Nutanix Objects has no STS)")
	} else {
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		out, err := stsClient(e.AWS, e.O).GetCallerIdentity(cctx, &sts.GetCallerIdentityInput{})
		cancel()
		if err != nil {
			r.Log("STS GetCallerIdentity: %s", describe(err))
			r.Worsen(WARN, summary+"; STS GetCallerIdentity failed",
				"Credentials resolved but STS is unreachable or refused (a VPC without an STS endpoint, a proxy, "+
					"or a regional STS disabled). S3 may still work; IRSA itself needs STS, so check the pod's egress.")
		} else {
			r.Set("arn", aws.ToString(out.Arn))
			r.Set("account", aws.ToString(out.Account))
			r.Log("caller: %s (account %s)", aws.ToString(out.Arn), aws.ToString(out.Account))
			summary += "; caller " + aws.ToString(out.Arn)
		}
	}
	if strings.HasPrefix(creds.Source, "EC2RoleProvider") && os.Getenv("AWS_EC2_METADATA_SERVICE_ENDPOINT") == "" && e.O.Store == "aws" {
		r.Worsen(WARN, summary, "The chain fell through to IMDS: this pod is using the NODE's instance role. "+
			"IRSA / Pod Identity is not wired for this ServiceAccount, and pods can reach IMDS (hop limit > 1). "+
			"Fix the ServiceAccount before trusting any other result's IAM behaviour.")
	}
	r.Worsen(PASS, summary, "")
}

// ---- user metadata round trip ----

func checkMetadata(ctx context.Context, e *Env, r *Result) {
	k := e.Key("metadata", "slot.parquet")
	meta := map[string]string{
		"oscope-kind":        "data",
		"oscope-content":     strings.Repeat("ab12", 16), // a 64-hex content key
		"oscope-rows":        "7",
		"oscope-epoch":       "e" + e.Run,
		"oscope-seq":         "00000000000000000042",
		"oscope-received-at": fmt.Sprint(time.Now().UnixMilli()),
		"oscope-producer":    "collector-0.otel.svc/traces",
		"oscope-schema":      "v1",
		"oscope-long":        strings.Repeat("x", 400),
	}
	const ct = "application/vnd.apache.parquet"
	_, err := e.put(ctx, putReq{Key: k, Body: "PAR1" + e.Run, CT: ct, Meta: meta, INM: "*"})
	r.Log("create-only PUT with %d x-amz-meta-* headers: %s", len(meta), describe(err))
	if err != nil {
		r.Worsen(FAIL, "create-only PUT with metadata failed: "+describe(err),
			"The inline design puts the slot's envelope (kind, content key, rows, received_at) in user metadata "+
				"so the consumer can HEAD instead of GET. Without it the consumer must GET each object's footer.")
		return
	}
	_, err = e.put(ctx, putReq{Key: k, Body: "other", CT: "text/plain", Meta: map[string]string{"oscope-kind": "tombstone"}, INM: "*"})
	o, _, _ := classify(err)
	r.Log("second create-only PUT with other metadata: %s", describe(err))
	h, err := e.head(ctx, k)
	if err != nil {
		r.Worsen(FAIL, "HEAD failed: "+describe(err), "")
		return
	}
	var missing, wrong []string
	for mk, mv := range meta {
		got, ok := h.Metadata[mk]
		switch {
		case !ok:
			missing = append(missing, mk)
		case got != mv:
			wrong = append(wrong, mk)
		}
	}
	sort.Strings(missing)
	sort.Strings(wrong)
	r.Log("HEAD: %d metadata keys, content-type %q, length %d", len(h.Metadata), aws.ToString(h.ContentType), aws.ToInt64(h.ContentLength))
	g, err := e.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: &e.O.Bucket, Key: &k})
	getMeta := 0
	if err == nil {
		getMeta = len(g.Metadata)
		g.Body.Close()
	}
	r.Log("GET returns %d metadata keys", getMeta)
	switch {
	case len(missing) > 0 || len(wrong) > 0:
		r.Worsen(FAIL, fmt.Sprintf("metadata not round-tripped: missing %v, changed %v", missing, wrong),
			"The consumer reads the envelope from HEAD. Put the envelope in the Parquet footer's key-value "+
				"metadata instead (parquetencoding already writes it) and GET the footer: one extra ranged GET per object.")
	case aws.ToString(h.ContentType) != ct:
		r.Worsen(WARN, "content type not preserved: "+aws.ToString(h.ContentType), "Nothing in the design keys on it; cosmetic.")
	case o == OK:
		r.Worsen(FAIL, "a second create-only PUT replaced the metadata", "If-None-Match ignored when metadata is present. "+fallback)
	default:
		r.Worsen(PASS, fmt.Sprintf("%d keys (incl. a 400-byte value) and content type round-trip on HEAD and GET; a losing create keeps the first metadata", len(meta)), "")
	}
}

// ---- read-after-write ----

func checkRAW(ctx context.Context, e *Env, r *Result) {
	n := e.Params.ConsistencyN
	var newMiss, overStale, delStale, headMiss int
	for i := 0; i < n; i++ {
		k := e.Key("raw", fmt.Sprintf("k-%03d", i))
		b1 := fmt.Sprintf("v1-%d-%s", i, e.Run)
		if _, err := e.put(ctx, putReq{Key: k, Body: b1, INM: "*"}); err != nil {
			r.Worsen(FAIL, "PUT failed: "+describe(err), "")
			return
		}
		if got, _, err := e.get(ctx, k); err != nil || string(got) != b1 {
			newMiss++
		}
		if _, err := e.head(ctx, k); err != nil {
			headMiss++
		}
		b2 := fmt.Sprintf("v2-%d-%s", i, e.Run)
		if _, err := e.put(ctx, putReq{Key: k, Body: b2}); err != nil {
			r.Worsen(FAIL, "overwrite failed: "+describe(err), "")
			return
		}
		if got, _, err := e.get(ctx, k); err != nil || string(got) != b2 {
			overStale++
		}
		if err := e.del(ctx, k, ""); err != nil {
			r.Worsen(FAIL, "DELETE failed: "+describe(err), "")
			return
		}
		if _, _, err := e.get(ctx, k); err == nil {
			delStale++
		}
	}
	r.Log("%d keys: GET right after create missed %d; HEAD missed %d; GET after overwrite stale %d; GET after DELETE still found %d",
		n, newMiss, headMiss, overStale, delStale)
	r.Set("new_miss", newMiss)
	r.Set("overwrite_stale", overStale)
	r.Set("delete_stale", delStale)
	switch {
	case newMiss+headMiss > 0:
		r.Worsen(FAIL, fmt.Sprintf("read-after-create misses: GET %d, HEAD %d of %d", newMiss, headMiss, n),
			"A writer that reads a slot after a 412 or timeout could see it free and resend: still safe (the resend "+
				"gets 412), but the consumer would take a committed slot for a free one and stall until it shows. "+
				"More seriously, stale reads mean the store is not strongly consistent, so conditional writes are "+
				"suspect too. "+fallback)
	case overStale > 0:
		r.Worsen(FAIL, fmt.Sprintf("GET after overwrite returned the old body %d of %d times", overStale, n),
			"Ambiguous CAS resolution reads back the object; a stale read would make a writer think its CAS "+
				"did not apply and try again with an old ETag (412) and then misjudge ownership. "+fallback)
	case delStale > 0:
		r.Worsen(WARN, fmt.Sprintf("GET after DELETE still found the object %d of %d times", delStale, n),
			"Only GC deletes; readers that race GC already tolerate a missing object. Log truncation option (a) "+
				"(delete old slots after a zombie bound) is unaffected.")
	default:
		r.Worsen(PASS, fmt.Sprintf("%d keys: create, overwrite and delete all visible to the next GET/HEAD", n), "")
	}
}

// ---- LIST after write, StartAfter ordering ----

func slotKey(root, epoch string, seq int) string {
	return fmt.Sprintf("%s/%s/%020d.parquet", root, epoch, seq)
}

func checkList(ctx context.Context, e *Env, r *Result) {
	// 1. LIST-after-write lag.
	lagRoot := e.Key("list", "lag")
	var misses int
	var maxLag time.Duration
	for i := 0; i < e.Params.ConsistencyN; i++ {
		k := fmt.Sprintf("%s/%03d", lagRoot, i)
		if _, err := e.put(ctx, putReq{Key: k, Body: "x"}); err != nil {
			r.Worsen(FAIL, "PUT: "+describe(err), "")
			return
		}
		t0 := time.Now()
		for first := true; ; first = false {
			keys, _, err := e.listKeys(ctx, lagRoot+"/", fmt.Sprintf("%s/%03d", lagRoot, i-1), 1000)
			if err != nil {
				r.Worsen(FAIL, "LIST: "+describe(err), listMeaning(err))
				return
			}
			if len(keys) > 0 && keys[0] == k {
				break
			}
			if first {
				misses++
			}
			if time.Since(t0) > e.Params.ListLagMax {
				r.Log("key %d not listed after %s", i, e.Params.ListLagMax)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if lag := time.Since(t0); lag > maxLag {
			maxLag = lag
		}
	}
	r.Log("LIST right after PUT: %d of %d missed the new key; worst time to visibility %s", misses, e.Params.ConsistencyN, maxLag.Round(time.Millisecond))
	r.Set("list_after_write_misses", misses)

	// 2. StartAfter over slot keys, as the consumer lists.
	lane := e.Key("list", "lane")
	seqs := []int{0, 1, 2, 5, 9, 10, 11, 100}
	for _, ep := range []string{"e1", "e2"} {
		for _, s := range seqs {
			if _, err := e.put(ctx, putReq{Key: slotKey(lane, ep, s), Body: "x", INM: "*"}); err != nil {
				r.Worsen(FAIL, "PUT slot: "+describe(err), "")
				return
			}
		}
	}
	want := func(ep string, from int) []string {
		var o []string
		for _, s := range seqs {
			if s > from {
				o = append(o, slotKey(lane, ep, s))
			}
		}
		return o
	}
	type q struct {
		name, prefix, after string
		want                []string
	}
	e1After2 := want("e1", 2)
	all := append(append([]string{}, want("e1", -1)...), want("e2", -1)...)
	cases := []q{
		{"epoch-scoped, StartAfter slot 2", lane + "/e1/", slotKey(lane, "e1", 2), e1After2},
		{"lane-wide, StartAfter e1 slot 2 (returns later epochs too)", lane + "/", slotKey(lane, "e1", 2), append(append([]string{}, e1After2...), want("e2", -1)...)},
		{"StartAfter a key that does not exist (slot 3)", lane + "/e1/", slotKey(lane, "e1", 3), want("e1", 3)},
		{"StartAfter {epoch}/0 (the consumer's slot-0 form)", lane + "/", lane + "/e1/0", all},
		{"StartAfter the last key", lane + "/", slotKey(lane, "e2", 100), nil},
	}
	for _, c := range cases {
		got, pages, err := e.listKeys(ctx, c.prefix, c.after, 1000)
		if err != nil {
			r.Worsen(FAIL, "LIST StartAfter: "+describe(err), listMeaning(err))
			return
		}
		ok := fmt.Sprint(got) == fmt.Sprint(c.want)
		r.Log("%s: %d keys in %d page(s), %v", c.name, len(got), pages, map[bool]string{true: "as expected", false: "WRONG"}[ok])
		if !ok {
			r.Log("  got  %v", short(got, lane))
			r.Log("  want %v", short(c.want, lane))
			r.Worsen(FAIL, "LIST StartAfter returned the wrong keys ("+c.name+")",
				"The consumer discovers new slots with LIST StartAfter its checkpoint key. It never skips a gap, so "+
					"wrong results stall a lane rather than lose data, but discovery is broken: run the consumer "+
					"with HEAD-probing of consecutive slots only (no LIST), or fall back to the Keeper Coordinator.")
		}
	}
	// A StartAfter naming a "directory": AWS compares strings; SeaweedFS skips the whole directory.
	got, _, _ := e.listKeys(ctx, lane+"/", lane+"/e1/", 1000)
	dirOK := fmt.Sprint(got) == fmt.Sprint(all)
	r.Log("StartAfter %q (directory form): %d keys (%s)", "…/e1/", len(got), map[bool]string{true: "string order, as AWS", false: "NOT string order (SeaweedFS skips the directory); the consumer's {epoch}/0 form avoids it"}[dirOK])
	r.Set("startafter_directory_string_order", dirOK)

	// 3. Pagination with StartAfter.
	paged, pages, err := e.listKeys(ctx, lane+"/", slotKey(lane, "e1", 2), 3)
	r.Log("same lane-wide LIST at MaxKeys=3: %d keys in %d pages (%s)", len(paged), pages, describe(err))
	if fmt.Sprint(paged) != fmt.Sprint(cases[1].want) {
		r.Worsen(FAIL, "paginated LIST with StartAfter differs from the single-page one",
			"Continuation tokens and StartAfter do not compose: the consumer would lose or repeat slots across pages. Use MaxKeys 1000 and HEAD-probing.")
	}
	sorted := sort.StringsAreSorted(paged)
	if !sorted {
		r.Worsen(FAIL, "LIST is not in lexicographic (UTF-8 binary) order", "Slot discovery assumes key order = slot order.")
	}

	// 4. Delimiter: the epochs of a lane.
	out, err := e.list(ctx, lane+"/", "", "/", 0, nil)
	var eps []string
	if err == nil {
		for _, p := range out.CommonPrefixes {
			eps = append(eps, strings.TrimPrefix(aws.ToString(p.Prefix), lane+"/"))
		}
	}
	r.Log("delimiter LIST of the lane: %v (%s)", eps, describe(err))
	if fmt.Sprint(eps) != "[e1/ e2/]" {
		r.Worsen(WARN, fmt.Sprintf("delimiter LIST returned %v, want [e1/ e2/]", eps),
			"Epoch discovery by delimiter is broken; list the lane flat from its floor (--full-list) instead.")
	}
	if r.Status == "" {
		if misses > 0 {
			r.Worsen(WARN, fmt.Sprintf("StartAfter ordering right; LIST lagged a new key %d/%d times (worst %s)", misses, e.Params.ConsistencyN, maxLag.Round(time.Millisecond)),
				"LIST is eventually consistent here. The consumer never skips a gap, so lag costs visibility latency, "+
					"not correctness; add the observed worst lag to the visibility budget. Log slots are probed by GET/HEAD, which must be consistent (see read-after-write).")
		} else {
			r.Worsen(PASS, fmt.Sprintf("LIST-after-write consistent (%d/%d); StartAfter, pagination, delimiter as AWS", e.Params.ConsistencyN, e.Params.ConsistencyN), "")
		}
	}
}

func short(keys []string, root string) []string {
	o := make([]string, len(keys))
	for i, k := range keys {
		o[i] = strings.TrimPrefix(k, root+"/")
	}
	return o
}

func listMeaning(err error) string {
	if o, _, _ := classify(err); o == Forbidden {
		return "LIST refused: grant s3:ListBucket on the bucket (see iam/). The consumer's discovery, " +
			"the GC sweep and HEAD-404 semantics all need it."
	}
	return ""
}

// ---- HEAD / GET on a missing key ----

func checkHeadMissing(ctx context.Context, e *Env, r *Result) {
	k := e.Key("head-missing", "never-written")
	_, herr := e.head(ctx, k)
	_, _, gerr := e.get(ctx, k)
	ho, hs, _ := classify(herr)
	gdesc := describe(gerr)
	r.Log("HEAD missing key: %s; GET missing key: %s", describe(herr), gdesc)
	r.Set("head_status", hs)
	switch ho {
	case NotFound:
		r.Worsen(PASS, "HEAD 404, GET "+gdesc, "")
	case Forbidden:
		r.Worsen(FAIL, "HEAD of a missing key answers 403, not 404",
			"The role lacks s3:ListBucket (S3 hides existence without it). The consumer HEADs the next slot to find "+
				"the end of a log and the writer HEADs a slot after a timeout: a 403 reads as 'denied', not 'free', "+
				"so lanes stall and ambiguous appends never resolve. Grant s3:ListBucket on the bucket. If you "+
				"scope it with an s3:prefix condition, re-run this check: a HEAD carries no s3:prefix, so a "+
				"prefix-conditioned grant may not count (unconditioned ListBucket on the bucket is the safe choice).")
	default:
		r.Worsen(FAIL, "HEAD of a missing key answers "+describe(herr), "Free slots must answer 404.")
	}
}

// ---- checksums ----

// checkChecksum tries the SDK's default (CRC32 when supported), a streamed
// body (aws-chunked with a trailing checksum), explicit algorithms, and
// when_required, and reads the checksum back. Newer SDKs (Go v2 from
// 2025-01, Rust, Java 2.30+) send CRC32 by default, which older
// S3-compatible stores reject.
func checkChecksum(ctx context.Context, e *Env, r *Result) {
	body := bytes.Repeat([]byte("0123456789abcdef"), 100<<10/16)
	supported := e.client(func(o *s3.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenSupported
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenSupported
	})
	required := e.client(func(o *s3.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	type res struct {
		name string
		err  error
	}
	var rs []res
	try := func(name string, f func() error) error {
		err := f()
		rs = append(rs, res{name, err})
		r.Log("%-58s %s", name, describe(err))
		return err
	}
	isPut := func(q *http.Request) bool { return q.Method == http.MethodPut }

	e.Rec.Capture(isPut)
	errDefault := try("PUT If-None-Match:*, default (CRC32 when_supported)", func() error {
		_, err := e.putWith(ctx, supported, putReq{Key: e.Key("checksum", "default"), Bytes: body, INM: "*"})
		return err
	})
	if h := e.Rec.Captured(); h != nil {
		var sent []string
		for k := range h {
			lk := strings.ToLower(k)
			if strings.HasPrefix(lk, "x-amz-checksum") || strings.HasPrefix(lk, "x-amz-sdk-checksum") || lk == "x-amz-trailer" ||
				lk == "content-encoding" || lk == "x-amz-content-sha256" || lk == "x-amz-decoded-content-length" || lk == "content-md5" {
				sent = append(sent, k+": "+h.Get(k))
			}
		}
		sort.Strings(sent)
		r.Log("  headers sent: %s", strings.Join(sent, "; "))
		r.Set("default_put_headers", sent)
		// Over https the SDK streams every PUT as aws-chunked with a trailing
		// CRC32; over plain http it sends a CRC32 header instead.
		trailer := strings.Contains(strings.ToLower(h.Get("Content-Encoding")), "aws-chunked")
		r.Set("default_uses_trailer", trailer)
		r.Log("  default PUT used a trailing checksum (aws-chunked): %v", trailer)
	}
	errStream := try("PUT streamed body (aws-chunked, trailing CRC32)", func() error {
		cctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
		defer cancel()
		_, err := supported.PutObject(cctx, &s3.PutObjectInput{Bucket: &e.O.Bucket, Key: aws.String(e.Key("checksum", "stream")),
			Body: io.NopCloser(bytes.NewReader(body)), ContentLength: aws.Int64(int64(len(body)))})
		return err
	})
	for _, alg := range []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha256} {
		alg := alg
		try("PUT explicit "+string(alg), func() error {
			cctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
			defer cancel()
			_, err := supported.PutObject(cctx, &s3.PutObjectInput{Bucket: &e.O.Bucket, Key: aws.String(e.Key("checksum", string(alg))),
				Body: bytes.NewReader(body), ChecksumAlgorithm: alg})
			return err
		})
	}
	errRequired := try("PUT If-None-Match:*, when_required (no checksum)", func() error {
		_, err := e.putWith(ctx, required, putReq{Key: e.Key("checksum", "required"), Bytes: body, INM: "*"})
		return err
	})
	var stored string
	errGet := try("GET with response validation (ChecksumMode ENABLED)", func() error {
		cctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
		defer cancel()
		out, err := supported.GetObject(cctx, &s3.GetObjectInput{Bucket: &e.O.Bucket, Key: aws.String(e.Key("checksum", "default")),
			ChecksumMode: types.ChecksumModeEnabled})
		if err != nil {
			return err
		}
		defer out.Body.Close()
		_, err = io.Copy(io.Discard, out.Body)
		stored = aws.ToString(out.ChecksumCRC32)
		return err
	})
	r.Log("  stored CRC32 returned by GET: %q", stored)
	r.Log("  GET responses so far without a checksum the SDK could validate: %d (fine: validation is skipped, not failed)", e.Rec.noChecksum.Load())
	// DeleteObjects needs Content-MD5 or a checksum; the SDK now sends CRC32.
	mk := func(name string) string {
		k := e.Key("checksum", "del-"+name)
		_, _ = e.putWith(ctx, required, putReq{Key: k, Body: "x"})
		return k
	}
	delWith := func(c *s3.Client, k string) error {
		cctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
		defer cancel()
		_, err := c.DeleteObjects(cctx, &s3.DeleteObjectsInput{Bucket: &e.O.Bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String(k)}}}})
		return err
	}
	errDelDefault := try("DeleteObjects, default (CRC32)", func() error { return delWith(supported, mk("default")) })
	errDelRequired := try("DeleteObjects, when_required", func() error { return delWith(required, mk("required")) })

	r.Set("default_ok", errDefault == nil)
	r.Set("stream_trailer_ok", errStream == nil)
	r.Set("when_required_ok", errRequired == nil)
	const fix = "Set AWS_REQUEST_CHECKSUM_CALCULATION=when_required and AWS_RESPONSE_CHECKSUM_VALIDATION=when_required " +
		"in every Go and Rust client's environment (parquetgo, otap-rs edge and consumer, this tool), or the " +
		"equivalent client option. ClickHouse's own S3 client is unaffected."
	switch {
	case errRequired != nil:
		r.Worsen(FAIL, "even a PUT without a checksum fails: "+describe(errRequired), "Not a checksum problem; see the other checks.")
	case errDefault != nil || errDelDefault != nil:
		r.Worsen(WARN, "the SDK's default CRC32 is rejected; when_required works", fix)
	case errGet != nil:
		r.Worsen(WARN, "response checksum validation fails on GET: "+describe(errGet), fix)
	case errStream != nil && strings.Contains(errStream.Error(), "without TLS"):
		r.Worsen(PASS, "default CRC32, CRC32C, SHA256, when_required, DeleteObjects accepted; trailer not tried (plain http: the SDK sends trailers only over TLS)", "")
	case errStream != nil:
		r.Worsen(WARN, "a streamed body (unknown length, trailing checksum) is rejected; the default PUT works",
			"The exporters send buffered bodies of known length, which is the default path that passed. Do not "+
				"switch them to streaming uploads on this store, or set when_required if you must.")
	case errDelRequired != nil:
		r.Worsen(WARN, "DeleteObjects fails: "+describe(errDelRequired), "GC must delete one object at a time.")
	default:
		r.Worsen(PASS, "default CRC32 (header and trailer), CRC32C, SHA256, when_required, DeleteObjects: all accepted", "")
	}
	_ = rs
}

// ---- bucket configuration (informational) ----

func checkBucketConfig(ctx context.Context, e *Env, r *Result) {
	r.Worsen(INFO, "", "")
	var notes []string
	cctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
	defer cancel()
	v, err := e.S3.GetBucketVersioning(cctx, &s3.GetBucketVersioningInput{Bucket: &e.O.Bucket})
	if err != nil {
		r.Log("GetBucketVersioning: %s", describe(err))
		notes = append(notes, "versioning unknown")
	} else {
		st := string(v.Status)
		if st == "" {
			st = "never enabled"
		}
		r.Log("versioning: %s", st)
		r.Set("versioning", st)
		notes = append(notes, "versioning "+st)
		if v.Status == types.BucketVersioningStatusEnabled {
			r.Worsen(WARN, "", "Versioning is on: conditions apply to the current version only (a delete marker "+
				"counts as absent), and GC's deletes only add delete markers. Add a lifecycle rule expiring "+
				"noncurrent versions and delete markers under the telemetry prefix, or storage never shrinks.")
		}
	}
	lc, err := e.S3.GetBucketLifecycleConfiguration(cctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: &e.O.Bucket})
	if err != nil {
		r.Log("GetBucketLifecycleConfiguration: %s", describe(err))
	} else {
		for _, rule := range lc.Rules {
			r.Log("lifecycle rule %q: %s", aws.ToString(rule.ID), rule.Status)
		}
		notes = append(notes, fmt.Sprintf("%d lifecycle rules", len(lc.Rules)))
	}
	pol, err := e.S3.GetBucketPolicy(cctx, &s3.GetBucketPolicyInput{Bucket: &e.O.Bucket})
	if err != nil {
		r.Log("GetBucketPolicy: %s", describe(err))
	} else {
		p := aws.ToString(pol.Policy)
		var keys []string
		for _, ck := range []string{"s3:if-none-match", "s3:if-match", "s3:ObjectCreationOperation"} {
			if strings.Contains(strings.ToLower(p), strings.ToLower(ck)) {
				keys = append(keys, ck)
			}
		}
		r.Set("policy_conditional_keys", keys)
		if len(keys) > 0 {
			notes = append(notes, "bucket policy enforces conditional writes ("+strings.Join(keys, ", ")+")")
			r.Worsen(WARN, "", "The bucket policy uses conditional-write condition keys: writers that send no "+
				"If-None-Match / If-Match (chDB s3(), ClickHouse INSERT INTO FUNCTION s3, CopyObject) will get 403 "+
				"in the covered prefixes. See plain-put.")
		} else {
			notes = append(notes, "bucket policy has no conditional-write keys")
		}
		var js any
		if json.Unmarshal([]byte(p), &js) == nil {
			r.Set("policy", js)
		}
	}
	r.Summary = strings.Join(notes, "; ")
}

// ---- clock ----

func checkClock(ctx context.Context, e *Env, r *Result) {
	e.Rec.mu.Lock()
	skew, seen := e.Rec.maxSkew, e.Rec.skewSeen
	e.Rec.mu.Unlock()
	if !seen {
		_, _ = e.head(ctx, e.Key("clock"))
		e.Rec.mu.Lock()
		skew, seen = e.Rec.maxSkew, e.Rec.skewSeen
		e.Rec.mu.Unlock()
	}
	if !seen {
		r.Worsen(SKIP, "no Date header seen", "")
		return
	}
	r.Set("max_skew_ms", skew.Milliseconds())
	switch {
	case skew > 10*time.Minute:
		r.Worsen(FAIL, fmt.Sprintf("store clock is %s off ours", skew.Round(time.Second)),
			"SigV4 rejects requests more than 15 minutes off (RequestTimeTooSkewed). Fix NTP on this host or the store.")
	case skew > 2*time.Second+time.Second: // Date has 1 s resolution
		r.Worsen(WARN, fmt.Sprintf("store clock is %s off ours", skew.Round(time.Millisecond)),
			"The consumer's server-side insert fence compares the holder's and ClickHouse's wall clocks within the "+
				"lease margin; a host this far off the store is likely off ClickHouse too. Check NTP.")
	default:
		r.Worsen(PASS, fmt.Sprintf("max |store Date − local| %s (1 s resolution)", skew.Round(time.Millisecond)), "")
	}
}

// ---- cleanup ----

func cleanupPrefix(ctx context.Context, e *Env, prefix string, r *Result) {
	keys, _, err := e.listKeys(ctx, prefix, "", 1000)
	if err != nil {
		r.Worsen(WARN, "LIST for cleanup failed: "+describe(err), "Delete "+prefix+" by hand.")
		return
	}
	deleted, failed := 0, 0
	for len(keys) > 0 {
		n := min(len(keys), 1000)
		batch := keys[:n]
		keys = keys[n:]
		var ids []types.ObjectIdentifier
		for _, k := range batch {
			ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
		}
		cctx, cancel := context.WithTimeout(ctx, 2*e.O.Timeout)
		out, err := e.S3.DeleteObjects(cctx, &s3.DeleteObjectsInput{Bucket: &e.O.Bucket, Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)}})
		cancel()
		if err == nil && len(out.Errors) == 0 {
			deleted += len(batch)
			continue
		}
		// DeleteObjects refused (checksum, not implemented): one at a time.
		for _, k := range batch {
			if e.del(ctx, k, "") == nil {
				deleted++
			} else {
				failed++
			}
		}
	}
	aborted := 0
	cctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
	ups, err := e.S3.ListMultipartUploads(cctx, &s3.ListMultipartUploadsInput{Bucket: &e.O.Bucket, Prefix: aws.String(prefix)})
	cancel()
	if err != nil {
		r.Log("ListMultipartUploads: %s (grant s3:ListBucketMultipartUploads, or rely on an abort-incomplete lifecycle rule)", describe(err))
	} else {
		for _, u := range ups.Uploads {
			e.abort(ctx, aws.ToString(u.Key), u.UploadId)
			aborted++
		}
	}
	left, _, _ := e.listKeys(ctx, prefix, "", 1000)
	r.Log("deleted %d objects (%d failed), aborted %d uploads, %d left under %s", deleted, failed, aborted, len(left), prefix)
	if failed > 0 || len(left) > 0 {
		r.Worsen(WARN, fmt.Sprintf("%d objects left under %s", len(left), prefix), "Delete them by hand (s3accept cleanup).")
		return
	}
	r.Worsen(PASS, fmt.Sprintf("deleted %d objects, aborted %d uploads; prefix empty", deleted, aborted), "")
}
