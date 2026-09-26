package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// The creds mode exercises each deployment credential mode through the SDK's
// own default chain, the way the exporters resolve credentials: for each
// mode it hides the environment of the others (and the shared files, except
// for the profile mode), so the chain can only land on that mode. Per mode:
// resolve, STS caller identity where there is an STS, a create-only PUT +
// HEAD + GET + DELETE, then a forced refresh (Invalidate + Retrieve, which
// goes back to the source: STS, the Pod Identity agent, IMDS, the helper
// process) and a HEAD with the refreshed credentials.

var modeEnv = map[string][]string{
	"static":       {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"},
	"profile":      {"AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"},
	"irsa":         {"AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_SESSION_NAME"},
	"pod-identity": {"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_CONTAINER_AUTHORIZATION_TOKEN"},
	"imds":         {"AWS_EC2_METADATA_SERVICE_ENDPOINT", "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE", "AWS_EC2_METADATA_DISABLED"},
}

var modeTitle = map[string]string{
	"chain":        "default chain, as the exporters resolve it",
	"static":       "static keys (Nutanix Objects, SeaweedFS)",
	"profile":      "shared-config profile (credential_process = aws_signing_helper, role chains)",
	"irsa":         "EKS IRSA (web identity token → STS)",
	"pod-identity": "EKS Pod Identity (container credentials agent)",
	"imds":         "IMDS (aws_signing_helper serve for Roles Anywhere; else the node role)",
}

func detectModes(o *Opts) []string {
	m := []string{"chain"}
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" || o.AccessKey != "" {
		m = append(m, "static")
	}
	if os.Getenv("AWS_PROFILE") != "" || o.Profile != "" {
		m = append(m, "profile")
	}
	if os.Getenv("AWS_ROLE_ARN") != "" && os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != "" {
		m = append(m, "irsa")
	}
	if os.Getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI") != "" || os.Getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") != "" {
		m = append(m, "pod-identity")
	}
	if os.Getenv("AWS_EC2_METADATA_SERVICE_ENDPOINT") != "" || (o.Store == "aws" && os.Getenv("AWS_EC2_METADATA_DISABLED") != "true") {
		m = append(m, "imds")
	}
	return m
}

// isolate runs f with only mode's credential environment visible.
func isolate(mode string, f func()) {
	saved := map[string]*string{}
	hide := func(k string) {
		if _, ok := saved[k]; ok {
			return
		}
		if v, ok := os.LookupEnv(k); ok {
			saved[k] = &v
		} else {
			saved[k] = nil
		}
		os.Unsetenv(k)
	}
	if mode != "chain" {
		for m, keys := range modeEnv {
			if m == mode {
				continue
			}
			for _, k := range keys {
				hide(k)
			}
		}
		if mode != "profile" {
			// No shared files: a default profile would otherwise win over IRSA etc.
			hide("AWS_CONFIG_FILE")
			hide("AWS_SHARED_CREDENTIALS_FILE")
			os.Setenv("AWS_CONFIG_FILE", os.DevNull)
			os.Setenv("AWS_SHARED_CREDENTIALS_FILE", os.DevNull)
		}
		if mode != "imds" {
			hide("AWS_EC2_METADATA_DISABLED")
			os.Setenv("AWS_EC2_METADATA_DISABLED", "true")
		}
	}
	defer func() {
		for k, v := range saved {
			if v == nil {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, *v)
			}
		}
	}()
	f()
}

func runCreds(ctx context.Context, o *Opts, run string, dry bool, modesFlag string, hold, every time.Duration,
	sample bool, out string, verbose bool) int {
	modes := detectModes(o)
	if modesFlag != "auto" {
		modes = strings.Split(modesFlag, ",")
	}
	root := o.Prefix + "/creds/" + run
	if dry {
		fmt.Printf("DRY RUN: nothing is sent.\n\ncredential modes to exercise, each with the others' environment hidden:\n")
		for _, m := range modes {
			fmt.Printf("  %-13s %s\n", m, modeTitle[m])
			fmt.Printf("  %-13s resolve; STS GetCallerIdentity (AWS or --sts-endpoint); PUT If-None-Match:* + HEAD + GET + DELETE of s3://%s/%s/%s.txt; Invalidate + Retrieve; HEAD\n", "", o.Bucket, root, m)
		}
		fmt.Printf("\ncredential environment seen:\n")
		for k, v := range credEnvHints() {
			fmt.Printf("  %s=%s\n", k, v)
		}
		if hold > 0 {
			fmt.Printf("\nthen, with the chain's client: a HEAD every %s for %s, logging every credential change\n", every, hold)
		}
		if sample {
			fmt.Printf("\nleave s3://%s/%s/creds/sample.tsv for the ClickHouse check (credcheck/clickhouse-central.sql)\n", o.Bucket, o.Prefix)
		}
		return 0
	}
	tr, pem, err := transport(o)
	if err != nil {
		fatal(err)
	}
	rec := &Recorder{base: tr}
	rep := &Report{Tool: "s3accept", Mode: "creds", Started: time.Now().UTC(), Store: o.Store, Target: target(o, root)}
	var chainEnv *Env
	for _, m := range modes {
		r := &Result{ID: m, Title: modeTitle[m], Level: Info}
		t0 := time.Now()
		isolate(m, func() {
			mo := *o
			if m != "chain" && m != "static" {
				mo.AccessKey, mo.SecretKey = "", ""
			}
			if m != "chain" && m != "profile" {
				mo.Profile = ""
			}
			cfg, err := loadAWS(ctx, &mo, rec, pem)
			if err != nil {
				r.Worsen(FAIL, "config: "+describe(err), "")
				return
			}
			e := &Env{O: &mo, AWS: cfg, Rec: rec, Base: tr, Run: run, Root: root}
			e.S3 = e.client(nil)
			credMode(ctx, e, m, r)
			if m == "chain" {
				chainEnv = e
			}
		})
		r.DurationMS = time.Since(t0).Milliseconds()
		if r.Status == "" {
			r.Status = PASS
		}
		rep.Results = append(rep.Results, r)
	}
	// Which mode won in the chain, and is anything shadowed?
	if len(rep.Results) > 0 && rep.Results[0].ID == "chain" && rep.Results[0].Data != nil {
		src, _ := rep.Results[0].Data["source"].(string)
		var configured []string
		for _, m := range modes {
			if m != "chain" && m != "imds" {
				configured = append(configured, m)
			}
		}
		if len(configured) > 1 {
			rep.Results[0].Worsen(WARN, "", fmt.Sprintf("Several credential modes are configured (%s) and the chain picked %q. "+
				"Order in aws-sdk-go-v2 (config.resolveCredentialChain): a profile passed in code (--profile, "+
				"parquetgo Config.Profile), then env keys, then IRSA (web identity env), then the shared-config profile "+
				"(AWS_PROFILE or [default]), then container credentials (Pod Identity), then IMDS. A leftover "+
				"AWS_ACCESS_KEY_ID shadows IRSA and Pod Identity, and a [default] profile with keys in a mounted "+
				"~/.aws shadows Pod Identity: remove what the deployment does not intend.", strings.Join(configured, ", "), sourceLabel(src)))
		}
	}
	if chainEnv != nil && hold > 0 {
		r := &Result{ID: "hold", Title: fmt.Sprintf("requests every %s for %s across credential refreshes", every, hold), Level: Info}
		t0 := time.Now()
		holdCreds(ctx, chainEnv, hold, every, r)
		r.DurationMS = time.Since(t0).Milliseconds()
		rep.Results = append(rep.Results, r)
	}
	if chainEnv != nil && sample {
		k := o.Prefix + "/creds/sample.tsv"
		_, err := chainEnv.put(ctx, putReq{Key: k, Body: "1\thello\n2\tworld\n3\tkeyless\n", CT: "text/tab-separated-values"})
		fmt.Printf("sample for ClickHouse: s3://%s/%s (%s)\n", o.Bucket, k, describe(err))
		if o.Endpoint != "" {
			fmt.Printf("  path-style URL: %s/%s/%s\n", o.Endpoint, o.Bucket, k)
		} else {
			fmt.Printf("  URL: https://%s.s3.%s.amazonaws.com/%s\n", o.Bucket, orDefault(o.Region, os.Getenv("AWS_REGION")), k)
		}
	}
	rep.Finished = time.Now().UTC()
	rep.Requests = rec.Counts()
	printTable(os.Stdout, rep, verbose)
	if err := writeJSON(out, rep); err != nil {
		fatal(err)
	}
	fmt.Printf("JSON report: %s\n", out)
	for _, r := range rep.Results {
		if r.Status == FAIL && r.ID == "chain" {
			return 1
		}
	}
	return 0
}

func credMode(ctx context.Context, e *Env, mode string, r *Result) {
	t0 := time.Now()
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	c1, err := e.AWS.Credentials.Retrieve(cctx)
	cancel()
	if err != nil {
		st := FAIL
		meaning := "This mode is configured but does not yield credentials. "
		switch mode {
		case "imds":
			st = INFO
			meaning = ""
			r.Summary = "IMDS not reachable (expected in a pod with IMDSv2 hop limit 1; required only for aws_signing_helper serve)"
		case "irsa":
			meaning += "Check the token file is mounted and readable, AWS_ROLE_ARN's trust policy names this ServiceAccount (sub/aud), and STS is reachable (sts.<region>.amazonaws.com, or a VPC endpoint)."
		case "pod-identity":
			meaning += "Check the eks-pod-identity-agent DaemonSet runs on this node, the association exists for this namespace/ServiceAccount, and the token file is mounted."
		case "profile":
			meaning += "Check the profile exists in AWS_CONFIG_FILE and, for credential_process, that aws_signing_helper runs by hand with the same arguments (certificate, key, trust anchor, profile and role ARNs)."
		}
		r.Log("retrieve: %v", err)
		if st == INFO {
			r.Worsen(INFO, r.Summary, "")
		} else {
			r.Worsen(st, "no credentials: "+describe(err), meaning)
		}
		return
	}
	d1 := time.Since(t0)
	r.Set("source", c1.Source)
	r.Set("access_key", mask(c1.AccessKeyID))
	r.Set("session_token", c1.SessionToken != "")
	exp := "does not expire"
	if c1.CanExpire {
		exp = "expires " + c1.Expires.UTC().Format(time.RFC3339) + " (in " + time.Until(c1.Expires).Round(time.Second).String() + ")"
		r.Set("expires", c1.Expires.UTC())
	}
	r.Log("resolved via %s → %s in %s; key %s, session token %v, %s", c1.Source, sourceLabel(c1.Source),
		d1.Round(time.Millisecond), mask(c1.AccessKeyID), c1.SessionToken != "", exp)
	summary := sourceLabel(c1.Source)

	if e.O.Store == "aws" || e.O.STSEndpoint != "" || os.Getenv("AWS_ENDPOINT_URL_STS") != "" {
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		id, err := stsClient(e.AWS, e.O).GetCallerIdentity(cctx, &sts.GetCallerIdentityInput{})
		cancel()
		if err != nil {
			r.Log("STS GetCallerIdentity: %s", describe(err))
		} else {
			r.Set("arn", aws.ToString(id.Arn))
			r.Log("caller %q (account %q)", aws.ToString(id.Arn), aws.ToString(id.Account))
			if a := aws.ToString(id.Arn); a != "" {
				summary += "; " + a
			}
		}
	}

	k := e.Root + "/" + mode + ".txt"
	_, perr := e.put(ctx, putReq{Key: k, Body: "credcheck " + mode, INM: "*"})
	_, herr := e.head(ctx, k)
	_, _, gerr := e.get(ctx, k)
	derr := e.del(ctx, k, "")
	r.Log("S3 with these credentials: PUT If-None-Match:* %s; HEAD %s; GET %s; DELETE %s", describe(perr), describe(herr), describe(gerr), describe(derr))
	if perr != nil || herr != nil || gerr != nil {
		if _, st, _ := classify(firstErr(perr, herr, gerr)); st == 0 {
			r.Worsen(FAIL, summary+"; S3 unreachable: "+describe(firstErr(perr, herr, gerr)),
				"No HTTP answer: TLS (a private CA needs --ca-bundle / AWS_CA_BUNDLE for Go, SSL_CERT_FILE for "+
					"chDB/ClickHouse), DNS, a proxy (HTTPS_PROXY/NO_PROXY), or a NetworkPolicy blocking egress.")
			return
		}
		r.Worsen(FAIL, summary+"; S3 refused: "+describe(firstErr(perr, herr, gerr)),
			"Credentials resolve but S3 refuses them: the role's policy lacks the actions in RUNBOOK.md (IAM), the bucket "+
				"policy denies this principal, or (static keys) the key belongs to another Nutanix Objects account/bucket.")
		return
	}
	if derr != nil {
		r.Worsen(WARN, summary+"; DELETE refused", "GC needs s3:DeleteObject on the data prefix.")
	}

	// Forced refresh: back to the source, then use the new credentials.
	cache, ok := e.AWS.Credentials.(*aws.CredentialsCache)
	if !ok {
		r.Log("credentials are not behind aws.CredentialsCache (%T): refresh not exercised", e.AWS.Credentials)
		r.Worsen(PASS, summary+"; S3 OK", "")
		return
	}
	if !c1.CanExpire {
		r.Worsen(PASS, summary+"; S3 OK; static (no refresh)", "")
		return
	}
	cache.Invalidate()
	t1 := time.Now()
	cctx, cancel = context.WithTimeout(ctx, 15*time.Second)
	c2, err := cache.Retrieve(cctx)
	cancel()
	if err != nil {
		r.Worsen(FAIL, summary+"; refresh FAILED: "+describe(err),
			"The first fetch worked but a second does not: the source is single-use or rate-limited (a token file "+
				"that is not rotated, an agent that refuses repeats). Long-running exporters would fail at the first expiry.")
		return
	}
	_, herr = e.head(ctx, e.Root+"/"+mode+"-after-refresh-missing")
	ho, _, _ := classify(herr)
	r.Log("forced refresh: re-fetched from %s in %s; %s; new expiry %s; HEAD with them: %s", c2.Source,
		time.Since(t1).Round(time.Millisecond), map[bool]string{true: "new key", false: "same key"}[c2.AccessKeyID != c1.AccessKeyID],
		c2.Expires.UTC().Format(time.RFC3339), describe(herr))
	r.Set("refresh_ok", ho == NotFound || herr == nil)
	if ho == Forbidden {
		r.Worsen(FAIL, summary+"; refreshed credentials refused by S3", "Refresh yields credentials S3 does not accept.")
		return
	}
	r.Worsen(PASS, summary+"; S3 OK; forced refresh OK", "")
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// holdCreds keeps using the chain's client: each tick retrieves (from the
// cache; the SDK refreshes near expiry) and HEADs a key. It reports every
// change of key or expiry, i.e. every real refresh, and every error.
func holdCreds(ctx context.Context, e *Env, hold, every time.Duration, r *Result) {
	end := time.Now().Add(hold)
	var lastKey string
	var lastExp time.Time
	refreshes, errs, ticks := 0, 0, 0
	tk := time.NewTicker(every)
	defer tk.Stop()
	for {
		ticks++
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		c, err := e.AWS.Credentials.Retrieve(cctx)
		cancel()
		if err != nil {
			errs++
			r.Log("%s retrieve failed: %s", time.Now().UTC().Format(time.TimeOnly), describe(err))
		} else {
			if lastKey != "" && (c.AccessKeyID != lastKey || !c.Expires.Equal(lastExp)) {
				refreshes++
				r.Log("%s credentials refreshed: expiry %s → %s", time.Now().UTC().Format(time.TimeOnly),
					lastExp.UTC().Format(time.TimeOnly), c.Expires.UTC().Format(time.TimeOnly))
			}
			lastKey, lastExp = c.AccessKeyID, c.Expires
		}
		_, herr := e.S3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &e.O.Bucket, Key: aws.String(e.Root + "/hold-probe")})
		if o, _, _ := classify(herr); herr != nil && o != NotFound {
			errs++
			r.Log("%s HEAD: %s", time.Now().UTC().Format(time.TimeOnly), describe(herr))
		}
		if time.Now().After(end) || ctx.Err() != nil {
			break
		}
		select {
		case <-tk.C:
		case <-ctx.Done():
		}
	}
	r.Set("ticks", ticks)
	r.Set("refreshes", refreshes)
	r.Set("errors", errs)
	switch {
	case errs > 0:
		r.Worsen(FAIL, fmt.Sprintf("%d errors in %d requests over %s (%d refreshes)", errs, ticks, hold, refreshes),
			"Requests failed while holding credentials: if around a refresh, the exporters will see the same. "+
				"Check the evidence timestamps against the expiry.")
	case refreshes == 0:
		r.Worsen(INFO, fmt.Sprintf("%d requests over %s, no errors, no refresh happened (expiry %s)", ticks, hold, lastExp.UTC().Format(time.RFC3339)),
			"")
	default:
		r.Worsen(PASS, fmt.Sprintf("%d requests over %s, no errors, %d refresh(es) crossed", ticks, hold, refreshes), "")
	}
}
