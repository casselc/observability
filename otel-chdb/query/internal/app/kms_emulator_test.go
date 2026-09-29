package app

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"

	"github.com/casselc/observability/otel-chdb/query/internal/basis"
	"github.com/casselc/observability/otel-chdb/query/internal/store"

	"github.com/casselc/observability/otel-chdb/testgate"
)

// TestKMSEmulator runs the KMS signer against a KMS emulator (moto server:
// `moto_server -p 18555`, ci/kms-emulator.sh) at QS_TEST_KMS_ENDPOINT:
// the startup check (DescribeKey) on an HMAC key and on a symmetric
// encryption key, two replicas built the way the service builds them
// (app.Bases, the SDK chain, the configured endpoint) minting and verifying
// each other's tokens, tampering, and the latency of a mint, an uncached
// verify and a cached one. Skipped without the variable.
//
// With QS_TEST_KMS_KEYS="arnCurrent,arnPrevious" (two existing HMAC_256
// keys) and no endpoint it runs against AWS KMS under the ambient chain
// instead, creating nothing: deploy/validation/eks-aws.md EKS-13 runs it in
// a pod under the query service's role.
func TestKMSEmulator(t *testing.T) {
	ep := os.Getenv("QS_TEST_KMS_ENDPOINT")
	if real := os.Getenv("QS_TEST_KMS_KEYS"); real != "" && ep == "" {
		arns := strings.Split(real, ",")
		if len(arns) != 2 {
			t.Fatal("QS_TEST_KMS_KEYS: want arnCurrent,arnPrevious")
		}
		kmsRoundTrips(t, "", []basis.KMSKey{{ID: "cur", Key: arns[0], Current: true}, {ID: "prev", Key: arns[1]}},
			basis.KMSKey{ID: "prev", Key: arns[1], Current: true})
		return
	}
	if ep == "" {
		testgate.Skip(t, "kms", "QS_TEST_KMS_ENDPOINT not set (moto_server; ci/kms-emulator.sh)")
	}
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing", "AWS_REGION": "us-east-1"} {
		if os.Getenv(k) == "" {
			t.Setenv(k, v)
		}
	}
	ctx := context.Background()
	cfg, err := store.AWSConfig(ctx, "us-east-1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	admin := kms.NewFromConfig(cfg, func(o *kms.Options) { o.BaseEndpoint = aws.String(ep) })
	mk := func(spec types.KeySpec, usage types.KeyUsageType) string {
		out, err := admin.CreateKey(ctx, &kms.CreateKeyInput{KeySpec: spec, KeyUsage: usage, Description: aws.String("kms- basis emulator test")})
		if err != nil {
			t.Fatal(err)
		}
		return *out.KeyMetadata.Arn
	}
	hmacA := mk(types.KeySpecHmac256, types.KeyUsageTypeGenerateVerifyMac)
	hmacB := mk(types.KeySpecHmac256, types.KeyUsageTypeGenerateVerifyMac)
	sym := mk(types.KeySpecSymmetricDefault, types.KeyUsageTypeEncryptDecrypt)
	alias := "alias/kms-basis-test-" + time.Now().Format("150405.000000")[7:]
	if _, err := admin.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: aws.String(alias), TargetKeyId: aws.String(hmacA)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		admin.DeleteAlias(ctx, &kms.DeleteAliasInput{AliasName: aws.String(alias)})
		for _, k := range []string{hmacA, hmacB, sym} {
			admin.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyId: aws.String(k), PendingWindowInDays: aws.Int32(7)})
		}
	})
	// the startup check
	if _, err := Bases(ctx, kmsConf(ep, basis.KMSKey{ID: "s", Key: sym, Current: true}), nil, nil); err == nil {
		t.Fatal("a symmetric encryption key passed the startup check")
	}
	kmsRoundTrips(t, ep, []basis.KMSKey{{ID: "b", Key: hmacB, Current: true}, {ID: "a", Key: alias}},
		basis.KMSKey{ID: "a", Key: hmacA, Current: true})
}

func kmsConf(ep string, keys ...basis.KMSKey) *Config {
	c := &Config{}
	c.Basis.Signer, c.Basis.KMSEndpoint, c.Basis.KMSKeys = "kms", ep, keys
	c.Basis.MintReuseS = new(int)
	return c
}

// kmsRoundTrips: two replicas over keys mint and verify each other's
// tokens; tampering is a mismatch; a replica minting with prev alone makes
// tokens the others verify; latency logged.
func kmsRoundTrips(t *testing.T, ep string, keys []basis.KMSKey, prev basis.KMSKey) {
	ctx := context.Background()
	conf := func(keys ...basis.KMSKey) *Config { return kmsConf(ep, keys...) }
	r1, err := Bases(ctx, conf(keys...), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := Bases(ctx, conf(keys...), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	onlyA, err := Bases(ctx, conf(prev), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// replicas: minted on one, verified on the other (no shared cache)
	var mint, verify, cached []time.Duration
	for i := range 30 {
		b := basis.Basis{IssuedNs: int64(i), Clusters: map[string]uint64{"prod-a": uint64(1000 + i)}, Signals: []string{"logs"}}
		t0 := time.Now()
		tok, err := r1.Mint(ctx, &b)
		mint = append(mint, time.Since(t0))
		if err != nil {
			t.Fatal(err)
		}
		t0 = time.Now()
		got, err := r2.Open(ctx, tok)
		verify = append(verify, time.Since(t0))
		if err != nil || got.Clusters["prod-a"] != uint64(1000+i) {
			t.Fatal(err, got)
		}
		t0 = time.Now()
		if _, err := r2.Open(ctx, tok); err != nil {
			t.Fatal(err)
		}
		cached = append(cached, time.Since(t0))
		// tampered: a flipped MAC byte is a mismatch (KMSInvalidMacException), not an outage
		bad := tok[:len(tok)-3] + string("AB"[i%2]) + tok[len(tok)-2:]
		if bad != tok {
			if _, err := r2.Open(ctx, bad); !errors.Is(err, basis.ErrInvalid) {
				t.Fatalf("tampered: %v", err)
			}
		}
	}
	// a token under the alias's key verifies on a replica naming it by ARN
	b := basis.Basis{Clusters: map[string]uint64{basis.Fleet: 7}}
	tokA, err := onlyA.Mint(ctx, &b)
	switch {
	case ep == "":
		// AWS under the service role: deploy/iam/query-basis-kms.json grants
		// only VerifyMac on a previous key, so minting with it is refused
		if !errors.Is(err, basis.ErrUnavailable) {
			t.Fatalf("the role minted with a previous (verify-only) key: %v", err)
		}
	case err != nil:
		t.Fatal(err)
	default:
		if _, err := r1.Open(ctx, tokA); err != nil {
			t.Fatalf("verify-only key a (by alias) refused a token minted under its ARN: %v", err)
		}
	}
	p := func(d []time.Duration, q float64) time.Duration {
		s := append([]time.Duration(nil), d...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		return s[int(q*float64(len(s)-1))]
	}
	t.Logf("latency against %s (n=30): mint p50 %v p95 %v; uncached verify p50 %v p95 %v; cached verify p50 %v p95 %v",
		ep, p(mint, .5), p(mint, .95), p(verify, .5), p(verify, .95), p(cached, .5), p(cached, .95))
}
