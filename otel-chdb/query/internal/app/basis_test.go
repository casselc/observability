package app

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"

	"github.com/casselc/observability/otel-chdb/query/internal/basis"
)

const (
	arnOld = "arn:aws:kms:eu-west-1:111122223333:key/0ld00000-1111-2222-3333-444444444444"
	arnNew = "arn:aws:kms:eu-west-1:111122223333:key/9e900000-1111-2222-3333-444444444444"
)

// stubKMS answers DescribeKey (spec per key, or down) and MACs with one
// secret per key.
type stubKMS struct {
	down bool
	spec types.KeySpec
}

func (s *stubKMS) GenerateMac(_ context.Context, in *kms.GenerateMacInput, _ ...func(*kms.Options)) (*kms.GenerateMacOutput, error) {
	if s.down {
		return nil, errors.New("down")
	}
	m := hmac.New(sha256.New, []byte(aws.ToString(in.KeyId)))
	m.Write(in.Message)
	return &kms.GenerateMacOutput{Mac: m.Sum(nil)}, nil
}

func (s *stubKMS) VerifyMac(ctx context.Context, in *kms.VerifyMacInput, _ ...func(*kms.Options)) (*kms.VerifyMacOutput, error) {
	out, err := s.GenerateMac(ctx, &kms.GenerateMacInput{KeyId: in.KeyId, Message: in.Message})
	if err != nil {
		return nil, err
	}
	if !hmac.Equal(out.Mac, in.Mac) {
		return nil, &types.KMSInvalidMacException{}
	}
	return &kms.VerifyMacOutput{MacValid: true}, nil
}

func (s *stubKMS) DescribeKey(_ context.Context, in *kms.DescribeKeyInput, _ ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	if s.down {
		return nil, errors.New("dial tcp: no route to kms")
	}
	spec := s.spec
	if spec == "" {
		spec = types.KeySpecHmac256
	}
	return &kms.DescribeKeyOutput{KeyMetadata: &types.KeyMetadata{Arn: in.KeyId, KeySpec: spec, Enabled: true, KeyState: types.KeyStateEnabled,
		KeyUsage: types.KeyUsageTypeGenerateVerifyMac, MacAlgorithms: []types.MacAlgorithmSpec{types.MacAlgorithmSpecHmacSha256}}}, nil
}

func TestBasisSignerConfig(t *testing.T) {
	t.Setenv("QS_CH_PASSWORD", "x")
	t.Setenv("QS_BASIS_SIGNER", "kms")
	if _, err := Load("../../queryd.example.json"); err == nil || !strings.Contains(err.Error(), "kms_keys") {
		t.Fatalf("kms without keys: %v", err)
	}
	t.Setenv("QS_BASIS_SIGNER", "hsm")
	if _, err := Load("../../queryd.example.json"); err == nil {
		t.Fatal("an unknown signer was accepted")
	}
	t.Setenv("QS_BASIS_SIGNER", "kms")
	t.Setenv("QS_BASIS_KMS_KEYS", "old="+arnOld+", new="+arnNew+"*")
	c, err := Load("../../queryd.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Basis.KMSKeys) != 2 || c.Basis.KMSKeys[0].Current || !c.Basis.KMSKeys[1].Current || c.Basis.KMSKeys[1].Key != arnNew {
		t.Fatalf("%+v", c.Basis.KMSKeys)
	}
	if r, err := kmsRegion(c); err != nil || r != "eu-west-1" {
		t.Fatal(r, err)
	}
	// migration: the static keys verify, KMS mints
	static := "s1:" + base64.StdEncoding.EncodeToString([]byte(strings.Repeat("s", 32)))
	t.Setenv("QS_BASIS_KEYS", static)
	kr, _ := basis.ParseKeys(static, "")
	oldTok, _ := kr.Encode(basis.Basis{Clusters: map[string]uint64{"prod-a": 1}})
	api := &stubKMS{}
	bs, err := Bases(context.Background(), c, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bs.Current() != "k:new" {
		t.Fatal(bs.Current())
	}
	if _, err := bs.Open(context.Background(), oldTok); err != nil {
		t.Fatalf("a static token during the migration: %v", err)
	}
	b := basis.Basis{Clusters: map[string]uint64{"prod-a": 1}}
	tok, err := bs.Mint(context.Background(), &b)
	if err != nil || b.Kid != "k:new" {
		t.Fatal(err, b.Kid)
	}
	// startup: an unreachable KMS or a non-HMAC key fails it...
	for _, bad := range []*stubKMS{{down: true}, {spec: types.KeySpecSymmetricDefault}} {
		if _, err := Bases(context.Background(), c, bad, nil); err == nil {
			t.Errorf("started with %+v", bad)
		}
	}
	// ...unless start_without_signer: then minting is 503, not a crash
	c.Basis.StartWithoutSigner = true
	bs, err = Bases(context.Background(), c, &stubKMS{down: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bs.Mint(context.Background(), &b); !errors.Is(err, basis.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := bs.Open(context.Background(), tok); !errors.Is(err, basis.ErrUnavailable) {
		t.Fatal(err)
	}
	// keys in two regions: refused (one client)
	c.Basis.KMSKeys = append(c.Basis.KMSKeys, basis.KMSKey{ID: "far", Key: strings.Replace(arnOld, "eu-west-1", "us-east-1", 1)})
	if _, err := kmsRegion(c); err == nil {
		t.Fatal("two regions accepted")
	}
	// an alias only: the S3 region
	c.Basis.KMSKeys = []basis.KMSKey{{ID: "a", Key: "alias/qs-basis", Current: true}}
	c.S3.Region = "ap-southeast-2"
	if r, _ := kmsRegion(c); r != "ap-southeast-2" {
		t.Fatal(r)
	}
	// static stays the default, with the random key when none are set
	t.Setenv("QS_BASIS_SIGNER", "")
	t.Setenv("QS_BASIS_KMS_KEYS", "")
	t.Setenv("QS_BASIS_KEYS", "")
	c, err = Load("../../queryd.example.json")
	if err != nil || c.Basis.Signer != "static" {
		t.Fatal(err, c.Basis.Signer)
	}
	if bs, err := Bases(context.Background(), c, nil, nil); err != nil || bs.MintSigner() != "static" {
		t.Fatal(err)
	}
}
