package basis

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
)

// kmsKidPrefix starts every KMS key id in a token: "k:" + a configured id.
// A static kid never contains ':' (kidRE), so the two spaces are disjoint.
const kmsKidPrefix = "k:"

// KMSKey is one configured KMS key (basis.kms_keys[]): ID is the short name
// tokens carry (as "k:" + ID), Key the key ARN or alias it names, Current
// the one new bases are minted under (at most one; the others verify).
type KMSKey struct {
	ID      string `json:"id"`
	Key     string `json:"key"`
	Current bool   `json:"current"`
}

// KMSAPI is what KMSSigner needs from the AWS KMS client.
type KMSAPI interface {
	GenerateMac(ctx context.Context, in *kms.GenerateMacInput, opts ...func(*kms.Options)) (*kms.GenerateMacOutput, error)
	VerifyMac(ctx context.Context, in *kms.VerifyMacInput, opts ...func(*kms.Options)) (*kms.VerifyMacOutput, error)
	DescribeKey(ctx context.Context, in *kms.DescribeKeyInput, opts ...func(*kms.Options)) (*kms.DescribeKeyOutput, error)
}

var (
	kmsIDRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,30}$`)
	// a key or alias ARN, or an alias name; never a bare key id (which
	// names a key only in the caller's own account and region, silently)
	kmsKeyRE = regexp.MustCompile(`^(arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:(key/[A-Za-z0-9-]+|alias/[A-Za-z0-9/_-]+)|alias/[A-Za-z0-9/_-]+)$`)
)

// KMSSigner MACs with AWS KMS HMAC_SHA_256 keys (GenerateMac, VerifyMac).
// The key material never leaves KMS; every replica with the role's
// permission computes the same MAC. Only the configured keys are ever
// used: the kid a token carries selects among them and is never passed to
// KMS itself, so a caller cannot make the service call VerifyMac on a key
// of its choosing.
//
// The message KMS MACs is a domain-separated SHA-256 of "b1." + payload,
// not the payload itself: GenerateMac and VerifyMac take at most 4,096
// bytes and a token may carry 16 KiB. HMAC over a collision-resistant
// digest is as strong as HMAC over the message for this use.
type KMSSigner struct {
	api     KMSAPI
	keys    map[string]string // kid ("k:" + id) -> key ARN or alias (resolved to the key ARN by Check)
	current string
	timeout time.Duration
}

// NewKMSSigner checks keys (at least one; ids unique; at most one current;
// references key or alias ARNs or alias names). timeout bounds each call
// (default 2 s): a KMS that does not answer is an outage, not a hang.
func NewKMSSigner(api KMSAPI, keys []KMSKey, timeout time.Duration) (*KMSSigner, error) {
	if api == nil {
		return nil, errors.New("basis kms: no client")
	}
	if len(keys) == 0 {
		return nil, errors.New("basis kms: basis.kms_keys is empty")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	s := &KMSSigner{api: api, keys: map[string]string{}, timeout: timeout}
	for _, k := range keys {
		if !kmsIDRE.MatchString(k.ID) {
			return nil, fmt.Errorf("basis kms: key id %q: want %s", k.ID, kmsIDRE)
		}
		if !kmsKeyRE.MatchString(k.Key) {
			return nil, fmt.Errorf("basis kms: key %q (%s): want a key ARN, an alias ARN or alias/NAME", k.ID, k.Key)
		}
		kid := kmsKidPrefix + k.ID
		if _, dup := s.keys[kid]; dup {
			return nil, fmt.Errorf("basis kms: key id %q configured twice", k.ID)
		}
		s.keys[kid] = k.Key
		if k.Current {
			if s.current != "" {
				return nil, errors.New("basis kms: more than one key is current")
			}
			s.current = kid
		}
	}
	return s, nil
}

// Kind implements Signer.
func (s *KMSSigner) Kind() string { return "kms" }

// Current implements Signer.
func (s *KMSSigner) Current() string { return s.current }

// Holds implements Signer.
func (s *KMSSigner) Holds(kid string) bool { _, ok := s.keys[kid]; return ok }

// Kids are the configured kids, sorted.
func (s *KMSSigner) Kids() []string {
	out := make([]string, 0, len(s.keys))
	for k := range s.keys {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// kmsMessage is what KMS MACs for msg ("b1." + payload).
func kmsMessage(msg []byte) []byte {
	d := sha256.Sum256(msg)
	return append([]byte("otel-chdb basis v1 sha256\x00"), d[:]...)
}

// MAC implements Signer.
func (s *KMSSigner) MAC(ctx context.Context, kid string, msg []byte) ([]byte, error) {
	key, ok := s.keys[kid]
	if !ok {
		return nil, fmt.Errorf("basis kms: no configured key %q", kid)
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	out, err := s.api.GenerateMac(ctx, &kms.GenerateMacInput{KeyId: &key, MacAlgorithm: types.MacAlgorithmSpecHmacSha256, Message: kmsMessage(msg)})
	if err != nil {
		return nil, err
	}
	if len(out.Mac) == 0 {
		return nil, errors.New("basis kms: GenerateMac returned no MAC")
	}
	return out.Mac, nil
}

// Verify implements Signer: KMSInvalidMacException (or MacValid false) is
// ErrMismatch; every other failure is the signer's, never the token's.
func (s *KMSSigner) Verify(ctx context.Context, kid string, msg, mac []byte) error {
	key, ok := s.keys[kid]
	if !ok {
		return ErrMismatch
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	out, err := s.api.VerifyMac(ctx, &kms.VerifyMacInput{KeyId: &key, MacAlgorithm: types.MacAlgorithmSpecHmacSha256, Message: kmsMessage(msg), Mac: mac})
	if err != nil {
		var bad *types.KMSInvalidMacException
		if errors.As(err, &bad) {
			return ErrMismatch
		}
		return err
	}
	if !out.MacValid {
		return ErrMismatch
	}
	return nil
}

// Check describes every configured key (DescribeKey): each must be
// enabled, GENERATE_VERIFY_MAC, HMAC_256 and support HMAC_SHA_256. An
// alias is resolved to its key's ARN here, and the ARN is used from then
// on, so re-pointing an alias does not change a running replica's key
// (a restarted one takes the new key, and the old key's tokens under that
// id then fail: name keys by key ARN).
func (s *KMSSigner) Check(ctx context.Context) error {
	var errs []string
	for _, kid := range s.Kids() {
		key := s.keys[kid]
		cctx, cancel := context.WithTimeout(ctx, s.timeout)
		out, err := s.api.DescribeKey(cctx, &kms.DescribeKeyInput{KeyId: &key})
		cancel()
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s (%s): DescribeKey: %v", kid, key, err))
			continue
		}
		m := out.KeyMetadata
		switch {
		case m == nil:
			errs = append(errs, fmt.Sprintf("%s (%s): no key metadata", kid, key))
		case m.KeySpec != types.KeySpecHmac256:
			errs = append(errs, fmt.Sprintf("%s (%s): key spec %s, want HMAC_256", kid, key, m.KeySpec))
		case m.KeyUsage != types.KeyUsageTypeGenerateVerifyMac:
			errs = append(errs, fmt.Sprintf("%s (%s): key usage %s, want GENERATE_VERIFY_MAC", kid, key, m.KeyUsage))
		case !m.Enabled || m.KeyState != types.KeyStateEnabled:
			errs = append(errs, fmt.Sprintf("%s (%s): key state %s", kid, key, m.KeyState))
		// HMAC_256 keys support exactly HMAC_SHA_256; moto 5.2 leaves the
		// list empty, so only a list that omits it is refused
		case len(m.MacAlgorithms) > 0 && !slices.Contains(m.MacAlgorithms, types.MacAlgorithmSpecHmacSha256):
			errs = append(errs, fmt.Sprintf("%s (%s): HMAC_SHA_256 not supported", kid, key))
		default:
			if m.Arn != nil && *m.Arn != "" {
				s.keys[kid] = *m.Arn
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("basis kms: %s", strings.Join(errs, "; "))
	}
	return nil
}
