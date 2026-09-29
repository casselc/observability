package commit

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// ValidName's boundaries (a key segment: FORMAT.md §1), each side of each
// rule, so a mutated comparison or edge test fails here, not only in the
// edge's tests (nightly `mutants`: gremlins saw these lines uncovered).
func TestValidNameBoundaries(t *testing.T) {
	for s, want := range map[string]bool{
		"": false, "a": true, "0": true, "-": false, ".": false, "_": false, "A": false, "a/b": false,
		"a-b": true, "a.b": true, "a_b": true, "-a": false, "a-": false, "_a": false, "a_": false, ".a": false, "a.": false,
		"aZb": false, "a b": false, "a\x00b": false, "z9": true, "9z": true, "é": false, "a`b": false, "a{b": false, "a:b": false, "a@b": false,
		strings.Repeat("a", 63): true, strings.Repeat("a", 64): false, "a" + strings.Repeat("-", 61) + "a": true,
	} {
		if got := ValidName(s); got != want {
			t.Errorf("ValidName(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestLanePrefix(t *testing.T) {
	for _, c := range [][2]string{{"", "c/p/traces"}, {"/", "c/p/traces"}, {"r", "r/c/p/traces"}, {"/r/s/", "r/s/c/p/traces"}} {
		if got := LanePrefix(c[0], "c", "p", "traces"); got != c[1] {
			t.Errorf("LanePrefix(%q) = %q, want %q", c[0], got, c[1])
		}
	}
}

// Classify: only a 412 (by status or by code) is "exists"; every other
// answer, and no answer, is unknown: never OK, never exists by guess.
func TestClassify(t *testing.T) {
	status := func(code int) error {
		return &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: code}}, Err: errors.New("x")}
	}
	for name, c := range map[string]struct {
		err  error
		want PutOutcome
	}{
		"nil":         {nil, PutOK},
		"412":         {status(http.StatusPreconditionFailed), PutExists},
		"412 wrapped": {fmt.Errorf("put: %w", status(412)), PutExists},
		"code":        {&smithy.GenericAPIError{Code: "PreconditionFailed"}, PutExists},
		"409":         {status(http.StatusConflict), PutUnknown},
		"413":         {status(413), PutUnknown},
		"500":         {status(500), PutUnknown},
		"other code":  {&smithy.GenericAPIError{Code: "ConditionalRequestConflict"}, PutUnknown},
		"no answer":   {errors.New("connection reset"), PutUnknown},
	} {
		if got := Classify(c.err); got != c.want {
			t.Errorf("%s: Classify = %v, want %v", name, got, c.want)
		}
	}
}
