package parquetgo

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package's tests if a goroutine outlives them
// (research/go-verification.md §5). The tests write to S3 through SDK
// clients whose pools keep idle keep-alive connections (the SDK's
// transport is not ours to close); those connections' read and write loops
// are ignored, nothing else.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"))
}
