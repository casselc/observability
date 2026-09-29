package hdxadapter

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package's tests if a goroutine outlives them: a
// worker, ticker, watcher or request that a stop or a cancel did not end
// (research/go-verification.md §5).
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
