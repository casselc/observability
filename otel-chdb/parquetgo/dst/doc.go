// Package dst holds the Go edge's verification tests that need more than
// the edge itself: the linearizability checks of the commit path's store
// histories (../../casreg, porcupine) and the deterministic simulation of
// the edge against the in-memory S3 emulator (../internal/s3emu) in fake
// time. It is its own module so that parquetgo, which the collector builds
// and nine other modules import, takes on no test-only dependency (a
// filesystem replace is not inherited: every importer would need one).
// research/go-verification.md, VERIFICATION.md.
package dst
