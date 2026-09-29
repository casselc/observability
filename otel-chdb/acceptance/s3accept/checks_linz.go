package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/casselc/observability/otel-chdb/casreg"
)

// checkLinearizable checks the whole run's history, as the client saw it,
// against one register per key under If-None-Match / If-Match (casreg):
// the checks above test their own outcomes, this one asks whether some
// single order of every read and write, answered or not, explains them
// all. An answer-less write may have applied or not. It relies on the
// store's single-part ETags being content MD5s (etag-md5 in create-only);
// keys changed by requests the model has no place for (multipart
// completions, copies, conditional DELETEs) are left out.
func checkLinearizable(_ context.Context, e *Env, r *Result) {
	if e.Hist == nil {
		r.Worsen(SKIP, "no history recorded", "")
		return
	}
	h := e.Hist.History()
	r.Set("operations", len(h))
	r.Set("pending_writes", e.Hist.Pending())
	res, info := e.Hist.Check(time.Minute)
	switch res {
	case porcupine.Ok:
		r.Log("%d operations: linearizable", len(h))
	case porcupine.Unknown:
		r.Worsen(WARN, fmt.Sprintf("the check timed out over %d operations", len(h)), "no violation found in the time given; not a pass")
	case porcupine.Illegal:
		path := filepath.Join(os.TempDir(), "s3accept-"+e.Run+"-linearizability.html")
		if err := porcupine.VisualizePath(casreg.Model(), info, path); err != nil {
			path = "(no timeline: " + err.Error() + ")"
		}
		r.Set("timeline", path)
		r.Worsen(FAIL, fmt.Sprintf("%d operations are not explained by any order of the writes (timeline %s)", len(h), path),
			"the store lost an update, served a stale read, or honoured a condition it should have refused: conditional writes are not atomic here")
	}
}
