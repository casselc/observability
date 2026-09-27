package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// checkListRace is LIST under concurrent writes, the way the consumer meets
// it (AMBIGUITY.md, row "LIST"): several lanes each append create-only slots
// in order while listers run LIST StartAfter over the whole prefix. A LIST
// is an observation, not an answer: a key it leaves out may be committed.
// What the consumer relies on, and what this measures:
//
//   - acked-visible: every key whose PUT was answered 200 before the LIST
//     was sent is in the result (LIST-after-write under concurrency);
//   - prefix: within one lane a result is a contiguous prefix of the slots
//     (slot n+1 is PUT only after slot n was acked, so a result holding n+1
//     without n shows a later write before an earlier one: the consumer
//     treats that as a gap and waits, so it costs latency, not data).
//
// Missing acked keys WARN (eventual LIST: add the lag to the visibility
// budget); a hole FAILs only if it persists after the writers stop.
func checkListRace(ctx context.Context, e *Env, r *Result) {
	writers := e.Params.RaceWriters
	if writers <= 0 {
		writers = 8
	}
	slots := e.Params.ConsistencyN * 2
	if slots <= 0 {
		slots = 60
	}
	listers := 4
	root := e.Key("list-race")
	laneKey := func(w, s int) string { return slotKey(root, fmt.Sprintf("w%02d", w), s) }

	// acked[w] = number of slots of lane w answered 200 so far.
	acked := make([]atomic.Int64, writers)
	var done atomic.Bool
	var wg sync.WaitGroup
	var putErrs atomic.Int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for s := 0; s < slots; s++ {
				if _, err := e.put(ctx, putReq{Key: laneKey(w, s), Body: "x", INM: "*"}); err != nil {
					putErrs.Add(1)
					return
				}
				acked[w].Store(int64(s + 1))
			}
		}(w)
	}
	type obs struct{ missingAcked, holes, lists int }
	var mu sync.Mutex
	var total obs
	var worstMissing string
	var lwg sync.WaitGroup
	for l := 0; l < listers; l++ {
		lwg.Add(1)
		go func() {
			defer lwg.Done()
			for !done.Load() {
				before := make([]int64, writers)
				for w := range before {
					before[w] = acked[w].Load()
				}
				keys, _, err := e.listKeys(ctx, root+"/", "", 1000)
				if err != nil {
					continue
				}
				seen := make([]map[int]bool, writers)
				for w := range seen {
					seen[w] = map[int]bool{}
				}
				for _, k := range keys {
					var w, s int
					rest := strings.TrimPrefix(k, root+"/")
					if _, err := fmt.Sscanf(rest, "w%02d/%d", &w, &s); err == nil && w < writers {
						seen[w][s] = true
					}
				}
				var o obs
				o.lists = 1
				for w := 0; w < writers; w++ {
					for s := 0; s < int(before[w]); s++ {
						if !seen[w][s] {
							o.missingAcked++
							mu.Lock()
							worstMissing = fmt.Sprintf("lane w%02d slot %d acked before the LIST, not in it", w, s)
							mu.Unlock()
						}
					}
					max := -1
					for s := range seen[w] {
						if s > max {
							max = s
						}
					}
					for s := 0; s < max; s++ {
						if !seen[w][s] {
							o.holes++
						}
					}
				}
				mu.Lock()
				total.missingAcked += o.missingAcked
				total.holes += o.holes
				total.lists += o.lists
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	done.Store(true)
	lwg.Wait()
	// After the writers stop, every acked key must be listed (eventually).
	final, _, err := e.listKeys(ctx, root+"/", "", 1000)
	want := 0
	for w := range acked {
		want += int(acked[w].Load())
	}
	r.Log("%d lanes × %d create-only slots written concurrently with %d listers: %d LISTs; acked keys missing from a LIST sent after their 200: %d; holes (slot n+1 listed without n): %d; PUT errors %d",
		writers, slots, listers, total.lists, total.missingAcked, total.holes, putErrs.Load())
	if worstMissing != "" {
		r.Log("  e.g. %s", worstMissing)
	}
	r.Set("list_race_lists", total.lists)
	r.Set("list_race_missing_acked", total.missingAcked)
	r.Set("list_race_holes", total.holes)
	switch {
	case err != nil:
		r.Worsen(FAIL, "final LIST: "+describe(err), listMeaning(err))
	case len(final) != want:
		r.Worsen(FAIL, fmt.Sprintf("after the writers stopped, LIST shows %d keys of %d acked", len(final), want),
			"LIST loses acked keys at rest: discovery cannot be trusted. HEAD-probe consecutive slots instead.")
	case putErrs.Load() > 0:
		r.Worsen(WARN, fmt.Sprintf("%d PUTs failed during the race", putErrs.Load()), "")
	case total.missingAcked > 0 || total.holes > 0:
		r.Worsen(WARN, fmt.Sprintf("LIST under concurrent writes: %d acked keys missing, %d holes, in %d LISTs", total.missingAcked, total.holes, total.lists),
			"LIST is eventually consistent under load. The consumer treats a missing slot as not yet visible and never skips a gap, "+
				"so this costs visibility latency; the closing tombstone is create-only and never relies on LIST.")
	default:
		r.Worsen(PASS, fmt.Sprintf("LIST under concurrent writes: %d LISTs, every acked key listed, no holes", total.lists), "")
	}
}
