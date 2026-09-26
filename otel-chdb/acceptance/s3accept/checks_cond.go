package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const fallback = "Use the Keeper-backed Coordinator from model/S3NATIVE.md section 9 for the control plane " +
	"(log slots, leases, checkpoint, gc.json); data objects can stay on this store with plain PUTs."

// ---- create-only ----

func checkCreateOnly(ctx context.Context, e *Env, r *Result) {
	k := e.Key("create-only", "obj")
	first := "first-" + e.Run
	etag, err := e.put(ctx, putReq{Key: k, Body: first, INM: "*"})
	r.Log("PUT If-None-Match:* on a new key: %s", describe(err))
	if o, _, _ := classify(err); o != OK {
		meaning := "create-only PUT is refused outright, so the manifest-less commit cannot run here. " + fallback
		if o == Forbidden {
			meaning = "403 on a create-only PUT: check IAM (s3:PutObject on the prefix) before judging the store; " +
				"conditional writes need no extra IAM action."
		}
		r.Worsen(FAIL, "create-only PUT on a new key failed: "+describe(err), meaning)
		return
	}
	if !sameETag(etag, md5ETag([]byte(first))) {
		r.Worsen(WARN, "ETag is not the body's MD5", "The ETag is not the MD5 of the body (SSE-KMS/SSE-C, or the "+
			"store's own ETag scheme). The design resolves ambiguous writes by comparing content or an embedded "+
			"token, never by MD5, so this is fine; do not add MD5-based resolution on this store.")
		r.Log("ETag %s, MD5 %s", etag, md5ETag([]byte(first)))
	} else {
		r.Log("ETag is the body's MD5 (%s)", etag)
	}
	_, err = e.put(ctx, putReq{Key: k, Body: "second-" + e.Run, INM: "*"})
	o2, st, code := classify(err)
	r.Log("second PUT If-None-Match:*, other body: %s", describe(err))
	_, err = e.put(ctx, putReq{Key: k, Body: first, INM: "*"})
	o3, _, _ := classify(err)
	r.Log("identical retry PUT If-None-Match:*: %s", describe(err))
	b, _, gerr := e.get(ctx, k)
	r.Log("GET after both: %q (%s)", string(b), describe(gerr))
	switch {
	case o2 == OK || o3 == OK:
		r.Worsen(FAIL, "If-None-Match:* was ignored: a second create returned 200",
			"The store accepts the header but does not enforce it (as MinIO before 2024-09 and Garage). "+
				"The manifest-less commit would silently overwrite log slots. "+fallback)
	case o2 != PreconditionFailed:
		r.Worsen(FAIL, fmt.Sprintf("second create answered HTTP %d %s, not 412", st, code),
			"A refused create must be distinguishable (412) from other errors; the writer treats anything else "+
				"as ambiguous and would retry forever. "+fallback)
	case string(b) != first:
		r.Worsen(FAIL, "412 returned but the body changed", "The condition is reported but not enforced. "+fallback)
	default:
		r.Worsen(PASS, "200, then 412 for a different body and for an identical retry; body unchanged", "")
	}
	r.Set("etag_is_md5", sameETag(etag, md5ETag([]byte(first))))
}

// ---- plain PUT (a bucket policy that demands conditional writes shows here) ----

func checkPlainPut(ctx context.Context, e *Env, r *Result) {
	k := e.Key("plain-put", "obj")
	_, err1 := e.put(ctx, putReq{Key: k, Body: "v1"})
	r.Log("unconditional PUT, new key: %s", describe(err1))
	_, err2 := e.put(ctx, putReq{Key: k, Body: "v2"})
	r.Log("unconditional PUT, overwrite: %s", describe(err2))
	o1, _, _ := classify(err1)
	o2, _, _ := classify(err2)
	switch {
	case o1 == OK && o2 == OK:
		r.Worsen(PASS, "unconditional PUTs allowed (new key and overwrite)", "")
	case o1 == Forbidden || o2 == Forbidden:
		r.Worsen(FAIL, "unconditional PUT refused with 403 ("+describe(err1)+" / "+describe(err2)+")",
			"Either IAM lacks s3:PutObject, or a bucket policy requires conditional writes "+
				"(s3:if-none-match / s3:if-match condition keys). chDB's s3() and s3_plain_rewritable disks, "+
				"ClickHouse INSERT INTO FUNCTION s3, and the checkpoint compaction's plain overwrites send no "+
				"conditional headers, so they would all be refused: exempt the data prefixes from the policy, or "+
				"publish data only through the Go/Rust writers with If-None-Match.")
	default:
		r.Worsen(FAIL, "unconditional PUT failed: "+describe(err1)+" / "+describe(err2), "Exporters cannot publish data objects.")
	}
}

// ---- If-Match CAS ----

func checkIfMatch(ctx context.Context, e *Env, r *Result) {
	k := e.Key("if-match", "obj")
	e1, err := e.put(ctx, putReq{Key: k, Body: "v1-" + e.Run, INM: "*"})
	if err != nil {
		e1, err = e.put(ctx, putReq{Key: k, Body: "v1-" + e.Run})
	}
	if err != nil {
		r.Worsen(FAIL, "setup PUT failed: "+describe(err), "")
		return
	}
	e2, err := e.put(ctx, putReq{Key: k, Body: "v2-" + e.Run, IM: e1})
	oCur, _, _ := classify(err)
	r.Log("PUT If-Match current ETag: %s (new ETag %s)", describe(err), e2)
	_, err = e.put(ctx, putReq{Key: k, Body: "v3-" + e.Run, IM: e1})
	oStale, _, _ := classify(err)
	r.Log("PUT If-Match stale ETag: %s", describe(err))
	_, err = e.put(ctx, putReq{Key: k, Body: "x", IM: `"00000000000000000000000000000000"`})
	oWrong, _, _ := classify(err)
	r.Log("PUT If-Match made-up ETag: %s", describe(err))
	b, cur, _ := e.get(ctx, k)
	r.Log("GET: %q ETag %s", string(b), cur)
	_, err = e.put(ctx, putReq{Key: e.Key("if-match", "missing"), Body: "x", IM: e1})
	oMiss, stMiss, _ := classify(err)
	r.Log("PUT If-Match on a missing key: %s (AWS documents 404)", describe(err))
	_, err = e.put(ctx, putReq{Key: k, Body: "v4-" + e.Run, IM: cur[1 : len(cur)-1]})
	oUnq, _, _ := classify(err)
	r.Log("PUT If-Match current ETag, unquoted: %s", describe(err))
	r.Set("missing_key_status", stMiss)
	switch {
	case oCur != OK:
		r.Worsen(FAIL, "If-Match with the current ETag was refused: "+fmt.Sprint(oCur),
			"CAS with the current ETag must succeed: leases, checkpoints and gc.json cannot advance. "+fallback)
		return
	case oStale == OK || oWrong == OK:
		r.Worsen(FAIL, "If-Match was ignored: a stale or made-up ETag replaced the object",
			"The store does not enforce If-Match: two consumers could both take a lease or advance a checkpoint. "+fallback)
		return
	case oStale != PreconditionFailed || oWrong != PreconditionFailed:
		r.Worsen(FAIL, "stale ETag answered "+oStale.String()+", not 412", "A refused CAS must be a 412. "+fallback)
		return
	case string(b) != "v2-"+e.Run:
		r.Worsen(FAIL, "body after the refused CAS is not the winner's", "CAS reported but not enforced. "+fallback)
		return
	case !sameETag(e2, cur):
		r.Worsen(WARN, "the ETag returned by the CAS PUT differs from GET's",
			"Chaining CASes on the PUT response's ETag would fail spuriously; re-read the ETag after each CAS (costs a GET).")
	}
	switch oMiss {
	case OK:
		r.Worsen(FAIL, "If-Match on a missing key created it",
			"A CAS on a deleted lease/checkpoint would recreate it; the consumer's takeover logic assumes it cannot. "+fallback)
	case NotFound:
	case PreconditionFailed:
		r.Worsen(WARN, "If-Match on a missing key answers 412 (AWS: 404)",
			"Harmless for the design (either is 'not applied'), but code must not read 412 as 'exists with another ETag'.")
	default:
		r.Worsen(WARN, "If-Match on a missing key answers "+oMiss.String(), "Treat it as 'not applied'.")
	}
	if oUnq != OK {
		r.Worsen(WARN, "an unquoted ETag in If-Match is refused",
			"Always send the ETag exactly as returned (quoted). The SDK paths do; hand-built requests must too.")
	}
	if r.Status == PASS || r.Status == "" {
		r.Worsen(PASS, "current 200, stale 412, made-up 412, missing "+oMiss.String()+", unquoted "+oUnq.String(), "")
	}
}

// ---- conditional DELETE ----

func checkCondDelete(ctx context.Context, e *Env, r *Result) {
	k := e.Key("cond-delete", "obj")
	e1, err := e.put(ctx, putReq{Key: k, Body: "v1"})
	if err != nil {
		r.Worsen(FAIL, "setup PUT: "+describe(err), "")
		return
	}
	_, _ = e.put(ctx, putReq{Key: k, Body: "v2"})
	err = e.del(ctx, k, e1)
	oStale, _, _ := classify(err)
	r.Log("DELETE If-Match stale ETag: %s", describe(err))
	_, err = e.head(ctx, k)
	stillThere := err == nil
	r.Log("object after the stale conditional DELETE: present=%v", stillThere)
	_, cur, _ := e.get(ctx, k)
	err = e.del(ctx, k, cur)
	oCur, _, _ := classify(err)
	r.Log("DELETE If-Match current ETag: %s", describe(err))
	_, err = e.head(ctx, k)
	gone := err != nil
	err = e.del(ctx, k, "*")
	oStarMissing, stStar, _ := classify(err)
	r.Log("DELETE If-Match:* on a missing key: %s (AWS: 412 or 404)", describe(err))
	r.Set("star_on_missing_status", stStar)

	const meaning = "Conditional DELETE is not required: GC deletes unconditionally below a CAS'd horizon " +
		"(model/S3NATIVE.md, Readers and GC; otap-rs consumer gc.rs). Never add code that relies on it on this store."
	switch {
	case oStale == OK && !stillThere:
		r.Worsen(WARN, "If-Match on DELETE is ignored: a stale ETag deleted the object", meaning)
	case oStale == NotImplemented || oCur == NotImplemented:
		r.Worsen(WARN, "conditional DELETE not implemented", meaning)
	case oStale != PreconditionFailed || !stillThere:
		r.Worsen(WARN, "stale conditional DELETE answered "+oStale.String(), meaning)
	case oCur != OK || !gone:
		r.Worsen(WARN, "DELETE If-Match current ETag did not delete ("+oCur.String()+")", meaning)
	default:
		r.Worsen(PASS, fmt.Sprintf("stale 412 (kept), current 204 (gone), * on missing %d", stStar), "")
		if oStarMissing == OK {
			r.Log("note: 204 for If-Match:* on a missing key is indistinguishable from a delete")
		}
	}
}

// ---- concurrent create race ----

func checkCreateRace(ctx context.Context, e *Env, r *Result) {
	n, rounds := e.Params.RaceWriters, e.Params.RaceRounds
	var bad, conflicts, others int
	winsHist := map[int]int{}
	for rd := 0; rd < rounds; rd++ {
		k := e.Key("create-race", fmt.Sprintf("slot-%03d", rd))
		var wins, conf, other atomic.Int32
		var winner atomic.Value
		var wg sync.WaitGroup
		start := make(chan struct{})
		var firstErr atomic.Value
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				b := fmt.Sprintf("writer-%d-%s", i, e.Run)
				_, err := e.putWith(ctx, e.racer(i), putReq{Key: k, Body: b, INM: "*"})
				switch o, _, _ := classify(err); o {
				case OK:
					wins.Add(1)
					winner.Store(b)
				case PreconditionFailed:
				case Conflict:
					conf.Add(1)
				default:
					other.Add(1)
					firstErr.CompareAndSwap(nil, describe(err))
				}
			}(i)
		}
		close(start)
		wg.Wait()
		w := int(wins.Load())
		winsHist[w]++
		conflicts += int(conf.Load())
		others += int(other.Load())
		got, _, _ := e.get(ctx, k)
		if w != 1 {
			bad++
			r.Log("round %d: %d winners (409s %d, other %d %v)", rd, w, conf.Load(), other.Load(), firstErr.Load())
		} else if string(got) != winner.Load() {
			bad++
			r.Log("round %d: stored %q but the 200 went to %q", rd, got, winner.Load())
		}
	}
	r.Set("rounds", rounds)
	r.Set("writers", n)
	r.Set("endpoints", len(e.Racers))
	if len(e.Racers) > 1 {
		r.Log("writers spread over %d endpoints (main + --race-endpoints)", len(e.Racers))
	}
	r.Set("winners_histogram", winsHist)
	r.Set("conflicts_409", conflicts)
	r.Set("other_errors", others)
	r.Log("winners per round histogram: %v; 409 ConditionalRequestConflict: %d; other errors: %d", winsHist, conflicts, others)
	switch {
	case bad > 0 && winsHist[0] == bad && others > 0:
		r.Worsen(FAIL, fmt.Sprintf("%d of %d rounds had no winner (errors, not 412s)", bad, rounds),
			"Writers got errors instead of a decision; the store may be throttling or failing under 16-way contention. "+
				"Re-run with fewer --race-writers; if it persists, conditional writes are not usable under contention. "+fallback)
	case bad > 0:
		r.Worsen(FAIL, fmt.Sprintf("%d of %d rounds did not have exactly one winner", bad, rounds),
			"Create-only is not atomic: the condition is checked outside the write, so two writers can both 'win' a "+
				"slot. The manifest-less commit (log slots, the epoch fence, tombstones) is unsafe here. "+fallback)
	default:
		r.Worsen(PASS, fmt.Sprintf("%d rounds × %d writers: exactly one winner each, stored body = winner's", rounds, n), "")
		if conflicts > 0 {
			r.Log("409s are allowed (AWS: retry); the writer treats them like a timeout and reads the slot")
		}
	}
}

// ---- CAS race with lost-update detection ----

func checkCASRace(ctx context.Context, e *Env, r *Result) {
	k := e.Key("cas-race", "counter")
	if _, err := e.put(ctx, putReq{Key: k, Body: "0"}); err != nil {
		r.Worsen(FAIL, "setup PUT: "+describe(err), "")
		return
	}
	n, per := e.Params.CASWriters, e.Params.CASIncrements
	var success, refused, conflicts atomic.Int32
	var dup atomic.Int32
	var errs sync.Map
	seen := sync.Map{}
	var wg sync.WaitGroup
	deadline := time.Now().Add(3 * time.Minute)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for done := 0; done < per && time.Now().Before(deadline); {
				b, etag, err := e.getWith(ctx, e.racer(i), k)
				if err != nil {
					errs.Store(describe(err), true)
					return
				}
				var v int
				fmt.Sscan(string(b), &v)
				_, err = e.putWith(ctx, e.racer(i), putReq{Key: k, Body: fmt.Sprint(v + 1), IM: etag})
				switch o, _, _ := classify(err); o {
				case OK:
					if _, loaded := seen.LoadOrStore(v+1, i); loaded {
						dup.Add(1)
					}
					success.Add(1)
					done++
				case PreconditionFailed:
					refused.Add(1)
				case Conflict:
					conflicts.Add(1)
				default:
					errs.Store(describe(err), true)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	b, _, _ := e.get(ctx, k)
	final := string(b)
	r.Log("%d writers × %d increments: %d CAS won, %d refused (412), %d 409, final value %s, values installed twice %d",
		n, per, success.Load(), refused.Load(), conflicts.Load(), final, dup.Load())
	errs.Range(func(k, _ any) bool { r.Log("error: %v", k); return true })
	r.Set("won", success.Load())
	r.Set("refused", refused.Load())
	r.Set("final", final)
	r.Set("duplicates", dup.Load())
	switch {
	case final != fmt.Sprint(success.Load()) || dup.Load() > 0:
		r.Worsen(FAIL, fmt.Sprintf("lost updates: %d successful CASes but the counter reads %s (%d values installed twice)",
			success.Load(), final, dup.Load()),
			"Two CASes on the same ETag both succeeded. Two consumer workers could both hold a lease or both "+
				"advance a checkpoint (duplicate inserts, skipped slots), and gc.json could lose a reader lease "+
				"(data deleted under a reader). "+fallback)
	case int(success.Load()) != n*per:
		r.Worsen(WARN, fmt.Sprintf("no lost updates, but only %d of %d increments completed", success.Load(), n*per),
			"Errors other than 412 under contention (see evidence). CAS is sound but the store struggles with "+
				"hot-object contention; keep CAS rates low (group commit, checkpoint every N slots).")
	default:
		r.Worsen(PASS, fmt.Sprintf("%d writers × %d: final %s = successes, none installed twice (%d refused)",
			n, per, final, refused.Load()), "")
	}
}

// ---- CAS racing a conditional DELETE (informational) ----

func checkCASvsDelete(ctx context.Context, e *Env, r *Result) {
	both, rounds := 0, 20
	for rd := 0; rd < rounds; rd++ {
		k := e.Key("cas-vs-delete", fmt.Sprintf("obj-%02d", rd))
		etag, err := e.put(ctx, putReq{Key: k, Body: "v0"})
		if err != nil {
			r.Worsen(FAIL, "setup PUT: "+describe(err), "")
			return
		}
		var pe, de error
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, pe = e.put(ctx, putReq{Key: k, Body: "v1", IM: etag}) }()
		go func() { defer wg.Done(); <-start; de = e.del(ctx, k, etag) }()
		close(start)
		wg.Wait()
		if pe == nil && de == nil {
			both++
		}
	}
	r.Log("%d rounds of a CAS racing a conditional DELETE on one ETag: both succeeded %d times", rounds, both)
	if both > 0 {
		r.Worsen(WARN, fmt.Sprintf("CAS and conditional DELETE on the same ETag both succeeded %d/%d times", both, rounds),
			"Conditional DELETE is not atomic with CAS. The design does not combine them; never delete a lease or checkpoint conditionally.")
	} else {
		r.Worsen(PASS, fmt.Sprintf("%d rounds: never both succeeded", rounds), "")
	}
}

// ---- ambiguous outcomes: cancel once the request is on the wire ----

// cancelOnWrite returns a context cancelled the moment the request has been
// written: the client always sees an error although the server has the whole
// request and may apply it (the exporter's timeout case).
func cancelOnWrite(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { cancel() }}), cancel
}

func checkAmbiguousCreate(ctx context.Context, e *Env, r *Result) {
	n := e.Params.AmbiguousN
	landed, notLanded, notAmbiguous, lateLanded := 0, 0, 0, 0
	for i := 0; i < n; i++ {
		k := e.Key("ambiguous-create", fmt.Sprintf("slot-%03d", i))
		body := fmt.Sprintf(`{"kind":"commit","epoch":1,"content":"h%d","run":"%s"}`, i, e.Run)
		cctx, cancel := cancelOnWrite(ctx)
		_, err := e.put(cctx, putReq{Key: k, Body: body, INM: "*"})
		cancel()
		if err == nil {
			notAmbiguous++
			continue
		}
		// The design: read the slot; if free, resend the identical request.
		_, herr := e.head(ctx, k)
		freeAtFirstLook := herr != nil
		time.Sleep(e.Params.Settle)
		_, err = e.put(ctx, putReq{Key: k, Body: body, INM: "*"})
		switch o, _, _ := classify(err); o {
		case OK:
			notLanded++
		case PreconditionFailed, Conflict:
			got, _, gerr := e.get(ctx, k)
			if gerr != nil || string(got) != body {
				r.Worsen(FAIL, fmt.Sprintf("attempt %d: retry got 412 but the slot holds %q", i, got),
					"Someone else's object is in a slot only this writer targeted: the store applied a request "+
						"the client never sent, or corrupted it. The timeout rule is unsound here. "+fallback)
				return
			}
			landed++
			if freeAtFirstLook {
				lateLanded++
			}
		default:
			r.Worsen(FAIL, fmt.Sprintf("attempt %d: identical retry answered %s", i, describe(err)),
				"The retry of an ambiguous create must be decided (200 or 412). "+fallback)
			return
		}
	}
	r.Log("%d create-only PUTs cancelled once written: %d had landed (retry 412, body ours), %d had not (retry created it), "+
		"%d answered before the cancel; %d looked free right after the cancel but had landed by the retry (late landing)",
		n, landed, notLanded, notAmbiguous, lateLanded)
	r.Set("landed", landed)
	r.Set("not_landed", notLanded)
	r.Set("late_landed", lateLanded)
	r.Set("not_ambiguous", notAmbiguous)
	r.Worsen(PASS, fmt.Sprintf("%d ambiguous creates, each resolved exactly once (%d had landed, %d had not, %d late)",
		landed+notLanded, landed, notLanded, lateLanded), "")
	if lateLanded > 0 {
		r.Log("late landing is expected and handled: the in-flight copy and the resend cannot both apply")
	}
}

func checkAmbiguousCAS(ctx context.Context, e *Env, r *Result) {
	k := e.Key("ambiguous-cas", "head")
	etag, err := e.put(ctx, putReq{Key: k, Body: "v0-" + e.Run})
	if err != nil {
		r.Worsen(FAIL, "setup PUT: "+describe(err), "")
		return
	}
	n := e.Params.AmbiguousN
	won, lost, notAmb := 0, 0, 0
	for i := 1; i <= n; i++ {
		body := fmt.Sprintf("v%d-%s", i, e.Run) // a unique token: no ABA
		cctx, cancel := cancelOnWrite(ctx)
		ne, err := e.put(cctx, putReq{Key: k, Body: body, IM: etag})
		cancel()
		if err == nil {
			notAmb++
			etag = ne
			continue
		}
		time.Sleep(e.Params.Settle)
		ne, err = e.put(ctx, putReq{Key: k, Body: body, IM: etag})
		switch o, _, _ := classify(err); o {
		case OK:
			lost++
			etag = ne
		case PreconditionFailed, Conflict:
			got, cur, _ := e.get(ctx, k)
			if string(got) != body {
				r.Worsen(FAIL, fmt.Sprintf("attempt %d: 412 on retry but the object holds %q, not ours", i, got),
					"An ambiguous CAS by the only writer was neither applied nor still pending: the store lost or "+
						"reordered it. Checkpoint/lease resolution by read-back is unsound here. "+fallback)
				return
			}
			won++
			etag = cur
		default:
			r.Worsen(FAIL, fmt.Sprintf("attempt %d: retry answered %s", i, describe(err)), fallback)
			return
		}
	}
	r.Log("%d If-Match CASes cancelled once written: %d had applied (412 on retry, body ours), %d had not (retry applied), %d answered before the cancel",
		n, won, lost, notAmb)
	r.Set("applied", won)
	r.Set("not_applied", lost)
	r.Worsen(PASS, fmt.Sprintf("%d ambiguous CASes resolved by read-back (%d had applied, %d had not)", won+lost, won, lost), "")
}

// ---- conditional CompleteMultipartUpload (informational; the design does not use it) ----

func (e *Env) startUpload(ctx context.Context, k string, body []byte) (*string, []types.CompletedPart, error) {
	cr, err := e.S3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &e.O.Bucket, Key: &k})
	if err != nil {
		return nil, nil, err
	}
	pn := int32(1)
	// One part: the last part may be smaller than 5 MB, and a single part is
	// the last. Keeps the probe light on AWS and on a local store.
	up, err := e.S3.UploadPart(ctx, &s3.UploadPartInput{Bucket: &e.O.Bucket, Key: &k, UploadId: cr.UploadId,
		PartNumber: &pn, Body: bytes.NewReader(body)})
	if err != nil {
		e.abort(ctx, k, cr.UploadId)
		return nil, nil, err
	}
	return cr.UploadId, []types.CompletedPart{{ETag: up.ETag, PartNumber: aws.Int32(1)}}, nil
}

func (e *Env) complete(ctx context.Context, k string, id *string, parts []types.CompletedPart, inm, im string) error {
	in := &s3.CompleteMultipartUploadInput{Bucket: &e.O.Bucket, Key: &k, UploadId: id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}
	if inm != "" {
		in.IfNoneMatch = aws.String(inm)
	}
	if im != "" {
		in.IfMatch = aws.String(im)
	}
	_, err := e.S3.CompleteMultipartUpload(ctx, in)
	return err
}

func (e *Env) abort(ctx context.Context, k string, id *string) {
	_, _ = e.S3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &e.O.Bucket, Key: &k, UploadId: id})
}

func checkMPU(ctx context.Context, e *Env, r *Result) {
	const meaning = "Conditional multipart completion is not atomic on this store (SeaweedFS 4.47 has the same " +
		"check-then-act race). The design does not use it: log entries, leases and checkpoints are single-part " +
		"PUTs, and data objects (100 KB-1 MB) are single PUTs. Keep it that way: never raise the exporters' " +
		"multipart threshold below the object size, and never make a control object multipart."
	k := e.Key("mpu", "seq")
	id, parts, err := e.startUpload(ctx, k, []byte("first"))
	if err != nil {
		o, _, _ := classify(err)
		if o == Forbidden {
			r.Worsen(SKIP, "multipart upload refused (403): "+describe(err), "")
			return
		}
		r.Worsen(WARN, "multipart upload failed: "+describe(err), meaning)
		return
	}
	err = e.complete(ctx, k, id, parts, "*", "")
	r.Log("complete If-None-Match:* on a new key: %s", describe(err))
	seqNew, _, _ := classify(err)
	id2, parts2, err := e.startUpload(ctx, k, []byte("second"))
	if err == nil {
		err = e.complete(ctx, k, id2, parts2, "*", "")
		r.Log("complete If-None-Match:* on an existing key: %s", describe(err))
		e.abort(ctx, k, id2)
	}
	seqExist, _, _ := classify(err)

	n, rounds := e.Params.MPUWriters, e.Params.MPURounds
	var hist []int
	bad := 0
	for rd := 0; rd < rounds; rd++ {
		k := e.Key("mpu", fmt.Sprintf("race-%02d", rd))
		ids := make([]*string, n)
		ps := make([][]types.CompletedPart, n)
		for i := range ids {
			ids[i], ps[i], err = e.startUpload(ctx, k, []byte(fmt.Sprintf("writer-%d", i)))
			if err != nil {
				r.Worsen(WARN, "upload setup failed: "+describe(err), meaning)
				return
			}
		}
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range ids {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				if e.complete(ctx, k, ids[i], ps[i], "*", "") == nil {
					wins.Add(1)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		for i := range ids {
			e.abort(ctx, k, ids[i]) // losers stay in progress on AWS; abort them
		}
		hist = append(hist, int(wins.Load()))
		if wins.Load() != 1 {
			bad++
		}
	}
	r.Log("%d rounds × %d concurrent If-None-Match:* completions on one key: winners per round %v", rounds, n, hist)
	r.Set("winners_per_round", hist)
	switch {
	case seqNew != OK || seqExist != PreconditionFailed:
		r.Worsen(FAIL, fmt.Sprintf("sequential conditional completion: new %v, existing %v (want 200, 412)", seqNew, seqExist), meaning)
	case bad > 0:
		r.Worsen(FAIL, fmt.Sprintf("not atomic: winners per round %v (want 1 each)", hist), meaning)
	default:
		r.Worsen(PASS, fmt.Sprintf("sequential 200/412; race: exactly one winner in each of %d rounds", rounds), "")
	}
}
