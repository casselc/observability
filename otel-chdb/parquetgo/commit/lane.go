package commit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Slot is what a HEAD of a slot found.
type Slot struct {
	Kind    SlotKind
	Epoch   string // Data only
	Content string // Data only
}

type SlotKind int

const (
	Free SlotKind = iota
	Data
	Tomb
)

func (k SlotKind) String() string { return [...]string{"free", "data", "tomb"}[k] }

// SlotFromMeta reads a slot's user metadata (lower-case keys without x-amz-meta-).
func SlotFromMeta(m map[string]string) Slot {
	if m[MetaKind] == KindTomb {
		return Slot{Kind: Tomb}
	}
	return Slot{Kind: Data, Epoch: m[MetaEpoch], Content: m[MetaContent]}
}

// PutOutcome is a create-only PUT's answer.
type PutOutcome int

const (
	PutOK      PutOutcome = iota // 200: created
	PutExists                    // 412: the key is taken
	PutUnknown                   // no answer (timeout, reset, 5xx): may have applied, may still apply, or never will
)

func (o PutOutcome) String() string { return [...]string{"ok", "exists", "unknown"}[o] }

// Store is what the protocol needs from a bucket.
type Store interface {
	// PutCreate is PUT If-None-Match: * with user metadata.
	PutCreate(ctx context.Context, key string, body []byte, contentType string, meta map[string]string) PutOutcome
	// Head returns the object's user metadata; found is false on 404.
	Head(ctx context.Context, key string) (meta map[string]string, found bool, err error)
}

// Object is one encoded batch for one slot.
type Object struct {
	Body        []byte
	ContentType string
	// Meta is the description the lane doesn't set itself (signal, rows,
	// times, received, schema); the lane adds kind, epoch, seq, content and
	// producer.
	Meta map[string]string
}

// Ref is where a batch is committed.
type Ref struct {
	Epoch string
	Seq   uint64
}

// Encoder encodes the batch for slot r. It is called again only when the
// batch needs bytes for a slot it has none for (another batch or a
// tombstone took the one it was encoded for), so envelope columns can carry
// the epoch and the slot.
type Encoder func(r Ref) (Object, error)

// Phase is a lane's state (../../otap-rs/src/proto.rs `Phase`).
type Phase int

const (
	Idle       Phase = iota // no batch in hand
	Ready                   // a batch is assigned to slot next; the PUT is to be sent
	Waiting                 // the PUT is out
	Unresolved              // the PUT got no answer: the slot must be read before anything else
	Halted                  // the consumer closed this log
)

func (p Phase) String() string {
	return [...]string{"idle", "ready", "waiting", "unresolved", "halted"}[p]
}

// Mutation is a deliberate protocol bug, for showing that the model checks catch it.
type Mutation int

const (
	NoMutation  Mutation = iota
	RetryNewKey          // a 412 or a timeout moves to the next slot without reading it
	NoHalt               // a tombstone in our slot is skipped
	AckOnAny             // a request is ACKed once any of its objects committed
)

// Stats counts one model event each.
type Stats struct {
	Committed, ResolvedOwn, Resent, LearnedOther, Halted, KnownSkipped atomic.Int64
	Encodes, Puts, Heads                                               atomic.Int64
	// Unresolved: the HEAD that should resolve a slot failed. Inconsistent:
	// 412, then the HEAD found the slot free. Both leave the slot unresolved.
	Unresolved, Inconsistent atomic.Int64
}

// Event is one protocol step, for an Observer (the quintgo trace recorder).
type Event struct {
	Lane     string // "{signal}/{lane index}"
	Kind     string // start, known, put, putResult, head, committed, resolvedOwn, resend, learnedOther, halted, inconsistent, headError, resendLimit
	Ref      Ref
	Content  string
	Outcome  PutOutcome // putResult
	Found    Slot       // head
	NewEpoch string     // halted
	Err      string
	// The lane's state when the event is emitted.
	Epoch string
	Next  uint64
	Phase Phase
}

// Observer receives every Event, in order per lane.
type Observer interface{ Observe(Event) }

// ErrUnresolved: the commit's outcome is not known yet. The lane keeps the
// slot, and the next append (a retry of this batch, or another batch)
// resolves it first. Retryable.
type ErrUnresolved struct{ Msg string }

func (e *ErrUnresolved) Error() string { return "commit unresolved: " + e.Msg }

// ErrEncode: the batch cannot be encoded. Permanent.
type ErrEncode struct{ Err error }

func (e *ErrEncode) Error() string { return "encode: " + e.Err.Error() }
func (e *ErrEncode) Unwrap() error { return e.Err }

const recentCap = 4096

// MaxResends bounds the resends of one Append (a PUT without an answer,
// then a HEAD that finds the slot free): past it the call returns
// *ErrUnresolved, and the lane stays at the slot for the next call. Without
// it a store that fails every PUT (a full SeaweedFS volume, 503 SlowDown),
// or a caller whose deadline has passed (every PUT then fails at once),
// spins on the slot forever, holding the lane and its heartbeats. The
// Rust edge's runner.rs MAX_RESENDS.
const MaxResends = 8

// Timeouts bound the PUT and the HEAD that resolves it.
type Timeouts struct{ Put, Head time.Duration }

// Lane is one writer lane: one epoch's log, appended one batch at a time.
// Append serializes on the lane.
type Lane struct {
	Name     string // for events: "{signal}/{index}"
	Prefix   string // {root}/{cluster}/{producer}/{signal} (LanePrefix)
	Producer string
	Store    Store
	Timeouts Timeouts
	Stats    *Stats
	Observer Observer
	Mutation Mutation
	// NewEpoch names epochs (default NewEpoch); tests pin it.
	NewEpoch func() string

	mu sync.Mutex
	// Epoch is named at the lane's first write, not when the lane is made:
	// an idle lane's first slot must not land under a name older than the
	// epochs the consumer has since closed and compacted past
	// (../../otap-rs/src/runner.rs).
	epoch    string
	next     uint64
	phase    Phase
	entry    string // content hash of the batch in hand
	after412 bool
	recent   map[string]Ref
	order    []string
	// The last encoding, so a resend, or a retry of the same batch into the
	// same slot, sends identical bytes.
	cacheContent string
	cacheRef     Ref
	cacheObj     *Object
}

func (l *Lane) emit(e Event) {
	if l.Observer != nil {
		e.Lane, e.Epoch, e.Next, e.Phase = l.Name, l.epoch, l.next, l.phase
		l.Observer.Observe(e)
	}
}

// Epoch is the lane's current epoch ("" before its first write).
func (l *Lane) Epoch() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch
}

// State is the lane's (epoch, next slot, phase), for tests and observers.
func (l *Lane) State() (string, uint64, Phase) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch, l.next, l.phase
}

// Known reports where a content hash was found committed by this lane.
func (l *Lane) Known(content string) (Ref, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.recent[content]
	return r, ok
}

func (l *Lane) remember(content string, at Ref) {
	if l.recent == nil {
		l.recent = map[string]Ref{}
	}
	if _, ok := l.recent[content]; ok {
		return
	}
	l.recent[content] = at
	l.order = append(l.order, content)
	if len(l.order) > recentCap {
		delete(l.recent, l.order[0])
		l.order = l.order[1:]
	}
}

func (l *Lane) slot() Ref { return Ref{Epoch: l.epoch, Seq: l.next} }

func (l *Lane) newEpoch() string {
	if l.NewEpoch != nil {
		return l.NewEpoch()
	}
	return NewEpoch()
}

// own: the batch in hand is committed at slot().
func (l *Lane) own() Ref {
	at := l.slot()
	l.remember(l.entry, at)
	l.entry = ""
	l.next++
	l.phase = Idle
	l.after412 = false
	return at
}

func inc(c *atomic.Int64) { c.Add(1) }

// Append commits the batch with content hash content at the next free slot
// of the lane's log (../../otap-rs/src/runner.rs `append`). It returns once
// the batch is committed (by this call, by an earlier attempt whose answer
// was lost, or found already committed); with *ErrUnresolved while the
// outcome is unknown (call again with the same content to resolve it, never
// committing twice); with *ErrEncode if enc fails.
func (l *Lane) Append(ctx context.Context, content string, enc Encoder) (Ref, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.Stats
	if st == nil {
		st = &Stats{}
		l.Stats = st
	}
	// start: a batch this lane already found committed is done at once. From
	// Unresolved the new (or the same) batch goes to the same slot: the
	// earlier request may still land, and whichever lands first wins (the
	// model's switchPayload).
	if r, ok := l.recent[content]; ok {
		inc(&st.KnownSkipped)
		l.emit(Event{Kind: "known", Ref: r, Content: content})
		return r, nil
	}
	l.entry = content
	l.phase = Ready
	l.after412 = false
	l.emit(Event{Kind: "start", Content: content})
	resends := 0
	for {
		if l.epoch == "" {
			l.epoch = l.newEpoch()
		}
		here := l.slot()
		if l.cacheObj == nil || l.cacheContent != content || l.cacheRef != here {
			obj, err := enc(here)
			if err != nil {
				// Nothing was sent for it; the lane's slot stays as it was.
				if l.phase == Ready {
					l.phase = Idle
				}
				l.entry = ""
				return Ref{}, &ErrEncode{err}
			}
			inc(&st.Encodes)
			l.cacheContent, l.cacheRef, l.cacheObj = content, here, &obj
		}
		obj := l.cacheObj
		meta := make(map[string]string, len(obj.Meta)+5)
		for k, v := range obj.Meta {
			meta[k] = v
		}
		if meta[MetaKind] == "" { // a heartbeat's encoder says KindBeat
			meta[MetaKind] = KindData
		}
		meta[MetaEpoch] = here.Epoch
		meta[MetaSeq] = strconv.FormatUint(here.Seq, 10)
		meta[MetaContent] = content
		meta[MetaProducer] = l.Producer
		key := SlotKey(l.Prefix, here.Epoch, here.Seq)
		l.phase = Waiting
		inc(&st.Puts)
		l.emit(Event{Kind: "put", Ref: here, Content: content})
		o := l.put(ctx, key, obj, meta)
		l.emit(Event{Kind: "putResult", Ref: here, Content: content, Outcome: o})
		switch {
		case o == PutOK:
			inc(&st.Committed)
			at := l.own()
			l.emit(Event{Kind: "committed", Ref: at, Content: content})
			return at, nil
		case l.Mutation == RetryNewKey:
			l.next++
			l.phase = Ready
			continue
		case o == PutExists:
			l.after412 = true
		default:
			l.phase = Unresolved
		}
		// 412, or no answer: read the slot. With no answer the request may
		// still be in flight; If-None-Match lets at most one copy land. The
		// caller's deadline has usually passed when the PUT timed out, so
		// the HEAD runs on its own short deadline.
		inc(&st.Heads)
		hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.headTimeout())
		m, found, herr := l.Store.Head(hctx, key)
		cancel()
		if herr != nil {
			inc(&st.Unresolved)
			l.phase = Unresolved
			l.emit(Event{Kind: "headError", Ref: here, Content: content, Err: herr.Error()})
			return Ref{}, &ErrUnresolved{fmt.Sprintf("put %s: %v; head: %v", key, o, herr)}
		}
		found1 := Slot{Kind: Free}
		if found {
			found1 = SlotFromMeta(m)
		}
		l.emit(Event{Kind: "head", Ref: here, Content: content, Found: found1})
		switch {
		case found1.Kind == Data && found1.Content == l.entry && found1.Epoch == l.epoch:
			inc(&st.ResolvedOwn)
			at := l.own()
			l.emit(Event{Kind: "resolvedOwn", Ref: at, Content: content})
			return at, nil
		case found1.Kind == Free && l.after412:
			// 412, then the HEAD found nothing: the store isn't
			// read-after-write consistent, or the slot was deleted. Never
			// guess: stay unresolved.
			inc(&st.Inconsistent)
			l.phase = Unresolved
			l.emit(Event{Kind: "inconsistent", Ref: here, Content: content})
			return Ref{}, &ErrUnresolved{fmt.Sprintf("put %s: 412 but HEAD finds no object (store not read-after-write consistent?)", key)}
		case found1.Kind == Free && resends >= MaxResends:
			// Not sent again by this call: the PUT may still land, so the
			// slot stays unresolved (the next Append starts there).
			inc(&st.Unresolved)
			l.phase = Unresolved
			// To the model the lane is ready to resend, and the next call's
			// PUT is that resend: nothing to translate (modelcheck).
			l.emit(Event{Kind: "resendLimit", Ref: here, Content: content})
			return Ref{}, &ErrUnresolved{fmt.Sprintf("put %s: %v, and %d resends after HEADs found the slot free", key, o, resends)}
		case found1.Kind == Free:
			resends++
			inc(&st.Resent)
			l.phase = Ready
			l.emit(Event{Kind: "resend", Ref: here, Content: content})
		case found1.Kind == Tomb && l.Mutation != NoHalt:
			// The consumer closed this log here: a new epoch, at slot 0,
			// with the same batch in hand.
			inc(&st.Halted)
			l.phase = Halted
			ne := l.newEpoch()
			l.emit(Event{Kind: "halted", Ref: here, Content: content, NewEpoch: ne})
			l.epoch, l.next, l.after412 = ne, 0, false
			l.phase = Ready
		case found1.Kind == Tomb:
			l.next++
			l.phase, l.after412 = Ready, false
		default:
			// Another batch holds the slot: it is committed; ours moves on.
			inc(&st.LearnedOther)
			l.remember(found1.Content, here)
			l.next++
			l.phase, l.after412 = Ready, false
			l.emit(Event{Kind: "learnedOther", Ref: here, Content: found1.Content})
		}
	}
}

func (l *Lane) headTimeout() time.Duration {
	if l.Timeouts.Head > 0 {
		return l.Timeouts.Head
	}
	return 2 * time.Second
}

func (l *Lane) put(ctx context.Context, key string, obj *Object, meta map[string]string) PutOutcome {
	pctx := ctx
	if l.Timeouts.Put > 0 {
		var cancel context.CancelFunc
		pctx, cancel = context.WithTimeout(ctx, l.Timeouts.Put)
		defer cancel()
	}
	return l.Store.PutCreate(pctx, key, obj.Body, obj.ContentType, meta)
}

// ---- requests with several objects ------------------------------------------

// PartOutcome is how one object of a request ended up.
type PartOutcome struct {
	Signal string
	Ref    Ref
	Err    error // nil: committed
}

// Verdict is what the request's sender is told.
type Verdict int

const (
	Ack    Verdict = iota // every object is committed (or there were none)
	Retry                 // at least one object is unresolved: retry the whole request
	Reject                // a part can't be encoded: permanent
)

// RequestVerdict is the request-level decision for a request split into
// several objects (a metrics request: one object per type and layout-B
// namespace, each in its own log): ACK only when every object has
// committed (../../model/s3InlineMetrics.qnt, `reqAckedImpliesAllCommitted`).
// The parts already committed are found in their lanes' known set on the
// retry, or after a restart committed again in a new epoch, where the
// consumer's content-key check skips the copy.
func RequestVerdict(parts []PartOutcome, m Mutation) (Verdict, error) {
	var retry error
	any := false
	for _, p := range parts {
		var ee *ErrEncode
		switch {
		case p.Err == nil:
			any = true
		case errors.As(p.Err, &ee):
			return Reject, p.Err
		default:
			if retry == nil {
				retry = fmt.Errorf("%s: %w", p.Signal, p.Err)
			}
		}
	}
	switch {
	case retry != nil && any && m == AckOnAny:
		return Ack, nil
	case retry != nil:
		return Retry, retry
	}
	return Ack, nil
}
