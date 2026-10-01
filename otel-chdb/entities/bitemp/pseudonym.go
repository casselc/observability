package bitemp

// Pseudonymise on departure (owner decision O-G9; DECISIONS D38 items 11-17;
// research/grants.md §9.6; model ../../model/bitemporalCatalog.qnt).
//
// A correction is an Event{Source: Steward, Kind: Pseudonymise} for one
// named entity, its Version the pseudonym's content address. It is an
// overlay, not a lifecycle event: resolution (Resolve, the Replayer,
// Current's partition) ignores it, and Redact replaces the Version of every
// Asserted row of that entity by the pseudonym, at every valid time and at
// EVERY basis, including system times before the correction was recorded:
// a basis issued earlier must not keep the name (D38 item 13). The FIRST
// correction (lowest SystemFrom, then Seq) gives the pseudonym, so a
// duplicate, late or re-keyed signal changes no answer.

// FirstCorrection returns entity's first correction among events, if any.
// Every correction counts, whatever its SystemFrom: the caller passes what
// is recorded now, not what was recorded at the basis.
func FirstCorrection(events []Event, entity uint64) (Event, bool) {
	var first *Event
	for i := range events {
		e := &events[i]
		if e.Kind != Pseudonymise || e.Entity != entity || e.Entity == All {
			continue
		}
		if first == nil || e.SystemFrom < first.SystemFrom || (e.SystemFrom == first.SystemFrom && e.Seq < first.Seq) {
			first = e
		}
	}
	if first == nil {
		return Event{}, false
	}
	return *first, true
}

// Redact applies the entity's corrections (any events; others are ignored)
// to a resolved row: an Asserted row shows the first correction's pseudonym.
// The state, interval, source and Uncertain flag are kept.
func Redact(r Row, events []Event) Row {
	if r.State != Asserted {
		return r
	}
	if c, ok := FirstCorrection(events, r.Entity); ok {
		r.Version, r.Pseudonymised = c.Version, true
	}
	return r
}

// ResolveNamed is Resolve as a reader gets it: at (entity, vt) as of st,
// pseudonymised by every correction in events (whatever its SystemFrom).
func ResolveNamed(events []Event, entity uint64, vt, st Time, p Policy) Row {
	return Redact(Resolve(events, entity, vt, st, p), events)
}

// RowsNamed is Rows, pseudonymised.
func RowsNamed(events []Event, entity uint64, vtFrom, vtTo, st Time, p Policy) []Row {
	rows := Rows(events, entity, vtFrom, vtTo, st, p)
	for i := range rows {
		rows[i] = Redact(rows[i], events)
	}
	return rows
}
