package bitemp

// Current is the materialised current view (VT = now, ST = now): XTDB's
// recency partition. It keeps only the events that can still decide an
// answer at a valid time >= now. An event leaves it when
//
//   - its ValidTo <= now, or
//   - every valid time of it from now on is hidden by a higher event of its
//     own tier that applies to it: an authority event by a higher authority
//     event of any kind (Unknown hides too); an announcement only by a later
//     announcement, never by an authority event, because a later authority
//     Unknown can reopen the evidence tier. An All event is hidden only by
//     All events.
//
// Get(entity) then equals Resolve over every event at (now, now): the
// model's currentIsResolved. Events must be added in arrival order
// (non-decreasing SystemFrom); now must not go back.
type Current struct {
	p    Policy
	now  Time
	ents map[uint64][]Event
	all  []Event
	// pseu: every correction (O-G9) by entity, never pruned: it applies at
	// every valid time from the moment it is recorded
	pseu map[uint64][]Event
}

// NewCurrent starts an empty view at now.
func NewCurrent(p Policy, now Time) *Current {
	return &Current{p: p, now: now, ents: map[uint64][]Event{}, pseu: map[uint64][]Event{}}
}

// Now is the view's valid and system time.
func (c *Current) Now() Time { return c.now }

// Add takes the next event (arrival order).
func (c *Current) Add(e Event) {
	if e.Kind == Pseudonymise {
		c.pseu[e.Entity] = append(c.pseu[e.Entity], e)
		return
	}
	if e.Entity == All {
		c.all = c.prune(append(c.all, e), nil)
		return
	}
	c.ents[e.Entity] = c.prune(append(c.ents[e.Entity], e), c.all)
}

// Advance moves now forward. Pruning is lazy (Get, Add, Compact).
func (c *Current) Advance(now Time) {
	if now > c.now {
		c.now = now
	}
}

// Get resolves entity at (now, now), pseudonymised (Redact).
func (c *Current) Get(entity uint64) Row {
	evs := c.prune(c.ents[entity], c.all)
	if len(evs) == 0 {
		delete(c.ents, entity)
	} else {
		c.ents[entity] = evs
	}
	all := make([]Event, 0, len(evs)+len(c.all))
	all = append(append(all, evs...), c.all...)
	return Redact(Resolve(all, entity, c.now, Inf, c.p), c.pseu[entity])
}

// Compact prunes every entity at now.
func (c *Current) Compact() {
	c.all = c.prune(c.all, nil)
	for k, evs := range c.ents {
		if evs = c.prune(evs, c.all); len(evs) == 0 {
			delete(c.ents, k)
		} else {
			c.ents[k] = evs
		}
	}
}

// Len is the number of events kept; Entities the number of entities with any.
func (c *Current) Len() int {
	n := len(c.all)
	for _, evs := range c.ents {
		n += len(evs)
	}
	for _, evs := range c.pseu {
		n += len(evs)
	}
	return n
}
func (c *Current) Entities() int { return len(c.ents) }

// EntityIDs lists the entities with events in the view.
func (c *Current) EntityIDs() []uint64 {
	out := make([]uint64, 0, len(c.ents))
	for k := range c.ents {
		out = append(out, k)
	}
	return out
}

// Prunable reports whether e can no longer decide any answer at a valid time
// >= now, given the other events hiders (which must include every event that
// applies to e's entity).
func Prunable(e *Event, hiders []Event, now Time, p Policy) bool {
	if e.ValidTo <= now {
		return true
	}
	var cov ivset
	for i := range hiders {
		h := &hiders[i]
		if h.Seq == e.Seq || !(h.Entity == All || h.Entity == e.Entity) || auth(h) != auth(e) || !p.Higher(h, e) {
			continue
		}
		cov.add(iv{h.ValidFrom, h.ValidTo})
	}
	return cov.covers(iv{max(now, e.ValidFrom), e.ValidTo})
}

// prune drops from evs what is prunable against evs and all (All events are
// hidden only by All events: pass all = nil for the All list).
func (c *Current) prune(evs, all []Event) []Event {
	if len(evs) == 0 {
		return evs
	}
	hiders := evs
	if len(all) > 0 {
		hiders = make([]Event, 0, len(evs)+len(all))
		hiders = append(append(hiders, evs...), all...)
	}
	out := evs[:0:0]
	for i := range evs {
		if !Prunable(&evs[i], hiders, c.now, c.p) {
			out = append(out, evs[i])
		}
	}
	return out
}
