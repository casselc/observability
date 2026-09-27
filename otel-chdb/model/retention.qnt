// -*- mode: Bluespec; -*-
/**
 * retention: central drops partitions by received day, and an edge replays
 * its durable buffer after an outage with each request's ORIGINAL
 * received_at (D19; s3InlineReceived's replayKeepsReceivedAt, taken as given
 * here). STPA LS-10: retention is cut while an edge site is offline for days,
 * and its replay lands in partitions that have already expired; requirement
 * R-S6: retention must exceed the longest edge-buffer outage.
 *
 * Time is in days (the `day` of s3InlineReceived and s3InlineConsumer).
 *  - Clients send one request a day. The edge takes it into custody (the
 *    WAL; received_at = today) if its buffer has room. When it is full:
 *    `backpressure` (the deployed policy, D19) refuses it (503: custody stays
 *    with the client, which resends later as a new request), or
 *    DROP_OLDEST acks it and drops the oldest request in custody.
 *  - The edge loses and regains S3 at any time. While offline it keeps
 *    taking requests into custody; online, it publishes them, and the
 *    consumer inserts each into its received day's partition (ingest lag is
 *    minutes, not modelled). The environment bounds how long a request stays
 *    in custody: MAX_AGE days. That, not the outage, is the quantity R-S6
 *    needs: the first version of this model bounded one outage and its
 *    design broke acceptedVisible twice: a request still queued when the
 *    outage began was older than the outage at the replay, and an edge whose
 *    link flapped (back for a moment, not drained, down again) reset the
 *    outage clock while its oldest request kept aging. Custody age is the
 *    outage with its flaps, plus the backlog before it, plus the drain after
 *    it.
 *  - TTL: a partition d is retained while d > day - ret. A request inserted
 *    into an expired partition is written and dropped by the next TTL merge:
 *    it was acked to its client and is never visible (`lateExpired`). (With
 *    a hot tier of HOT days, one older than HOT goes straight to cold:
 *    `wReplayToCold`, a latency cost, not a loss.)
 *  - The operator can cut retention at runtime (`cut`). The design allows a
 *    cut only to a value above BOUND, the longest custody age it plans for
 *    (GUARD_CUT).
 *
 * The finding: under backpressure the age of the oldest request in custody
 * at the replay is the outage's length, whatever the buffer cap: a full
 * buffer stops taking new requests but keeps its oldest ones. So D19's
 * "the TTL must exceed the longest outage the edge buffer rides out
 * (retention_size_cap at the edge's rate)" holds for the outage, not for
 * cap / rate (mutant `capSized`: BOUND = the cap in days, custody longer
 * than it). Only DROP_OLDEST bounds the age by the cap, by losing acked
 * requests at the edge instead (`dropOldest`: acceptedVisible holds,
 * noEdgeDrop does not; D19 measured 47 of 64 lost).
 */
module retention {
  const RET: int           // retention at start, in days
  const BOUND: int         // the longest custody age the design plans for; a cut must stay above it
  const MAX_AGE: int       // the environment: the longest a request stays in the edge's custody
  const CAP: int           // the edge buffer's capacity, in requests (one a day)
  const DROP_OLDEST: bool  // the buffer's policy when full (false: backpressure)
  const GUARD_CUT: bool    // design: ret' > BOUND

  pure val MAX_DAY: int = 7
  pure val HOT: int = 1    // days on the hot tier (1.3: 1-7 days)
  pure val NEVER: int = 1000

  // ---- state ----------------------------------------------------------------------
  var day: int
  var ret: int
  var online: bool
  var offSince: int
  var lastRecv: int              // the last day a request arrived
  var buffer: Set[int]           // requests in custody, by received day (one a day: the day is the id)
  var accepted: Set[int]         // requests acked to their client (in custody or past it)
  var published: int -> int      // request -> the day it was inserted (NEVER: not yet)
  var parts: Set[int]            // requests in a retained partition of central
  var lateExpired: Set[int]      // inserted into an already expired partition
  var edgeDropped: Set[int]      // acked, then dropped from a full buffer
  var events: Set[str]

  pure def imin(s: Set[int]): int = s.fold(NEVER, (m, x) => if (x < m) x else m)
  def retained(d: int): bool = d > day - ret

  action keep = all {
    day' = day, ret' = ret, online' = online, offSince' = offSince, lastRecv' = lastRecv, buffer' = buffer,
    accepted' = accepted, published' = published, parts' = parts, lateExpired' = lateExpired, edgeDropped' = edgeDropped,
  }

  // ---- environment ------------------------------------------------------------------
  // A day passes: today's request has arrived (clients always send), and
  // no request stays in custody more than MAX_AGE days (the environment).
  action tick: bool = all {
    day < MAX_DAY, lastRecv == day, buffer.forall(q => day + 1 - q <= MAX_AGE),
    day' = day + 1,
    ret' = ret, online' = online, offSince' = offSince, lastRecv' = lastRecv, buffer' = buffer,
    accepted' = accepted, published' = published, parts' = parts, lateExpired' = lateExpired, edgeDropped' = edgeDropped,
    events' = events,
  }
  action goOffline: bool = all {
    online,
    online' = false, offSince' = day, events' = events.union(Set("offline")),
    day' = day, ret' = ret, lastRecv' = lastRecv, buffer' = buffer, accepted' = accepted, published' = published,
    parts' = parts, lateExpired' = lateExpired, edgeDropped' = edgeDropped,
  }
  action goOnline: bool = all {
    not(online),
    online' = true, events' = events.union(Set(if (day - offSince >= 2) "longOutage" else "outage")),
    day' = day, ret' = ret, offSince' = offSince, lastRecv' = lastRecv, buffer' = buffer, accepted' = accepted,
    published' = published, parts' = parts, lateExpired' = lateExpired, edgeDropped' = edgeDropped,
  }

  // ---- the edge --------------------------------------------------------------------------
  // Today's request arrives.
  action receive: bool = all {
    lastRecv < day,
    lastRecv' = day,
    if (buffer.size() < CAP) all {
      buffer' = buffer.union(Set(day)), accepted' = accepted.union(Set(day)), edgeDropped' = edgeDropped, events' = events,
    } else if (DROP_OLDEST) all {
      buffer' = buffer.exclude(Set(imin(buffer))).union(Set(day)), accepted' = accepted.union(Set(day)),
      edgeDropped' = edgeDropped.union(Set(imin(buffer))), events' = events.union(Set("dropOldest")),
    } else all {
      buffer' = buffer, accepted' = accepted, edgeDropped' = edgeDropped, events' = events.union(Set("backpressure")),
    },
    day' = day, ret' = ret, online' = online, offSince' = offSince, published' = published, parts' = parts, lateExpired' = lateExpired,
  }

  // Online: publish the oldest request in custody (the WAL drains in
  // order); the consumer inserts it into its received day's partition.
  action publish(q: int): bool = all {
    online, buffer.contains(q), q == imin(buffer),
    buffer' = buffer.exclude(Set(q)),
    published' = published.set(q, day),
    parts' = if (retained(q)) parts.union(Set(q)) else parts,
    lateExpired' = if (retained(q)) lateExpired else lateExpired.union(Set(q)),
    events' = events.union(
      (if (day - q > HOT and retained(q)) Set("toCold") else Set()).union(
       if (day - q >= 2) Set("oldReplay") else Set())),
    day' = day, ret' = ret, online' = online, offSince' = offSince, lastRecv' = lastRecv, accepted' = accepted,
    edgeDropped' = edgeDropped,
  }

  // ---- central ----------------------------------------------------------------------------
  // A TTL merge drops expired partitions.
  action ttl: bool = all {
    parts.exists(q => not(retained(q))),
    parts' = parts.filter(q => retained(q)),
    events' = events.union(Set("ttlDrop")),
    day' = day, ret' = ret, online' = online, offSince' = offSince, lastRecv' = lastRecv, buffer' = buffer,
    accepted' = accepted, published' = published, lateExpired' = lateExpired, edgeDropped' = edgeDropped,
  }
  // The operator cuts retention.
  def mayCut(r: int): bool = 1 <= r and r < ret and (GUARD_CUT implies r > BOUND)
  action cut(r: int): bool = all {
    mayCut(r),
    ret' = r,
    events' = events.union(Set(if (online) "cut" else "cutOffline")),
    day' = day, online' = online, offSince' = offSince, lastRecv' = lastRecv, buffer' = buffer,
    accepted' = accepted, published' = published, parts' = parts, lateExpired' = lateExpired, edgeDropped' = edgeDropped,
  }

  // ---- state machine --------------------------------------------------------------------
  action init = all {
    day' = 0, ret' = RET, online' = true, offSince' = 0, lastRecv' = -1,
    buffer' = Set(), accepted' = Set(), published' = 0.to(MAX_DAY).mapBy(_ => NEVER), parts' = Set(),
    lateExpired' = Set(), edgeDropped' = Set(), events' = Set(),
  }
  action step = {
    nondet r = 1.to(RET).oneOf()
    any {
      tick, goOffline, goOnline, receive,
      all { buffer != Set(), nondet q = buffer.oneOf() publish(q) },
      ttl, cut(r),
    }
  }

  // ---- properties ---------------------------------------------------------------------------
  // R-S6: every request the edge acked and published lands in a partition
  // that is still retained: it is visible at least once.
  val acceptedVisible: bool = lateExpired == Set()
  // No acked request is dropped at the edge.
  val noEdgeDrop: bool = edgeDropped == Set()
  // The buffer never holds a request older than BOUND days (what the design plans for).
  val custodyAgeBounded: bool = buffer.forall(q => day - q <= BOUND)
  val safety: bool = and { acceptedVisible, noEdgeDrop }

  // ---- witnesses ----------------------------------------------------------------------------
  val wOldReplay: bool = events.contains("oldReplay") and acceptedVisible
  val wReplayToCold: bool = events.contains("toCold")
  val wCutOffline: bool = events.contains("cutOffline")
  val wTtlDrop: bool = events.contains("ttlDrop")
  val wBackpressure: bool = events.contains("backpressure") and events.contains("oldReplay")
  // The oldest request in custody is older than the cap in days.
  val wOlderThanCap: bool = buffer.exists(q => day - q >= CAP)

  // ---- scripted scenarios -----------------------------------------------------------------------
  // The edge goes offline on day 0 with that day's request in custody, and
  // stays offline MAX_AGE days (receiving what fits); then replays the oldest.
  run outagePrefix =
    init.then(receive).then(goOffline).then(MAX_AGE.reps(_ => tick.then(receive))).then(goOnline)
  run replayVisibleDesignTest = outagePrefix.then(publish(0)).expect(and { safety, wOldReplay })
  run retentionShortBreaksTest = outagePrefix.then(publish(0)).expect(not(acceptedVisible))
  // Retention cut while the edge is offline (unguarded).
  run cutOfflineBreaksTest =
    init.then(receive).then(goOffline).then(tick).then(receive).then(tick).then(receive).then(cut(1)).then(goOnline).then(publish(0))
      .expect(and { wCutOffline, not(acceptedVisible) })
  run cutGuardDesignTest =
    init.then(receive).then(goOffline).then(tick).then(receive).then(tick).then(receive).expect(and { not(mayCut(1)), not(mayCut(3)), mayCut(4) })
  // Backpressure: a full buffer keeps its oldest request for the whole outage.
  run capSizedBreaksTest =
    outagePrefix.expect(and { wOlderThanCap, not(custodyAgeBounded), events.contains("backpressure") })
      .then(publish(0)).expect(not(acceptedVisible))
  // Drop-oldest: the age is bounded by the cap, and acked requests are lost.
  run dropOldestTest = outagePrefix.expect(and { custodyAgeBounded, not(noEdgeDrop) })
}

// The design: retention above the longest custody age (RET 5 > MAX_AGE 3),
// cuts only above it, backpressure.
module retentionDesign {
  import retention(RET = 5, BOUND = 3, MAX_AGE = 3, CAP = 2, DROP_OLDEST = false, GUARD_CUT = true).*
}
// Retention no longer than the custody age.
module retentionShort {
  import retention(RET = 3, BOUND = 3, MAX_AGE = 3, CAP = 2, DROP_OLDEST = false, GUARD_CUT = true).*
}
// A cut below the outage bound, while the edge is offline (LS-10).
module cutDuringOutage {
  import retention(RET = 5, BOUND = 3, MAX_AGE = 3, CAP = 2, DROP_OLDEST = false, GUARD_CUT = false).*
}
// Retention sized from the buffer cap (2 days at the edge's rate), not the custody age.
module capSized {
  import retention(RET = 3, BOUND = 2, MAX_AGE = 3, CAP = 2, DROP_OLDEST = false, GUARD_CUT = true).*
}
// Drop-oldest: the cap bounds the age, at the price of acked requests.
module dropOldest {
  import retention(RET = 3, BOUND = 2, MAX_AGE = 3, CAP = 2, DROP_OLDEST = true, GUARD_CUT = true).*
}
