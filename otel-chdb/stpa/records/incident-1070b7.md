---
id: incident-1070b7
label: CAST-64
batch: go-upgrade-and-verification-2026-09-29
title: "The Go edge's lane serialised Append on a sync.Mutex held across its S3 requests, up to MaxResends resends with their timeouts: a caller queued behind a lane stuck in resends waited with no regard for its own deadline"
found_by: "The new Go edge DST (TestDSTEdgeCommit): the fake clock froze behind a goroutine waiting on the mutex"
hazard: "H-7 and H-8 (CAST-39 class): callers and their queues held beyond their deadlines while a lane retries"
controller: "`commit.Lane.Append`: \"a queued caller is bounded by the holder's resend limit\""
why: "Append serialises by design and the holder's resends are bounded, so the wait looked bounded too; it was bounded only by the holder's budget, not the caller's"
fix: "The lane's lock is a one-slot channel: a queued caller gives up at its context's end (\"lane busy\", retryable, nothing sent); commit 71d39a8; TestDSTEdgeCommit and TestAppendResendLimit"
lesson: "A waiter needs its own deadline, not only the holder's; a lock held across I/O turns the holder's retry budget into everyone's"
state: accepted
---
