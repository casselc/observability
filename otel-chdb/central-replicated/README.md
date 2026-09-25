# Replicated central with zero-copy replication on S3 (ClickHouse 26.10)

Two ClickHouse 26.10.1.618 replicas and a three-node ClickHouse Keeper on one
box. The otap-rs consumer ingests into ReplicatedMergeTree versions of the
central tables: a local "hot" volume, then a TTL move to an S3 volume on
SeaweedFS, with `allow_remote_fs_zero_copy_replication = 1`.

Labels: **[M]** measured here, **[D]** from the 26.10 docs or source
(commit `596f844`, the build's), **[E]** estimate.

## Verdict

**Zero-copy is not safe enough to be the plan on 26.10. Use plain
ReplicatedMergeTree, with each replica keeping its own S3 copy.** If S3
storage cost forces zero-copy, it can be run under the rules below, but it
leaks storage under faults, and the cold data then has one copy only.

- **Status [D].** `allow_remote_fs_zero_copy_replication` is tier
  **Experimental**, default 0. Its description reads "Don't use this setting
  in production, because it is not ready". The storage and replication docs
  say: "Zero-copy replication is not ready for production. Zero-copy
  replication is disabled by default in ClickHouse version 22.8 and higher.
  This feature is not recommended for production use." Nothing in 26.10
  replaces it for self-managed clusters (see [Alternatives](#alternatives)).
- **What held [M].** No active data was lost:
  - a 15-minute consumer soak with 13 replica SIGKILLs, 6 Keeper-node kills
    and 3 Keeper quorum losses;
  - a replica killed in the middle of an S3 merge;
  - a replica dropping its copy of a table;
  - DROP PARTITION, TTL DELETE and part replacement after merges.

  Both replicas held identical data, and every live blob was referenced by
  both replicas: one copy.
- **What broke [M]:**
  1. **Leaked blobs.** Crashes left objects no part references. The chaos
     soak left 4,120 objects (20.0 MB), 23% of the 88.8 MB of live data. A
     5-minute run with 8 replica kills left 1,634 more. The fault-free steps
     leaked nothing, except 6 objects from one killed merge. Open-source
     ClickHouse has no tool that reclaims them.
  2. **Leaked Keeper lock nodes.** After cleanup, 137 zero-copy lock nodes
     remained for 17 parts.
  3. **Detached parts are not protected.** Restarts after kills detached 626
     `ignored_` parts, holding 23 MB of blobs. One of them already pointed at
     24 blobs that the other replica had deleted. Attaching it would have
     failed or lost data.
  4. **One copy of the cold data.** Replication no longer protects the cold
     tier against losing the bucket, a bad lifecycle rule, or a zero-copy bug
     that deletes shared blobs. Zero-copy has had such bugs; see the source
     comments quoted below.
- **What it saves.** Only the S3 tier: one copy instead of one per replica.
  The hot tier stays one copy per replica, because it is local disk.
  - **Storage [E]:** at the calculator's plan (1–7 days hot, 90 days total),
    S3 holds 83–89 days of data. With 2 replicas zero-copy halves that
    storage; with 3 it cuts it to a third.
  - **Requests [M]:** a lifecycle of 10 parts (insert, move to S3, merge,
    clean, drop) made 377 PUTs against 754, and 1,449 GETs against 2,100.
  - **CPU [M]:** merges of parts already on S3 run on one replica: 2.8 s
    against 5.0 s. With 1+ days hot, most merging happens before the move,
    on local disk, where zero-copy doesn't apply. So the CPU saving is small
    in production [E].
  - **Keeper [M]:** zero-copy costs twice the Keeper transactions: 1,165
    against 545 for the same lifecycle.
- **If zero-copy is used anyway**, these are the operational rules:
  - enable it only on the tiered tables;
  - keep `disable_{detach,fetch,freeze}_partition_for_zero_copy_replication = 1`
    (the defaults);
  - never attach a detached part: drop detached parts;
  - run an orphan sweeper: list the prefix, subtract every replica's
    `system.remote_data_paths`, and delete only objects older than a day or
    so (`scripts/blobcheck.py` does the diff);
  - BACKUP the cold tier to another bucket, since there is one copy;
  - use a 3-node Keeper;
  - upgrade one replica at a time and keep versions equal.

**The consumer is correct on a replicated central with one change, and
needs neither `select_sequential_consistency` nor `insert_quorum` for
exactly-once [M].** Before a check that may follow statements committed on
another replica, it must run `SYSTEM SYNC REPLICA … LIGHTWEIGHT`. That is
the new `--sync-replica` flag.

- **With the flag:** the soak passed the exactly-once audit on both
  replicas.
- **Without it:** the same soak, with workers split across replicas and
  replicas killed, duplicated 7 batches in 5 minutes. The workers could not
  see the duplicates, since they arrived later by replication.

## Setup

| | |
|---|---|
| Keeper | 3 nodes: `clickhouse keeper`, client ports 29181–29183, raft 29231–29233, `configs/keeper{1,2,3}.xml` |
| replicas | `r1`: HTTP 28123, TCP 29000, interserver 29009; `r2`: 38123 / 39000 / 39009. Configs in `configs/replica{1,2}.xml`, macros `{cluster}=central {shard}=01 {replica}=rN`. Each replica has `max_server_memory_usage_to_ram_ratio = 0.2`, small caches, and `part_log`, `query_log` and `zookeeper_log` on |
| disks | `default` (local, the hot volume); `s3_zc` → `http://127.0.0.1:18333/central-zc/zc/`, the **same prefix on both replicas**; `s3_own` → `central-zc/own-r1/` and `own-r2/`, one prefix per replica (the plain-replication baseline) |
| policies | `tiered_zc` = hot `default` + cold `s3_zc`; `tiered_own` = hot `default` + cold `s3_own` |
| tables | `scripts/ddl.py` takes the consumer's own DDL (`consume --print-ddl`: `otel_traces` and `otel_logs` with the envelope columns, the layout-B tables from `otap-rs/sql/series_tables.sql`, plus `content_key` and the `by_content` projection) and rewrites it to `ReplicatedMergeTree('/clickhouse/tables/{shard}/<db>/<table>', '{replica}')` (Replicated**Aggregating**MergeTree for `otel_metrics_series`), with `TTL toDateTime(received_at) + INTERVAL <move> TO VOLUME 'cold', … + INTERVAL <delete> DELETE` (`LastSeen` for the series table) and `SETTINGS storage_policy = 'tiered_zc', allow_remote_fs_zero_copy_replication = 1`. The soak used a 3-minute move and a 1-day delete; `sql/central_zc.sql` is an example |
| restart | `scratchpad/start-replicas.sh` starts whatever is down (`stop` stops the replicas and Keeper by pid file). It creates bucket `central-zc` and never touches the shared server on 18123/19000 |

**Left running:** Keeper k1–k3 and replicas r1 and r2 are still up, and
start-replicas.sh restarts them after a container restart. They hold the
main soak's database `rsoak_rsoak1790373757` (2.5 M rows, one copy on S3)
and `zexp`. The leaked objects are still in `central-zc/zc/`, as evidence.
The whole footprint is about 1 GB under `scratchpad/crep` plus about 140 MB
in the bucket.

## 1. The consumer on the replicated tables

### Consumer changes (the only code changes)

`otap-rs/src/consumer/sql.rs` and `otap-rs/src/bin/consume.rs`. All 29
existing unit tests pass. New flags:

- **`--ch URL1,URL2,…`:** the worker sticks to one replica and moves to the
  next on a transport error (no HTTP answer). With one URL, `--ch`
  behaves as before.
- **`--sync-replica [--sync-timeout 5s] [--switch-hold <budget+12s>]`:**
  - Before a check, the worker runs `SYSTEM SYNC REPLICA <table>
    LIGHTWEIGHT` with `receive_timeout = sync-timeout`. It skips this when
    the table's last operation on the current replica was the worker's own
    statement, i.e. the verify right after an insert or a repair.
  - A failed sync fails the check, so the lane is deferred to the next poll.
  - After a replica switch, checks fail for `switch-hold`. This waits out a
    statement that may still be committing on the old replica: see the
    Keeper caveat under "A time-bound caveat" below.
- **`--no-ddl`:** `ensure` checks that the table exists instead of creating
  it. The consumer's own DDL makes a plain MergeTree, which on a replica
  where the operator's table is missing would silently become an
  unreplicated table.
- **`--insert-setting k=v`** (repeatable): extra insert settings, for
  example `insert_quorum=2`.
- **Stats:** `ch_syncs`, `ch_sync_errors`, `ch_switches`, `ch_replica`.

### Soak [M] (`scripts/soak_replicated.sh`, `results/soak/`)

The setup is `otap-rs/scripts/consumer_soak.sh`, shortened, with these
changes:

- 3 edges behind the 3 fault proxies (ambiguous, slow and dropped PUTs) at
  2 requests/s per signal per edge.
- 3 workers **split across replicas on purpose**: w1 and w3 use
  `--ch r1,r2`, w2 uses `--ch r2,r1`. Lane takeovers therefore cross
  replicas.
- Lease timing as before: TTL 6 s, margin 1 s, budget 2 s.
- The tables move to S3 after 3 minutes, so merges and moves ran on the
  zero-copy volume throughout the run.
- Chaos every 8–20 s: worker SIGKILL, worker SIGSTOP past its lease, edge
  SIGKILL, **replica SIGKILL for 5–20 s**, **Keeper node SIGKILL for 3–8 s**,
  or **Keeper quorum loss** (2 of 3 nodes for 8–15 s).
- At the end: drain, `SYSTEM SYNC REPLICA` on both replicas, then
  `consumer_soak_check.py` against each replica.

| | `--sync-replica` (15 min) | control: no sync (5 min) |
|---|---|---|
| chaos | 13 replica kills, 6 Keeper node kills, 3 Keeper quorum losses, 4 worker kills, 5 pauses, 6 edge kills | 8 replica kills, 4 worker kills, 3 pauses |
| acked requests / committed objects | 14,079 / 31,188 | 4,742 / 10,422 |
| central, **r1 and r2 identical** | traces 4,560 batches / 912,000 rows; logs 5,249 / 1,049,800; number points 4,270 / 170,800; histogram, exponential histogram and summary points 4,270 / 85,400 each: **missing 0, partial 0, duplicated 0, uncommitted 0; every request exactly once** | **FAIL: 7 exponential-histogram batches duplicated (140 rows)** on both replicas; everything else exact |
| workers | 7 incarnations, 4,057 statements (7.9 objects each), 3,881 syncs, **815 syncs failed** (a replica or Keeper was down: those checks were deferred), 24 replica switches, `retried_missing` 1, `over_count` 0 | `retried_missing` 37, `over_count` 0 (the duplicates arrived after the checks) |

The run was stopped by the disk guard at 909 s instead of 900 s: SeaweedFS
does not reclaim deleted space until a vacuum. The box was shared with
another agent's merge benchmark, and the load average reached 26–35.

### The three questions

**Does the post-insert check need `select_sequential_consistency` or
`insert_quorum`?** No, provided the check reads the replica that ran the
insert. What it does need is a sync whenever that may not hold.

- **Same replica [M].** Read-your-writes on one replica holds. With fetches
  stopped on r2, a batch inserted on r1 counted 1,000 on r1 at once and 0 on
  r2.
- **`select_sequential_consistency = 1` without quorum inserts does nothing
  [M].** r2 still answered 0, with no error.
- **Quorum with `insert_quorum_parallel = 1` (the default).**
  `select_sequential_consistency` "does not work" [D], and r2 again answered
  0 [M]. A quorum timeout returns `UNKNOWN_STATUS_OF_INSERT`, but the part
  stays and is visible on r1 [M]. The consumer's verify handles that case.
- **Quorum with `insert_quorum_parallel = 0`** makes the setting work
  [M]:
  - a part inserted with quorum but not yet quorum-committed is hidden on
    r1, and appears on both replicas once r2 has it;
  - but the next insert into the table fails with
    `UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE` while one is pending. With
    several workers writing a table, that serializes them.
  - In 26.10, `async_insert = 1` is the default, and
    `insert_quorum_parallel = 0` is refused with it on (`UNSUPPORTED_PARAMETER`)
    [M].

  None of this is needed. The failure it would address is a check on a
  replica that hasn't fetched another replica's statements, and the control
  run shows that failure is real (7 duplicates). `SYSTEM SYNC REPLICA …
  LIGHTWEIGHT` addresses it directly [M]. It:
  - pulls the replication log;
  - waits for the fetches;
  - times out, rather than answering stale, when the source replica is down
    (`TIMEOUT_EXCEEDED` with fetches stopped [M]);
  - cost, under the soak's load: p50 24 ms, 1 ms CPU and 4 Keeper
    transactions [M].

  `insert_quorum` is a **durability** setting, not an exactly-once one.
  Until the other replica fetches it, an acked batch exists only on one
  replica's local hot disk. If that disk is lost in that window (sub-second
  in the soak), the batch is lost after the checkpoint moved past it. With
  2 replicas, `insert_quorum = 2` closes the window but stops ingest while
  either replica is down. With 3 replicas, `insert_quorum = 2` (parallel)
  keeps ingesting through one failure. The consumer tolerates the
  quorum-timeout `UNKNOWN_STATUS`: its verify reads the part [M].

**What happens when the worker's replica lags or is down? [M]**

- **Lag:** the sync waits for the fetch, up to `--sync-timeout`, then the
  check runs.
- **Replica down:**
  1. The worker's requests fail at transport level, so it switches to the
     next replica.
  2. It holds checks for the switch hold (14 s here).
  3. It syncs. If the dead replica holds parts the live one hasn't fetched,
     the sync fails until the dead replica returns. Those tables' lanes stall
     rather than risk a duplicate: 815 deferred checks in the soak, and the
     result still passed. If a replica is lost for good with unfetched
     parts, those batches are lost; see the durability point above.
- **A Keeper node down:** tables whose session sat on that node are
  read-only until the session moves ("Table is in readonly mode" on sync
  and insert, seconds).
- **Keeper quorum lost:** every replicated insert and sync fails until
  Keeper is back. The consumer retries at the next poll.
- **A SIGSTOPped worker** (a pause past its lease) also switched replicas,
  because its HTTP request timed out. That is harmless.

**A time-bound caveat [D].** The lease design assumes a statement cannot
commit after `fence + budget` (`max_execution_time`).

- The replicated sink retries Keeper operations and checks the time limit
  between retries (`ZooKeeperRetriesControl`, `checkTimeLimit()`).
- One Keeper request can still run for `operation_timeout_ms` (10 s)
  beyond it.
- So on a replicated central, **lease margin ≥ Keeper operation timeout**
  is the safe setting. The soak's 1 s margin didn't show a violation, but
  production should use a TTL of 30 s and a margin of 10 s or more. The
  switch hold uses budget + 12 s for the same reason.

**Is replicated dedup (`replicated_deduplication_window`,
`insert_deduplication_token`) compatible? Yes [M].**

- The dedup hashes live in Keeper, so they are shared between replicas:
  - the same token inserted on r1 and then on r2 landed once;
  - a new token landed again.
- 26.10's defaults are `replicated_deduplication_window = 10000` and
  `replicated_deduplication_window_seconds = 3600` [D].
- As on a single node, the token covers only an exact retry of the same
  statement. A regrouped retry is not deduplicated. The content-key check
  still makes ingest exactly-once, and the sync makes the check see
  everything.
- Keep the token: it is cheap. Drop `non_replicated_deduplication_window`,
  which means nothing on a replicated table (ddl.py removes it).

**Two further findings on the consumer:**

- **Checks hit S3 [M].** The content-key check reads the `by_content`
  projection of every part, including parts already moved to S3. In the
  soak (move after 3 min) a check cost 11 ms CPU and 5 S3 GETs (6,223
  checks: 71.6 s CPU across both replicas). At 1–7 days hot and 90 days in
  total, every check would touch the whole cold tier. **Add a partition
  predicate** to the check (`toDate(received_at) >= <oldest pending
  object's day − 1>`), so cold partitions are pruned. Not implemented here.
- **Insert CPU on the replicated tables** was 58.6 µs per row on r1 and
  66.7 on r2 (1,452 and 2,515 statements) [M]. That is well above the
  single-node figure (2.4 ms per 200-row object, 12 µs per row [M, otap-rs
  README]). Two causes: the box was heavily loaded, and a statement here
  averaged 7.9 objects against 32. Treat it as an upper bound. Replication
  added about 2 Keeper transactions per insert [M].

## 2. Zero-copy behaviour

`scripts/zc_experiments.sh` (the helpers), `scripts/blobcheck.py`,
`scripts/bench_s3.sh`, and `results/zc/`.

**One copy? Yes, for every live part [M].**

- After the soak: 1,436 blobs (88.8 MB) of active parts, each referenced by
  **both** replicas.
- In the controlled run (10 parts moved, then merged), `zx.t_zc` held one
  copy (137 blobs, 25.7 MB) and `zx.t_own` two (2 × 25.7 MB, one per
  prefix).
- **Inserts** land on the local hot volume on each replica: no sharing
  there.
- **TTL moves** are not coordinated: each replica runs its own
  (2,116 and 2,252 `MovePart` in the soak). The move code first tries
  `tryToFetchIfShared`: if another replica already has the part on the
  shared disk, it takes that replica's metadata instead of uploading [D:
  `MergeTreePartsMover::clonePart`]. In the bench the first mover did all
  240 PUTs and the second none [M]. Two replicas moving the same part at the
  same instant could both upload, since there is no lock. No active part
  ended up with two copies [M].

**Which replica merges? [M]** Only one per merge, for parts on S3, through
the zero-copy exclusive lock [D: `MergeFromLogEntryTask`,
`tryCreateZeroCopyExclusiveLock`].

- The other replica waits, then fetches the merged part zero-copy: 19 GETs
  of metadata and nothing written.
- Soak: r1 ran 451 S3 merges (70.1 s CPU, 427 MB written) and r2 ran 466
  (69.2 s, 372 MB). They took each other's results as 417 and 473 zero-copy
  fetches. The merge work was split, not duplicated.
- Bench: zero-copy merged once (2.6 s CPU, 137 PUTs). Plain replication
  merged on both (2.5 + 2.5 s, 274 PUTs).
- Merges on the hot volume are ordinary: each replica merges its own copy.
- `remote_fs_execute_merges_on_single_replica_time_threshold` (10,800 s)
  also exists [D].

**Blob lifecycle [M]:**

| operation | result |
|---|---|
| part replaced by a merge | old blobs deleted once *both* replicas drop the outdated part: 20 DeleteObjects on the last replica against 20 + 20 without zero-copy. Cleanup took up to about 5 min at default cleanup delays. No orphans. |
| DROP PARTITION | blobs deleted at once: 137 objects, 25.7 MB. No orphans. |
| TTL DELETE (MODIFY TTL + materialize) | rows removed. The outdated parts' blobs were deleted by the cleanup: 107 MB freed within 200 s. No orphans. |
| replica drop (DROP DATABASE on r2 only) | r2's references removed, **the shared blobs kept**: r1 read every row and CHECK TABLE passed. Then DROP on r1 removed all 688 blobs. |
| SIGKILL of the merging replica mid-merge on S3 | r2 waited on the lock (it is ephemeral, so held until r1's session ended). r1 restarted and redid the merge. 960,000 rows on both, CHECK TABLE ok. **6 orphans (118 KB).** |
| DROP DETACHED PARTITION ALL | removed 7,952 blobs of r2's detached parts. **312 of r1's became orphans**, and 24 dangling references disappeared with the part that held them. |

**Leaks and loss [M]:**

- **Orphans.** Objects that no replica's metadata references
  (`blobcheck.py`: a listing minus the union of `system.remote_data_paths`).
  - They accumulate only under faults: 4,120 (20.0 MB) in the chaos soak,
    1,634 in the control run, 6 from the killed merge. The fault-free steps
    left none.
  - By upload time they cluster in the chaos windows
    (`results/soak/orphans-by-time.txt`).
  - The server logs name the paths that cause them:
    - "Since zero-copy replication is enabled we are not going to remove
      blobs from shared storage for …/tmp_… or …/moving/…": temporary parts
      found at startup;
    - "Node with parent zookeeper lock … doesn't exist … refuse to remove
      blobs";
    - "Lock is lost, because session was expired".
  - The source accepts this: "Worst outcomes: trash in object storage
    and/or orphaned shared zero-copy lock. It is acceptable." [D:
    `MergeTreePartsMover.cpp`].
  - At the end the bucket held 123.5 MB for 97.9 MB of live data: 26%
    garbage.
- **Lock nodes.** After cleanup, 137 zero-copy lock nodes remained for the
  soak's 17 S3 parts (`results/zc/keeper-locks-after-cleanup.txt`). Dropping
  the table logs "There are some lost locks inside. Removing them all" and
  clears them.
- **Detached parts.** A restart after a kill detaches the merged-away source
  parts as `ignored_…`: 604 on r2 and 22 on r1 in the soak. They keep their
  blobs (23 MB) until dropped. One of r1's pointed at 24 blobs that were
  already deleted. **No active part ever referenced a missing blob.** A
  detached part on zero-copy, however, is not safe to attach.
- **Source comments [D]:**
  - `MergeFromLogEntryTask.cpp`: "In case of mutation and hardlinks it can
    even lead to extremely rare dataloss";
  - `StorageReplicatedMergeTree.cpp` (`getParentLockedBlobs`): "flaws in
    hardlinks tracking … it will lead to dataloss";
  - `MergeTreePartsMover.cpp`: "TODO"/"FIXME" around zero-copy moves.
  - Open upstream issues that apply to object-storage disks in general:
    [#113179](https://github.com/ClickHouse/ClickHouse/issues/113179)
    (local metadata rewritten in place: a SIGKILL during cleanup can lose a
    committed part);
    [#111330](https://github.com/ClickHouse/ClickHouse/issues/111330)
    (object-storage disks ignore fsync);
    [#98933](https://github.com/ClickHouse/ClickHouse/issues/98933)
    (clickhouse-disks doesn't delete blobs);
    [#85203](https://github.com/ClickHouse/ClickHouse/issues/85203) (MODIFY
    TTL with zero-copy fails with NOT_ENOUGH_SPACE).

**Settings that govern zero-copy [D]:**

- `allow_remote_fs_zero_copy_replication` (Experimental, 0);
- `remote_fs_zero_copy_zookeeper_path` (`/clickhouse/zero_copy`);
- `remote_fs_zero_copy_path_compatible_mode`;
- `remote_fs_execute_merges_on_single_replica_time_threshold` (10,800 s);
- `zero_copy_merge_mutation_min_parts_size_sleep_before_lock` (1 GiB) and
  `…_no_scale_before_lock` (0);
- `zero_copy_concurrent_part_removal_max_split_times` (5) and
  `…_max_postpone_ratio` (0.05);
- `disable_freeze_partition_for_zero_copy_replication`,
  `disable_detach_partition_for_zero_copy_replication` and
  `disable_fetch_partition_for_zero_copy_replication` (all 1);
- `allow_feature_tier`: a server set to 1 or more rejects the setting.

The lock layout in Keeper is
`/clickhouse/zero_copy/zero_copy_s3/<table uuid>/<part>/<blob id>/<replica>`,
plus `…/<part>/part_exclusive_lock` for merges [M].

## 3. Cost

**One lifecycle** (`scripts/bench_s3.sh`, `results/zc/bench_s3.txt` [M]):
10 parts of 30,000 spans (27 MB), inserted on r1, fetched by r2, moved to
S3, merged into one part, cleaned and dropped. Both replicas summed.

| | zero-copy | plain replicated S3 (a copy per replica) |
|---|---|---|
| PUT (insert + move / merge) | **240 / 137 = 377** | 480 / 274 = 754 |
| GET | 1,449 | 2,100 |
| DeleteObjects (clean + drop) | 23 | 46 |
| server CPU (all phases) | 4.8 s | 6.5 s |
| merge CPU | 2.6 s on one replica (+0.2 s for the other's fetch) | 2.5 s + 2.5 s |
| Keeper transactions | **1,165** | 545 |
| bucket bytes after the merge | 1× | 2× |

Moves, merges on S3 and removal all do half the S3 work, but zero-copy
doubles Keeper traffic (locks for every part and blob).

**Soak, per replica [M]** (query_log and part_log; `system.events` resets
on every kill):

- **Inserts:** 64 s (r1) and 95 s (r2) of CPU, 58.6 and 66.7 µs per row,
  about 2 Keeper transactions each.
- **Checks:** 25 s and 46 s of CPU (11 ms each, from the S3 projection
  reads).
- **Syncs:** 1 s and 2 s.
- **S3 merges:** 70 s and 69 s of CPU, split and not duplicated. Hot-volume
  merges were negligible here because data moved at 3 min.
- **Keeper:** 281 and 286 requests/s per replica from `zookeeper_log`, 27%
  of them on zero-copy lock paths and about 8% on dedup blocks.

At the calculator's 90-day plan, and with 2 replicas [E]:

- **S3 storage:** zero-copy halves the S3 bytes (83–89 of 90 days). It adds
  garbage from crashes, 23% of live data after 13 replica kills in 15
  minutes and presumably far less at production crash rates, unless a
  sweeper runs.
- **Hot tier:** unchanged at 2×.
- **Merge CPU:** little change, since parts are merged before they move.
- **S3 requests:** moves cost half the PUTs.

At `$0.023/GB-month` S3 storage is usually small next to the hot tier and
compute. The calculator's "S3 per replica" against "S3, one copy" setting
shows the difference for a given fleet.

## Alternatives

- **Plain ReplicatedMergeTree, each replica its own S3 copy
  (`tiered_own`)** is what we recommend.
  - It measured correct and leak-free here: orphans 0 and missing 0 at
    every step.
  - It doubles S3 bytes and PUTs, and each replica merges its own S3 parts.
  - Each replica's cold data is independent, so a zero-copy blob-deletion
    bug cannot take out both replicas.
  - The consumer rules above (`--sync-replica`, `--no-ddl`, the lease
    margin) apply unchanged.
- **Nothing in open-source 26.10 replaces zero-copy [D]:**
  - SharedMergeTree is ClickHouse Cloud only ("Only available in ClickHouse
    Cloud" throughout its settings).
  - `s3_plain_rewritable` and the new `table_disk` keep the metadata in the
    bucket. They serve one writer, with read-only attach elsewhere, and do
    not replicate.
  - Issue [#70937](https://github.com/ClickHouse/ClickHouse/issues/70937)
    (closed): zero-copy did not work with plain_rewritable disks.
- **Cheaper still, if S3 cost dominates [E]:** replicate only the hot
  window. Move old partitions to a single cold table on S3 (non-replicated
  MergeTree on one node) and protect it with BACKUP. This trades
  availability of old data for storage. Not tested here.

## Reproduce

```bash
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
$S/start-services.sh; $S/start-replicas.sh
cd /home/user/oscope/spike/otel-chdb/central-replicated
# consumer (with the replicated flags)
(cd ../otap-rs && CARGO_TARGET_DIR=$S/otap-rs-target CARGO_BUILD_JOBS=2 cargo build --release --bin consume)
OUT=$PWD/results/soak DURATION=900 scripts/soak_replicated.sh            # SYNC=1 (default)
OUT=$PWD/results/soak-nosync SYNC=0 DURATION=300 KINDS="0 1 3 3" scripts/soak_replicated.sh
scripts/bench_s3.sh zc; scripts/bench_s3.sh own
python3 scripts/blobcheck.py s3_zc zc; python3 scripts/blobcheck.py s3_own 'own-{r}'
```

## Files

- `configs/`: `keeper{1,2,3}.xml`, `replica{1,2}.xml`, `users.xml`
- `scratchpad/start-replicas.sh`: starts Keeper and the replicas (outside
  the repo)
- `scripts/ddl.py`: the consumer's DDL made replicated and tiered;
  `sql/central_zc.sql`, `sql/zexp.sql`, `sql/zx*.sql`: generated examples
- `scripts/soak_replicated.sh`: the soak with replica and Keeper chaos;
  `common.sh`, `gen.sh` (synthetic spans), `s3du.py`
- `scripts/blobcheck.py`: orphans and missing blobs, sharing per database
  and part state
- `scripts/zc_experiments.sh`: helpers for the lifecycle steps;
  `scripts/bench_s3.sh`: zero-copy against plain replication
- `results/soak/`: the soak's `summary.txt`, `chaos.log`, worker logs and
  stats, `blobcheck-after.json`, `orphans-by-time.txt`
- `results/soak-nosync/`: the failing control run
- `results/smoke/`: a 90-second run with only Keeper, worker and edge chaos
  (no replica kills): PASS
- `results/zc/`: `log.txt`, `part_log.tsv`, `kill-merge.txt`,
  `drop-ttl.txt`, `replica-drop.txt`, `drop-detached.txt`, `bench_s3.txt`,
  `keeper-locks*.txt`, `blobcheck-final.json`
- Consumer: `otap-rs/src/consumer/sql.rs`, `otap-rs/src/bin/consume.rs`
