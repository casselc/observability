# Central on an idle machine: validation runbook

The calculator's central constants mix two kinds of evidence (DECISIONS §3):
idle-box baselines from `bench/clean` (a 4-vCPU box, idle and gated), and
ratios measured on a **loaded** box (load 2.5–20) multiplied onto them:
`usRow` 9.0 is 3.43 × option 2's loaded-box ×2.40–2.62; `mergeRow` 20.1 is
10.1 × a loaded-box ×1.95–2.02, itself projected to 10⁴ parts from runs of
161–2,100. Merges are ~32% of per-replica CPU and headroom and query load
another 58%, so these two constants move the answer most (risk 7, 7b).
Replication's cost (+9% per statement, risk 9) was measured once, at load
2–6. And the Keeper overrun bound behind the consumer's 20 s margin (a
commit landed 19.0 s past a 10 s limit; every statement answered within
29.0 s, under a 30 s session timeout) was measured on a 3-node Keeper on one
box, where a "partition" is iptables on loopback (risk 5c; AMBIGUITY C1, C5).

This runbook re-measures the constants on a dedicated idle machine at
production statement sizes, and the overrun bound on a production-like
Keeper.

## What this settles

| Q | Question | Tied to | Step |
|---|---|---|---|
| CEN-1 | Insert µs per span/log row in the consumer's statements, option 2 (ClickStack DDL + rollup view), at 32 objects per statement and at 1 | §3 `usRow` 9.0 (loaded-box ratio × idle base); risk 7b | §2 |
| CEN-2 | The option-2 ratio against the pre-alignment tables, on an idle box (the loaded ×2.40–2.62) | risk 7b; hyperdx/results/schema3-insert.md | §3 |
| CEN-3 | Merge µs per row for option 2 at production part counts (a day of real statement sizes, not a projection) | §3 `mergeRow` 20.1; risk 7 (the 5–60× extrapolation) | §4 |
| CEN-4 | Fixed ms per inserted object at production object sizes | §3 `fixedMs` 15.3 | §2 (the fit) |
| CEN-5 | Replication's insert cost on an idle pair (+9% per statement, +20% whole-replica CPU, measured at load 2–6) | risk 9; D13 | §5 |
| CEN-6 | **The Keeper overrun bound** on three Keeper hosts with real networks: how far past `max_execution_time` can a replicated INSERT commit, and is it below the session timeout? | risk 5c; AMBIGUITY C1 (settle bound "measured, not proven"), C5; STPA LS-2; the consumer's slack/margin 20 s, TTL 75 s | §6 |
| CEN-7 | HTTP 200 then an exception in the body: does it happen at the consumer's result sizes? | AMBIGUITY C7 (unverified) | §7 |

## 0. The machines

**The idle bench machine** (CEN-1…4, and real-cluster-telemetry.md §4):
dedicated, not shared, not burstable. Suggested: **c7i.8xlarge** (32 vCPU,
64 GB; the calculator's node is 32 vCPU with 4 GB/vCPU) or a bare-metal host
of the same class, with a 500 GB gp3/NVMe volume; Ubuntu 24.04 (the host
builds need glibc ≥ 2.39). Install ClickHouse **26.10.1.618** (the version
every [M] used; DECISIONS risk 12 pins behaviour to it), a SeaweedFS 4.47 or
MinIO on localhost for the objects, and build `otap-s3pq`, `consume`,
`otlpsend`, `mixgen` (eks/images.sh's Go builds; `cargo build --release
--bin otap-s3pq --bin consume` in otap-rs after `scripts/fetch-upstream.sh`).

**Isolation, as bench/clean did it:** ClickHouse pinned to its own CPUs
(`taskset -c 2-31 clickhouse server`), the consumer and the store on the
others (`taskset -c 0-1`), nothing else running; every run starts only when
the 1-minute load is ≤ 0.45 (`LOAD_MAX`); `env.jsonl` records load, the
steal counter, the CPU model and the kernel around every run. Record the
instance type and whether it is dedicated.

**The replicated set** (CEN-5, CEN-6): 2 ClickHouse replicas (e.g. 2 ×
m6i.2xlarge) and **3 Keeper hosts** (3 × m6i.large, `clickhouse-keeper` as a
systemd unit, **in three AZs or racks**, as production would run them), with
the production Keeper settings (`session_timeout_ms`, `operation_timeout_ms`
from central-replicated/configs/keeper*.xml unless production differs:
record them). The operator host needs passwordless SSH (BatchMode) to the
Keeper hosts and passwordless `sudo` there for `systemctl`, `kill` and
`iptables`: `keeper_faults.sh` stops, partitions and kills Keeper on purpose.
**Use Keepers that serve nothing else.**

**What the owner provides:** the machines (or an AWS account for them), SSH
access, and a decision on the production Keeper topology and session timeout
to test (the bound under test is the session timeout).

**Cost/time:** bench machine ~$1.4/h × ~10 h (§2–§4, with the day-long
merge run of §4 at a low rate: ~24 h more); replicated set ~$1/h × ~4 h.
About **$60–80** on AWS on-demand. Wall time: one working day plus the 24 h
merge run.

## 1. Server settings

Start ClickHouse with query_log and part_log on (both default on in a
packaged install), no other users. For the §2–§4 numbers to compare with
bench/clean, keep `bench/clean/lib/server-config.xml`'s settings (logging
levels, `max_server_memory_usage`); note every difference.

## 2. Insert cost at production statement sizes (CEN-1, CEN-4)

```sh
export OUT=$STATE/central/insert CH=http://127.0.0.1:8123 S3_ROOT=http://127.0.0.1:8333/bench/validation/$RUN BIN=/opt/otel/bin
central/bench_insert.sh prep      # 4 datasets x (32 traces + 32 logs requests of 10,000 rows) through otap-s3pq:
                                  # 128 trace objects and 128 log objects of 10k rows (mixgen: 120 services)
central/bench_insert.sh run       # REPS=5 x --max-batch 1 and 32: a fresh database each, consume --once,
                                  # every INSERT's own CPU from query_log -> $OUT/raw/statements.jsonl.gz
central/bench_insert.sh analyze   # bench/clean block 2's fit (per statement + per object + per row) and usRow
```

`$OUT/usrow.txt` prints µs/row per table and max-batch, and **usRow** as the
calculator defines it (0.75 spans + 0.25 logs, 32 objects per statement, the
option-2 tables with their rollup view, which is what the consumer creates).
`results.md` has the fit; its per-object term at 32/statement is `fixedMs`'s
replacement.

**Pass:** 5 repetitions within ±15% of their median; no run flagged in
`env.jsonl` (load at start > 0.5 or steal > 2%). **Record** usRow, the
per-object cost, and the ratio to the calculator's 9.0.

## 3. Option 2 against the pre-alignment tables, idle (CEN-2)

The loaded-box ratio came from hyperdx/scripts/schema_bench.py. Rerun it on
the idle machine, same objects:

```sh
cd ../../hyperdx/scripts
CH=http://127.0.0.1:8123 S3_PREFIX=http://127.0.0.1:8333/bench/validation/$RUN/edges CONSUME=$BIN/consume \
  VARIANTS=old,new_main,new,full OUT=$STATE/central/schema.jsonl python3 schema_bench.py 6
python3 schema_bench.py --summarize $STATE/central/schema.jsonl
```

(`S3_PREFIX` is where `bench_insert.sh prep` wrote; schema_bench inserts one
object per statement.) **Record** new/old for insert and merge (the loaded
box said ×2.40–2.62 insert, ×1.95–2.02 merge); if the idle ratio differs by
more than 15%, `usRow` and `mergeRow` change by the same factor.

## 4. Merges at production part counts (CEN-3)

bench/clean projected merge cost to 10⁴ parts per daily partition from runs
that reached 161–2,100. Measure it instead: 24 h of continuous ingest at a
steady rate into one database, with merges running as in production, then
the merge CPU per merged row from part_log.

```sh
# a steady feed: an edge and soaksend (or the mirror of real-cluster-telemetry.md) for 24 h, consumed continuously
consume --s3 $S3_ROOT/edges-merge --key otel --secret otelsecret --ch $CH --db merge_day --max-batch 32 &
# after 24 h (the day's partition has seen every merge it will see while it is the hot partition):
clickhouse client --query "
  SELECT table, count() AS merges, sum(rows) AS rows_merged,
         round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / sum(rows), 2) AS merge_us_per_row_merged,
         round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / any(t.inserted), 2) AS merge_us_per_row_inserted
  FROM system.part_log
  JOIN (SELECT table, sum(rows) AS inserted FROM system.part_log WHERE database = 'merge_day' AND event_type = 'NewPart' GROUP BY table) AS t USING table
  WHERE database = 'merge_day' AND event_type = 'MergeParts' GROUP BY table ORDER BY table FORMAT PrettyCompactMonoBlock"
```

`merge_us_per_row_inserted` is the calculator's `mergeRow` (merge CPU per
row ever inserted, all levels of merging). Also record the parts per
partition at the end of the day and the number of objects per statement the
consumer used. **Pass:** within ±27% of 20.1 (the projection's own error at
low part counts); otherwise replace it.

## 5. Replicated insert cost, idle (CEN-5)

On the replicated pair, the replicated DDL from the consumer's own
(`central-replicated/scripts/ddl.py`, database `vbench_repl` on both
replicas), then the same statements:

```sh
REPLICAS=http://<r2>:8123 DB=vbench_repl CH=http://<r1>:8123 OUT=$STATE/central/insert-repl central/bench_insert.sh run
central/bench_insert.sh analyze
```

The run passes `--ch r1,r2 --sync-replica --no-ddl` as the replicated
consumer does and collects both servers' query_log. **Record** µs/row per
statement against §2's single node (+9% measured at load 2–6) and both
replicas' total CPU over the run (`system.events` deltas; +20%).

## 6. The Keeper overrun bound (CEN-6)

```sh
KEEPERS="k1 k2 k3" R1=http://<r1>:8123 R2=http://<r2>:8123 OUT=$STATE/central/keeper \
  SSH_USER=ec2-user KEEPER_UNIT=clickhouse-keeper DURATION=1800 BUDGET=10 central/keeper_faults.sh
```

`keeper_faults.sh` is central-replicated/scripts/keeper_overrun.sh and
keeper_long_outage.sh with the faults sent over SSH to real hosts: leader
SIGSTOP, a node partitioned by iptables (client and raft ports, both
directions), quorum lost by SIGSTOP of two nodes, leader SIGKILL and
restart, and (kind 4) quorum lost for 15–40 s while an 8-second insert is
committing: the case that measured 19.0 s past a 10 s budget. It records
each replica's `system.zookeeper_connection.session_timeout_ms`, then
`overrun.py` reports per statement the start, the answer, the part's commit
time (part_log `NewPart`, same query_id) and the overrun past the budget.
Every fault is undone on exit (SIGCONT, the run's tagged iptables rules
deleted, the unit started).

Run it at least twice: `BUDGET=10` (the consumer's) and `BUDGET=2` (more
statements in flight per fault), 30 minutes each.

**Pass:** every commit lands before `start + session_timeout_ms` (so before
budget + slack = 30 s with the 20 s slack), i.e. the max overrun past the
budget is **< session timeout − budget**; no commit after its statement's
answer on the inserting replica. **Fail:** any commit later than that means
the consumer's margin (`--keeper-slack`, TTL 75 s) is short, and the bound
is not the session timeout: stop, keep `$OUT`, and re-open risk 5c.
**Record** the max overrun per fault kind, and the session timeout.

## 7. HTTP 200 then an exception (CEN-7)

AMBIGUITY C7: ClickHouse can send 200 and then an exception in the body once
it has streamed part of a result. The consumer's queries return a few lines.
On the bench machine, a result as large as the largest check the consumer
sends (the count check's GROUP BY over 32 content keys; the audit's
candidates) with a limit that trips mid-stream:

```sh
curl -sS -D - "$CH/?max_result_rows=1000&result_overflow_mode=throw&send_progress_in_http_headers=1" \
  --data-binary "SELECT number, repeat('x', 100) FROM numbers(100000) FORMAT TSV" | head -3
curl -sS -D - "$CH/?max_result_rows=1000&result_overflow_mode=throw&wait_end_of_query=1" \
  --data-binary "SELECT number, repeat('x', 100) FROM numbers(100000) FORMAT TSV" | head -3
```

Record the status code and whether `X-ClickHouse-Exception-Code` is set in
each case, and the size at which the first form starts streaming before the
error (vary `numbers(N)`). If the consumer's largest real answer (the
audit's) is anywhere near it, add `wait_end_of_query=1` to its queries (C7's
own recommendation).

## 8. Rows to update with the results

Fill `results/TEMPLATE.md` (section CEN) first; then:

- [ ] **DECISIONS §3** constants table: `usRow` (CEN-1; "loaded-box ratio × idle base" → idle [M] at 32 objects/statement), `fixedMs` (CEN-4), `mergeRow` (CEN-3; "projected" → measured at production part counts); the option-2 ratios (CEN-2); recompute the mid scenario (calculator v13) and the vCPU/shape line.
- [ ] **DECISIONS §4 risk 7** (merge extrapolation) and **7b** (loaded-box ratios): retire or restate.
- [ ] **DECISIONS §4 risk 9** (replicated insert cost): the idle +x% (CEN-5); add it to the calculator ("the +9% per statement is not in it yet").
- [ ] **DECISIONS §4 risk 5c** and **§1.4** "The slack is real" bullet: the bound on a production-like Keeper (CEN-6), with the session timeout used.
- [ ] **DECISIONS D9 / D13**: the margin (20 s) confirmed or changed; the start-up check's rule unchanged.
- [ ] **AMBIGUITY C1** settle bound: "measured on one box" → measured on real Keeper hosts; **C5** evidence; **C7** (CEN-7): *handled* or the `wait_end_of_query` change.
- [ ] **central-replicated/README.md** §4 (c): a production-like Keeper subsection.
- [ ] **bench/clean/README.md**: the idle-machine rows beside the 4-vCPU ones (same table).
- [ ] **STPA LS-2** (commit up to 29 s later): the measured bound (hand to the STPA owner).
