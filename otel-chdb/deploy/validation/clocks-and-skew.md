# Clocks and skew: validation runbook

AMBIGUITY.md E6 is *open*: "no NTP or skew metric". Four parts of the design
read a wall clock and assume it is close to the others:

| Clock use | Assumption | Constant | Where |
|---|---|---|---|
| `received_at` = the edge's wall clock at custody, never re-read; tables are partitioned by `toDate(received_at)` and retention drops partitions | a skewed edge puts rows in the wrong day (harmless to exactness: rows and metadata agree), and a replay keeps its day | – | D19, FORMAT.md §2 |
| `complete_through = min(t_list − skew, min over lanes)`: a lane born after the consumer's LIST has every `received_at` above its birth, up to the edges' skew against the consumer | edge clock − consumer clock < `--wm-skew` | **5 s** | FORMAT.md §3, `consumer/watermark.rs` |
| the server-side fence `now64() <= sent_wall + ttl − margin − budget` compares the worker's wall clock with ClickHouse's | worker − ClickHouse < the lease margin | **20 s** | D9, risk 5a |
| epochs are named by the wall clock (ms); checkpoint compaction assumes no producer steps back past the zombie bound | a step back < the zombie bound | **10 min** (`--zombie`) | D12, risk 5b |
| SigV4 | skew < 15 min, or 403 `RequestTimeTooSkewed` (a stall, never a guess: AMBIGUITY E6 keeps the slot unresolved) | **15 min** | D18 |

This runbook measures the skew these constants have to cover, on real
nodes, and then steps one node's clock on purpose to watch each consequence
happen.

## What this settles

| Q | Question | Tied to | Step |
|---|---|---|---|
| CLK-1 | Per-node clock offset against the reference, drift, and the widest node-to-node spread, over ≥ 24 h | AMBIGUITY E6; risk 5 | §2 |
| CLK-2 | Worker (consumer) and ClickHouse clocks against each other | risk 5a (the fence); D9 | §2 (`CH_URL`) |
| CLK-3 | The edges' clocks against the store's (S3 LastModified vs `received_at`), from the objects alone | E6; a cross-check needing no node access | §3 |
| CLK-4 | Is `--wm-skew 5 s` enough? Is the fence's 20 s? The zombie bound's 10 min? | FORMAT.md §3; D9; D12 | §2 verdicts |
| CLK-5 | What share of rows land in the wrong day's partition, and does anything but the day change? | D19; E6 | §2, §4 |
| CLK-6 | A clock stepped past each bound: +30 s / −30 s, across midnight, +16 min (SigV4), −15 min (epoch below the floor): what the edge, the consumer and `complete_through` do | E6's fault injection ("a clock step: replay_received.sh" is the only one, an LD_PRELOAD shim) | §4 |

## 0. Environment

- **Any cluster** for §2–§3: the EKS validation cluster (reference: the
  Amazon Time Sync Service at 169.254.169.123, what AL2023's chrony follows),
  a site cluster (reference: the site's NTP servers), or better both. A
  production cluster is fine for §2: the probe is one unprivileged pod per
  node, reading its node's clock, sending a few UDP packets every 10 s.
- **§4 needs the EKS validation cluster** and its `ng-clock` node group
  (eks-aws.md §1): the only node whose clock is stepped, tainted so nothing
  else runs there, terminated at the end. Never step a clock on a shared node.
- Off-cluster hosts that matter (the consumer's, ClickHouse's): run
  `clocks/sntp_probe.py` there too (stdlib python3).
- **Owner provides:** read access to the cluster, UDP/123 egress from pods to
  the reference (or the site's NTP IPs), and for §4 the EKS account.
- **Cost/time:** §2 runs 24 h–7 days unattended (negligible cost); §3 10 min;
  §4 2 h, plus one m6i.xlarge for 2 h (~$0.40).

```sh
cd otel-chdb/deploy/validation
export RUN=v1 BUCKET=... REGION=us-east-1        # as in eks-aws.md; KUBECONFIG defaults to its cluster
```

## 1. Why a probe, not the nodes' chrony

`chronyc tracking` on each node says how far the node thinks it is from its
source; it cannot see a source that is itself wrong, and it needs node
access. The probe asks the reference directly from every node (SNTP, RFC
4330: offset = ((t1 − t0) + (t2 − t3)) / 2, error ≤ delay/2), and optionally
ClickHouse (`now64(6)` over HTTP, error ≤ half the round trip), so every
node's offset is against the same reference. Two references (the AWS link-
local one and a public one) catch a shared bias.

## 2. Skew across nodes, and against ClickHouse (CLK-1, -2, -4, -5)

```sh
REFS=169.254.169.123,time.aws.com CH_URL=http://clickhouse.otel-validate:8123 clocks/run.sh up
# ... 24 h (7 days for weekly effects; collect daily: pod logs rotate)
clocks/run.sh collect
clocks/run.sh report               # skew_report.py: per node p50 / p99 / max |offset|, drift ms/h; the spread per
                                   # minute; each node vs ClickHouse; verdicts; the wrong-day share
clocks/run.sh down
# the consumer's and ClickHouse's hosts, if not in the cluster:
REFS=... NODE=consumer-host python3 clocks/sntp_probe.py >> ~/.otel-validation/$RUN/clocks/samples-consumer.log
```

`report.txt` ends with a verdict per constant, using the worst node offset
or node-to-node spread seen (for the fence: the worst node-vs-ClickHouse
offset):

| Verdict | Pass |
|---|---|
| wm-skew 5 s | worst spread < 5 s, and **≥ 10× headroom** (a node that loses NTP drifts; chrony's typical free-run drift is ~ms/h) |
| lease margin 20 s (fence) | worst worker/node vs ClickHouse < 20 s, ≥ 10× headroom |
| zombie bound 10 min | worst step back < 10 min (a step back shows as a negative jump in one node's series) |
| SigV4 15 min | worst < 15 min |
| wrong-day rows | the share E|offset| / 86,400 s: recorded (at 10 ms it is ~1e-7 of rows) |

A healthy NTP fleet will pass every one by orders of magnitude; what this
runbook buys is the measured number and a probe that can become the missing
production metric (AMBIGUITY E6: "no NTP or skew metric"). Record the p99
and max per node and the drift of the worst node.

## 3. Edge vs store, from the objects (CLK-3)

```sh
eks/fleet_test.sh gc off           # keep the objects (GC deletes ingested slots)
eks/fleet_test.sh send 90; eks/fleet_test.sh waitsend 90
sed "s#{s3}#'https://s3.$REGION.amazonaws.com/$BUCKET/validation/$RUN/edge/*/*/traces/*/*.parquet', '<key>', '<secret>'#" \
  clocks/s3_skew.sql | kubectl -n otel-validate exec -i toolbox -- sh -c 'curl -sS $CH --data-binary @-'
eks/fleet_test.sh gc on
```

Per producer: the smallest (S3 LastModified − `received_at`). It is the
custody time plus the skew, and custody time is ≥ 0, so a negative minimum
beyond S3's 1 s resolution means that edge's clock is ahead of S3's. **Pass:**
no producer ahead by more than 1 s; agrees with §2's offset for that node.

## 4. A clock stepped on purpose (CLK-5, CLK-6)

On the `ng-clock` node only (`CONFIRM_CLOCK_STEP=yes` is required):

```sh
export CONFIRM_CLOCK_STEP=yes
clocks/clock_step.sh node-up                    # ng-clock: 0 -> 1 node
clocks/clock_step.sh edge-up                    # otap-clock-0 (the StatefulSet's pod template, buffer on emptyDir)
                                                # + clock-sender (4 traces requests every 60 s, timestamps = now)
clocks/clock_step.sh observe                    # the baseline: commit outcomes, days, epochs, the watermark
```

Then each step, observe for 5 minutes, restore, observe:

| Step | Command | Expect (and record) |
|---|---|---|
| +30 s | `step 30` | rows' `received_at` 30 s ahead; commits normal; `complete_through` unaffected (a lane's watermark is its own `received_at`s; the `t_list − skew` cap only matters for a lane born after the LIST: a new epoch now would be up to 30 s ahead of `--wm-skew` 5 s, i.e. **this step exceeds the watermark's allowance** — look for a `complete_through` that passed a request still in custody: compare `consumer_complete_through_seconds` with the oldest `received_at` in the publisher's buffer) |
| −30 s | `step -30` | a new epoch named 30 s earlier than the last one; nothing breaks (well inside the 10-min zombie bound) |
| midnight | `set "$(date -u +%F) 23:59:00"` | requests stamped either side of 00:00 land in two days' partitions; the count check reads the right partitions (its range is the batch's own day ± 3 days); exactly once |
| +16 min | `step 960` | every PUT answered 403 `RequestTimeTooSkewed`: `s3pq_commit_outcomes_total{outcome="unresolved"}` climbs, nothing is committed or guessed, the buffer grows, readiness holds until the buffer fills; after `restore`, everything commits once. (IRSA's STS call may fail too: note it) |
| −15 min | `step -900` | the restarted exporter names its next epoch 15 min earlier than epochs already written: **below the zombie bound's 10 min**. Watch the consumer: the new epoch must still be discovered and ingested (checkpoint compaction assumes epochs don't go back past `--zombie`); if it is skipped or ingested late, that is risk 5b happening: record it |

```sh
clocks/clock_step.sh restore; clocks/clock_step.sh observe       # between steps and at the end
clocks/clock_step.sh edge-down
clocks/clock_step.sh node-down                  # the node, and its stepped clock, are terminated
eks/fleet_test.sh total                         # the clock node's rows: exactly once overall
```

**Pass:** exactly once for the clock node's rows through every step; no
`inconsistent` outcome; a 403 period resolves without a duplicate; the −15
min epoch is ingested (or the failure is recorded as risk 5b's evidence).

## 5. Rows to update with the results

Fill `results/TEMPLATE.md` (section CLK) first; then:

- [ ] **AMBIGUITY E6**: status *open* → *partly* (a measured skew and a probe) or *handled* (with a production metric: the probe's offset as a gauge and an alert tighter than 5 s); the fault-injection table's E6 row gets `clock_step.sh`.
- [ ] **AMBIGUITY "Open and unverified rows" item 4** (E6).
- [ ] **DECISIONS §4 risk 5** (a) fence: worker vs ClickHouse measured; (b) epoch step-back: the −15 min result; (d) SigV4: the 403 stall observed.
- [ ] **FORMAT.md §3**: `--wm-skew` 5 s: confirmed against the measured spread, or changed.
- [ ] **DECISIONS D19**: the wrong-day share of rows (measured).
- [ ] **deploy/alerts/**: a skew alert (a follow-up: the probe as a metric); recorded as a follow-up, not built here.
