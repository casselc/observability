# Real-cluster telemetry: validation runbook

Every rate in DECISIONS.md §1.2 is an estimate, the stored bytes per span
and per log (`bSpan` 80, `bLog` 60) are estimates that drive ~88% of the
storage total, and every byte and CPU figure behind them came from
synthetic data (12–21 resource keys; real fleets carry 20+). The entity
schema's savings (~2.8× less insert CPU per span, 3.2× per log, −44 to −57%
bytes) were measured on synthetic, time-sparse rows on a loaded box. This
runbook runs the edge on a real cluster with real workloads, measures what
the calculator and the entity-schema decision need, and runs the entity
controller at real churn.

**This is the measurement that gates the entity-schema switch** (DECISIONS
D21: "central keeps `ResourceAttributes`; the schema switch and the rewrite
proxy not decided"; entities/README §7 step 2).

## What this settles

| Q | Question | Tied to | Step |
|---|---|---|---|
| TEL-1 | Spans/s, logs/s per pod and per node, active series per pod and per node, the scrape interval | DECISIONS §1.2 (all [E]); risk 2 | §1 |
| TEL-2 | Rows/s per signal as central ingests them (after sampling), and the diurnal peak/average | §1.2; calculator basis | §3 `rates`, `rates_by_hour` |
| TEL-3 | Stored bytes per row after merges: spans, logs, points (layout B), series | §3 constants `bSpan`, `bLog` ([E]), `bPointB`, `bSeries` (synthetic); risk 2 | §3 `bytes_per_row` |
| TEL-4 | Parquet bytes per row on the wire | `bPq` 50 ([E]), `bPqPointB` | §3 `wire_bytes` |
| TEL-5 | Resource and span/log attribute shapes: keys per row, cardinalities, which keys the catalog covers | risk 2 ("real fleets carry 20+"); entities §3.1 covered set | §3 `resource_keys`, `attr_shape`, `attr_keys_top` |
| TEL-6 | `resource_id` distribution: resources, rows per resource, churn per hour, rows without an id | D21; entities §4.4 (dictionary memory), §3.6 (grace) | §3 `resource_ids`, `resource_churn`, `resource_id_zero` |
| TEL-7 | Rows per edge object and objects/s (the per-object fixed cost `fixedMs` is charged per object) | §3 `fixedMs`; risk 7b lever (a) larger objects | §3 `objects` |
| TEL-8 | **Entity schema on real rows**: insert µs/row, merge µs/row, stored bytes, against option 2 (ClickStack's DDL, the consumer's at HEAD) | risk 7b lever (d); entities §1 table; D21 | §4 |
| TEL-9 | Views over the entity schema exact on real rows (count, hash, `EXCEPT ALL` both ways) | entities §4.3 | §4 |
| TEL-10 | The entity controller at real churn: freshness, CPU, memory, record volume | entities controller README "Measured" (KWOK); STPA LS-5 | §5 |
| TEL-11 | `resource_id` agreement: the controller's ids against what the agents compute (API view), and against the ids on real rows | k8s-sim.md §3 (KWOK, 3 planted mismatches, fix unit-tested only); controller README gap 1 | §5 |

## 0. Environment and what the owner provides

**A real cluster with real workloads.** In order of preference:

1. **A read-only mirror of production telemetry.** The production collectors
   get one more exporter that sends a copy to this runbook's agents. It must
   not be able to push back on production: a `sending_queue` that drops when
   full (`blocking: false`), a short `retry_on_failure.max_elapsed_time`, and
   `timeout: 5s`:

   ```yaml
   exporters:
     otlp/mirror:
       endpoint: otel-agent.otel-edge.svc:4317      # this runbook's agents (deploy/base/common)
       tls: {insecure: true}
       timeout: 5s
       sending_queue: {enabled: true, queue_size: 5000, blocking: false}
       retry_on_failure: {enabled: true, max_elapsed_time: 60s}
   # add otlp/mirror to the traces, logs and metrics pipelines' exporters
   ```

   The copy is after the production collector's sampling, which is what the
   pipeline would receive. **This is a change to production config: the
   owner decides, and a production operator applies it.**
2. **A staging cluster** running the real services with realistic traffic
   (a load test or replayed traffic): the same shapes, not the same rates
   (use §1's Prometheus numbers for the rates).
3. **The EKS validation cluster** (eks-aws.md) with a representative app mix
   (e.g. the OpenTelemetry demo): attribute shapes only; not a rate source.

**Also needed:**

- Read access to the cluster's Prometheus (a `port-forward` is enough) for §1.
- A central to ingest the mirror: the **idle bench machine** of
  central-idle-bench.md (ClickHouse 26.10.1.618 with query_log and part_log,
  a local SeaweedFS for §4) and a bucket the edge can write (the EKS bucket,
  or Objects at a site).
- The edge deployed on that cluster (eks-aws.md §1–§6, or `TARGET=nutanix
  eks/deploy.sh edge`), with the consumer pointed at the bench machine's
  ClickHouse (`--ch`), database `mirror`.
- **Duration:** ≥ 24 h for the diurnal shape; 7 days to see a weekly cycle,
  deploy churn and series churn. The storage for a mirror of one cluster
  (~1/20 of the region: ~100k rows/s at the mid scenario) is ~300 GB/day
  compressed at the calculator's bytes [E]; mirror a namespace subset (a
  `filter` processor on the mirror pipeline) if the bench machine is smaller.
- **Data handling:** real telemetry can hold personal data. The owner decides
  where the mirror may be stored, who can read it, and for how long; the
  measurements below keep only aggregates (except §4's staged sample: drop it
  after the run).

**Cost:** the bench machine for the duration (e.g. c7i.8xlarge on demand
~$1.4/h: ~$35/day) plus the bucket's requests (~$1–2/day for one cluster's
objects) plus storage. Time: 1 h to set up, the observation period, then ~3 h
of measurement (§3–§5).

## 1. Rates from Prometheus (TEL-1): no mirror needed

```sh
kubectl -n monitoring port-forward svc/prometheus-operated 9090 &
python3 ../../acceptance/rates/collect.py --prom http://127.0.0.1:9090 [--selector 'cluster="prod-1"'] --heavy --out $STATE/rates.json
python3 ../../acceptance/rates/to_calculator.py $STATE/rates.json            # --basis avg | peak
```

acceptance/RUNBOOK.md §8 explains every input and the pitfalls (HA
Prometheus pairs double the series; collector receiver counts include every
tier). Paste the printed snippet into the calculator page to recompute the
sizing with real rates.

## 2. The mirror

Deploy the edge on the mirrored cluster as in eks-aws.md §6 (or at a site)
and point the consumer at the bench machine:

```sh
consume --s3 <S3_BASE> --key ... --secret ... --ch http://<bench>:8123 --db mirror --metrics-addr 0.0.0.0:9464 &
consume gc --s3 <S3_BASE> --key ... --secret ... --ch http://<bench>:8123 --db mirror --every 60s &
```

Check after an hour: `telemetry/measure.sh rates resource_id_zero objects`
(rows arrive, every trace and log row has a `resource_id`, objects are
reasonably full). Leave it for the observation period.

## 3. Measurements on real rows (TEL-2 … TEL-7)

```sh
export CH=http://<bench>:8123 DB=mirror FROM='2026-10-06 00:00:00' TO='2026-10-07 00:00:00' OUT=$STATE/telemetry
telemetry/measure.sh optimize            # once: merges the day before FROM to one part per partition
FROM='2026-10-05 00:00:00' TO='2026-10-06 00:00:00' telemetry/measure.sh bytes_per_row bytes_by_column index_bytes
telemetry/measure.sh                     # every block for the window -> $OUT/<block>.tsv
S3_GLOB="'https://s3.us-east-1.amazonaws.com/<bucket>/validation/$RUN/edge/*/*/traces/*/*.parquet', '<key>', '<secret>'" \
  telemetry/measure.sh wire_bytes        # while GC is paused, or on a lane the consumer does not own
```

| Block | Gives | Calculator / decision input |
|---|---|---|
| `rates`, `rates_by_hour` | rows/s per signal and cluster; the hourly curve | spans/logs/points per s (÷ pods, nodes from §1); peak/avg |
| `bytes_per_row` (on the merged day) | compressed B/row per table | `bSpan`, `bLog`, `bPointB`, `bSeries` |
| `bytes_by_column`, `index_bytes` | where the bytes go (the resource map and its items index are what the entity schema removes) | TEL-8's expected saving |
| `wire_bytes` | Parquet B/row | `bPq`, `bPqPointB` |
| `resource_keys`, `attr_shape`, `attr_keys_top` | keys per row, cardinalities, covered vs residual | the synthetic data's 12–21 keys vs real; entity §3.1 covered set; residual size |
| `resource_ids`, `resource_churn`, `resource_id_zero` | resources, rows per resource (skew), new resources per hour | dictionary size (entities §4.4: 2.15 GB for the synthetic fleet's 90 days); grace-window share |
| `objects` | rows per object, objects/s | `fixedMs` per object; risk 7b lever (a) |
| `series` | series per pod, attribute keys | series inputs; `bSeries` |

**Pass** is not a threshold here: each value replaces an estimate. Flag for
the owner any value more than 2× from the calculator's current constant.

## 4. The entity schema on real rows (TEL-8, TEL-9): the gate

The entities spike's own scripts, unchanged, on staged real rows, on the
**idle** bench machine (central-idle-bench.md §0's gating; a loaded box is
what made the synthetic numbers uncertain). They expect a SeaweedFS on
`127.0.0.1:18333` with keys `otel`/`otelsecret` and a bucket `ent-objects`,
and a ClickHouse on `CH` (default `127.0.0.1:18123`) with `CH_CLIENT` set to
the `clickhouse` binary for `clickhouse local` merges.

```sh
cd ../../entities/scripts
export CH=http://127.0.0.1:18123 CH_CLIENT=/path/to/clickhouse AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret
# 1. stage an hour of real traffic, 60 objects per signal (by content_key)
python3 - <<'EOF' > /tmp/stage.sql
import sys; s = open("../../deploy/validation/telemetry/stage_real.sql").read()
for k, v in {"src": "mirror", "dst": "ent_src", "from": "2026-10-06 14:00:00", "to": "2026-10-06 15:00:00", "n": "60"}.items():
    s = s.replace("{" + k + "}", v)
print(s)
EOF
$CH_CLIENT client --multiquery < /tmp/stage.sql          # prints objects, rows, resources, resource keys vs residual keys
# 2. the same scripts as entities/README.md §9 (objects.py builds the a / b / ann shapes from ent_src)
curl -X PUT --aws-sigv4 aws:amz:us-east-1:s3 --user otel:otelsecret http://127.0.0.1:18333/ent-objects
python3 objects.py --n 30
python3 bench.py 6 --variants a,b2,ann                    # insert CPU and bytes, 5 measured reps (rep 0 warms up)
python3 merge.py 4 --variants a,b2,ann                    # merge CPU, clickhouse local
python3 load.py a,b2 --objects 60 && python3 conformance.py b2 ent_b2       # exact through the view on real rows
python3 summarize.py bench ; python3 summarize.py conformance
```

What needs a catalog: `load.py`/`conformance.py` resolve `resource_id` through
the catalog database the entity spike built (`ent_cat`, from `fleet.py`). For
real rows the catalog is the aggregator's (§5, database `k8s_cat`) plus the
announcements: point the scripts' catalog at it (entities/README §9's
`dicts.py` step builds the dictionaries from the catalog), or run TEL-9 last,
after §5 has filled `k8s_cat` for the staged hour.

**The gate (proposed; the owner decides).** Switch to the entity schema if,
on real rows on the idle box:

| Measure | Synthetic (loaded box) | Proposed threshold on real rows |
|---|---|---|
| insert µs/row, option 2 ÷ b2, spans and logs | 2.8× / 3.2× | ≥ 2.0× both |
| stored bytes, b2 vs option 2 | −44% / −57% | ≤ −30% both |
| merge µs/row ratio | 1.9× / 2.7× | ≥ 1.5× |
| conformance (count, hash, `EXCEPT ALL`) | exact | exact |
| rows without a catalog match after the dictionary lag (§5 `joinrate`) | – | < 0.1% |

Below the thresholds the map plus index stays: the saving does not pay for
the rewrite proxy and a HyperDX fork. Record the measured ratios whatever the
outcome; the calculator's risk-7b lever (d) uses them.

Drop `ent_src`, the `ent_*` databases and the `ent-objects` bucket afterwards
(they hold real rows).

## 5. The entity controller at real churn (TEL-10, TEL-11)

On the mirrored cluster (IRSA role `$NAME-entityctl` on EKS; static keys
`ENTITY_KEY`/`ENTITY_SECRET` elsewhere), for the whole observation period:

```sh
export BUCKET=... CLUSTER=<the cluster's k8s.cluster.name> EDGE_DB=mirror
export STATIC=cloud.provider=aws,cloud.platform=aws_eks,cloud.region=us-east-1,cloud.account.id=<id>,deployment.environment.name=prod
telemetry/controller.sh up                  # entityctl: read-only on pods, nodes, namespaces, replicasets, jobs
telemetry/controller.sh aggregator          # into k8s_cat on the validation ClickHouse, merging the edges' announcements
telemetry/controller.sh sample 86400        # CPU (cgroup), memory, records/bytes/objects/relists per day
telemetry/controller.sh freshness           # change -> catalog lag, by level (p50/p95/p99/max)
telemetry/controller.sh volume              # lane objects and bytes: deltas vs syncs
telemetry/controller.sh joinrate            # rows whose resource_id the catalog knows; announce-only share
telemetry/controller.sh agree               # ridcheck: the controller's ids vs the agents' rules, every running container
N=10 telemetry/controller.sh probe          # ridcheck: create / relabel / delete a pod, time to the catalog
```

| Measure | KWOK (k8s-sim.md) | Pass |
|---|---|---|
| freshness p99, resource level | 9.2–9.7 s | < 30 s (the dictionary `LIFETIME(MIN 30 MAX 60)` dominates anyway) |
| controller CPU | 0.002–0.017 cores | < 0.1 core |
| controller memory | 80–300 MB (13 KB of heap per pod) | < 1 GiB |
| record volume per day | deltas ~0.55 MB, syncs 250 MB at 3k pods | recorded (syncs dominate: entities §5 proposes hourly or key-only) |
| `agree` | 3,600 of 3,603 (3 planted edge cases, fixed since and unit-tested only) | 100% match, or every mismatch explained (a new owner kind: Argo Rollouts, KEDA ScaledJobs: controller README gap 7) |
| `joinrate` known share | – | ≥ 99.9% after the lag; the announce-only share is the controller's miss rate |
| relists / gaps | 0 on a healthy API server | recorded; each gap has a record (X1) |

## 6. Rows to update with the results

Fill `results/TEMPLATE.md` (section TEL) first; then:

- [ ] **DECISIONS §1.2**: spans/logs per pod and node, series per pod and node, export interval: [E] → [M] (TEL-1, TEL-2), with the cluster and dates.
- [ ] **DECISIONS §3** constants: `bSpan`, `bLog` ([E] → [M], TEL-3), `bPq` (TEL-4), `bPointB`, `bSeries` (real vs synthetic), `fixedMs` context (rows per object, TEL-7); recompute the sizing (calculator) and the "What moves the answer most" paragraph.
- [ ] **DECISIONS §4 risk 2** (real rates and data shape): retire or restate with the measured spread.
- [ ] **DECISIONS §4 risk 7b** lever (d) and **D21** status: the real-row ratios (TEL-8) and the gate's outcome; "the schema switch ... not decided" → decided, either way.
- [ ] **entities/README.md** §1 table and §4.1: a real-data column; §7 step 2 done.
- [ ] **entities/controller/README.md** "Measured": a real-cluster column (TEL-10); "Known gaps" 1 (agreement after the rule fix) closed or restated (TEL-11).
- [ ] **deploy/results/k8s-sim.md** §9 item 4 (rerun agreement): done on a real cluster.
- [ ] **AMBIGUITY X1, X3, X9**: relists/gaps and announcement counts at real churn.
- [ ] **STPA LS-5** (controller down or lagging): the measured freshness and announce-only share (hand to the STPA owner).
- [ ] The calculator (`central-sizing.html`): the rates snippet from §1 and the new constants.
