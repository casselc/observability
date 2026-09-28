# Validation results: <runbook> — <date>

Copy this file to `results/<runbook>-<YYYY-MM-DD>.md` and fill the section of
the runbook that ran (delete the others). Every value cites its evidence: a
file under `$STATE` copied into `deploy/results/` (name it
`<runbook>-<what>.txt`), or a line of `$STATE/results.tsv`. Labels as in
DECISIONS.md: **[M]** measured here.

| Field | Value |
|---|---|
| run id (`RUN`) | |
| date(s), operator | |
| environment | EKS (account/region, k8s version, node type) / Nutanix (Objects version, site) / machine (type, dedicated?) |
| repository commit the scripts ran from | |
| deviations from the runbook | |

## EKS (eks-aws.md)

| Q | Measured | Pass/fail | Evidence | Rows to update |
|---|---|---|---|---|
| EKS-1 atomic conditional writes | run1/run2 verdicts; create-race winners/round; cas-race lost updates; ambiguous resolved | | `accept/run1.json`, `race.json` | DECISIONS §1.4 `ATOMIC_COND`; AMB S1, S3 |
| EKS-2 409 rate | 409s in `conflicts` (of 25,600); `resolved_own` in the fleet runs | | `accept/conflicts.json`, `fleet/metrics-*.txt` | risk 3; AMB S1, S3 |
| EKS-3 LIST | list-race: missing acked keys, holes, LISTs | | `accept/list.json` | AMB S6 ([D] → [M] for AWS) |
| EKS-4 free slot | head-missing: full / none / prefix-scoped ListBucket; `abac.free_slot_head` | | `accept/hm-*.json`, `abac-*.txt` | AMB S5; `iam/edge-publisher.json` |
| EKS-5 per-prefix limits | first 503 at t=…/rate …; ok/s per minute; time to < 1% 503; missed | | `load/*.tsv`, `*.json` | risk 3 |
| EKS-6 ABAC | FAIL rows (sts / pods / Pod Identity); bucket policy on? | | `abac-sts.txt`, `abac-pods.txt` | STPA R-S7 (owner); D18 |
| EKS-7 credentials | IRSA, Pod Identity env seen; hold: refreshes, errors | | `results.tsv deploy.*`, `accept/hold.json` | §1.1 EKS row; D18 |
| EKS-8 latency / throughput | PUT p50/p99 at 3 MiB, 8 MiB (c=1); objects/s per lane; `consumer_visible_seconds` p50/p99 | | `accept/perf1.txt`, `fleet/lanes.txt` | risk 3 |
| EKS-9 faults | per scenario E1–E17: rows / dup / missing per signal | | `fleet/checks.txt`, `events.log` | AMB E1, E3b, E4, E5; deploy/README §Validation |
| EKS-10 exactly once | `total` duplicates; audit late copies | | `fleet/audit.txt` | D3, D11 |
| EKS-11 cost | PUT/GET/HEAD/LIST per object and per lane-hour; $ per M rows | | `cost-*.json` | calculator prices; D8 |
| EKS-12 GC / watermark | GC lag; checkpoint and gc.json size; `complete_through` lag | | `fleet/metrics-soak.txt` | risk 6; AMB S4, S7, S11 |
| E9 PVC expansion time | request → capacity | | `fleet/events.log` | deploy/README §Durable buffer |
| E11 drain | publisher rescheduled? AZ | | `fleet/events.log` | deploy/README §Runbook |

## Nutanix (nutanix.md)

| Q | Measured | Pass/fail | Evidence | Rows to update |
|---|---|---|---|---|
| Objects version, endpoint, IPs | | | `results.tsv nutanix.objects_version` | §1.1 Nutanix row |
| NX-1 create-only, across gateways | create-only; create-race winners/round with `--race-endpoints` | | `nutanix/race.json` | **risk 1**; §1.4; AMB S1, S2, S10 |
| NX-2 CAS | if-match; cas-race lost updates | | `nutanix/race.json` | risk 1; AMB S3, S4, S11 |
| NX-3 ambiguous | ambiguous-create / -cas resolved | | `nutanix/race.json` | AMB S1, S3 |
| NX-4 read-after-write, 404 | | | `nutanix/list.json` | AMB S5 |
| NX-5 LIST | lag; list-race missing / holes | | `nutanix/list.json` | AMB S6 |
| NX-6 metadata | | | `nutanix/run1.json` | §1.1 |
| NX-7 checksum | default CRC32 accepted? trailer? `when_required` needed? | | `nutanix/run1.json` | §1.1; components/nutanix |
| NX-8 ETag = MD5 | n of 3 sizes | | `nutanix/etag.txt` | AMB X2 |
| NX-9 perf / load | PUT p50/p99; rate of first 503 | | `nutanix/perf*.txt`, `load-*.txt` | risk 3 (Nutanix) |
| NX-10 static keys, CA, ClickHouse | births committed; 497? named collection? | | `credcheck` output | D18 |
| NX-11 ABAC | policy accepted? statements enforced? FAIL rows | | `nutanix/abac.txt` | R-S7 (owner); D18; deploy/README §Write-side ABAC |
| NX-12 edge at the site | per scenario rows / dup / missing; outage drain time | | `fleet/checks.txt` | AMB E1–E5 |
| NX-13 versioning / WORM | | | `nutanix/run1.json` (`bucket-config`) | risk 6 |
| **Decision** | ACCEPTED / fallback (which part of the control plane on Keeper) | | | risk 1 |

## Real-cluster telemetry (real-cluster-telemetry.md)

| Q | Measured | Current value | Evidence | Rows to update |
|---|---|---|---|---|
| cluster, window, mirror mode | | | | |
| TEL-1 spans/logs per pod, per node; series per pod, per node; interval | | 10, 3, 5; 500, 2,000; 30 s [E] | `rates.json`, `to_calculator` output | §1.2 |
| TEL-2 rows/s by signal; peak/avg | | | `telemetry/rates*.tsv` | §1.2 |
| TEL-3 B/span, B/log, B/point (B), B/series (merged) | | 80, 60 [E]; 6.7, 38.4 (synthetic) | `bytes_per_row.tsv` | §3 `bSpan`, `bLog`, `bPointB`, `bSeries`; risk 2 |
| TEL-4 Parquet B/row | | 50 [E]; 24 | `wire_bytes.tsv` | §3 `bPq`, `bPqPointB` |
| TEL-5 resource keys per row (p50/p90); covered share; top attributes | | 12–21 (synthetic) | `resource_keys.tsv`, `attr_shape.tsv` | risk 2 |
| TEL-6 resources; rows per resource; new per hour; rows without id | | | `resource_ids.tsv`, `resource_churn.tsv` | D21; entities §4.4 |
| TEL-7 rows per object; objects/s | | 10,000 (design) | `objects.tsv` | `fixedMs`; risk 7b (a) |
| TEL-8 insert µs/row a vs b2 (span, log); merge; bytes | | 24.6/27.7 vs 8.7/8.7; −44/−57% (synthetic, loaded) | `entities/results/bench.jsonl` (new rows) | **D21 switch**; risk 7b (d) |
| TEL-9 conformance | | exact (synthetic) | `conformance.jsonl` | entities §4.3 |
| **Gate** | switch / keep the map | | | D21 |
| TEL-10 freshness p50/p99; CPU; memory; records/day; syncs/day | | 5.7/9.7 s; 0.003–0.017 cores; 80–300 MB | `telemetry/freshness.txt`, `controller-cost.txt`, `volume.txt` | controller README |
| TEL-11 agree; joinrate; announce-only share | | 3,600/3,603 (KWOK) | `agree.txt`, `joinrate.txt` | controller README gap 1; k8s-sim §9 item 4 |

## Central, idle (central-idle-bench.md)

| Q | Measured | Current value | Evidence | Rows to update |
|---|---|---|---|---|
| machine, pinning, ClickHouse version | | | `env.jsonl` | |
| CEN-1 usRow (32 obj/stmt), per table | | 9.0 | `central/insert/usrow.txt` | §3 `usRow` |
| CEN-4 per-object ms | | 15.3 | `central/insert/results.md` | §3 `fixedMs` |
| CEN-2 option 2 / pre-alignment: insert, merge | | ×2.40–2.62, ×1.95–2.02 (loaded) | `schema.jsonl` | risk 7b |
| CEN-3 mergeRow at a day's part count; parts/partition | | 20.1 (projected) | the part_log query output | §3 `mergeRow`; risk 7 |
| CEN-5 replicated µs/row vs single; whole-replica CPU | | +9%, +20% (load 2–6) | `central/insert-repl/` | risk 9; calculator |
| CEN-6 session timeout; max overrun past budget by fault kind; any commit after the answer? | | ≤ 19.0 s past 10 s; answered ≤ 29.0 s (30 s session) | `central/keeper/summary.txt`, `faults.log` | **risk 5c**; §1.4; AMB C1, C5; D9 |
| CEN-7 200 then an exception: at what size | | untested | the curl output | AMB C7 |

## Clocks (clocks-and-skew.md)

| Q | Measured | Constant | Evidence | Rows to update |
|---|---|---|---|---|
| reference(s), nodes, duration | | | `clocks/report.txt` | |
| CLK-1 per node p99 / max \|offset\|; worst drift ms/h; node spread p99 / max | | | `clocks/report.json` | AMB E6 |
| CLK-2 worker / node vs ClickHouse, max | | fence margin 20 s | `report.json` | risk 5a |
| CLK-3 edge vs S3 (min delay per producer) | | 1 s resolution | `s3_skew` output | AMB E6 |
| CLK-4 headroom: wm-skew, fence, zombie, SigV4 | | 5 s, 20 s, 10 min, 15 min | `report.txt` verdicts | FORMAT §3; D9; D12; D18 |
| CLK-5 wrong-day share | | | `report.txt` | D19 |
| CLK-6 per step: outcomes, days, epochs, watermark; exactly once? | | | `clocks/steps.log` | AMB E6; risk 5b, 5d |
