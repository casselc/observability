# Sorting edge objects by service: edge cost, read savings, routing

This experiment asks whether the Rust edge exporter (otap-rs) should sort
trace and log objects by `(ServiceName, Timestamp)` and cut them into
row-group buckets, and whether service-affine routing across a cluster's
publishers changes the answer. It was run on an idle 4-vCPU box. Each run was
gated on load ≤ 0.45 and checked for steal, and every measured cell has 5
repetitions.

## Step 0: no regression; the bench/clean block 1 slowdown was a harness artefact

bench/clean block 1 showed an 18–33% edge slowdown. This bisect
(`bisect/results.md`) looked for its cause. It compared current head, the
commit before the wire-size change (8cf80ad), and head without the
durable-buffer feature, 5 reps each. All three agree within ±6% on the
series layout, the ClickStack tables and an untouched control (traces through
encbench). Example: control-traces is 32.7 / 31.8 / 31.4 ms per 10k spans,
against 40.0 in block 1. There is no code regression. The block 1 numbers were
inflated by that block's harness, not by otap-rs.

## Recommendation

1. **Keep sorting off by default.** The code stays behind config
   (`parquet.sort`, default `by: none`, which gives arrival order and one row
   group, byte-identical to before).
   - Sorting costs +20–26% edge CPU. That is 12–16 ms per 10k rows, not the
     "about 1 ms" that lake/DESIGN.md assumed.
   - At the mid scenario (600k spans/s + 200k logs/s) that is about **+1.2
     vCPU fleet-wide**. Presorted input saves central only about 0.1–0.3 vCPU.
   - With ClickHouse's default S3 read path it saves **no** read bytes or
     GETs. `s3()` fetches each ~1 MB object whole, whatever the pruning.
2. **Deployments that query the raw tier heavily can turn it on.** Use
   `by: service_time, row_groups: 4, split: range`.
   - Range split is the only layout ClickHouse can prune with plain min/max.
     Hash buckets need a reader that knows the hash. Bloom pushdown on
     `ServiceName` fetched the filters but pruned nothing in 26.10.
   - Readers must use ranged IO: `remote_filesystem_read_method = 'read'` and
     `remote_read_min_bytes_for_seek = 0`. The Parquet footer cache
     (`use_parquet_metadata_cache`) should also be on.
   - With that, a one-service, one-hour query reads about 2× fewer bytes than
     on unsorted objects with the same projection, and 4–6× fewer once
     footers are cached.
   - Objects are 6–8% smaller than unsorted.
   - The CPU cost is about the same as one sorted row group: +23% traces,
     +26% logs.
3. **Don't use 16 buckets at today's ~10k-row objects.**
   - Edge CPU goes up +50–59% (about 1.3 ms per extra row group).
   - Objects grow 4–8%, because the footer grows from 4–6 KB to 43–68 KB.
   - Central inserts cost 10–32% more, because the reader splits the work
     into 16× more row groups.
   - The extra read savings over 4 buckets are real only once the footer is
     cached. A 16-row-group footer costs about as much to fetch as the service's
     data.
4. **Routing is the bigger lever, and it does not need sorting.** With
   `routing_key: service` on the cluster gateway's loadbalancing exporter, a
   one-service, one-hour query touches only the owning publisher's objects.
   - Objects touched per hour drop from 10.8k (N = 3), 28.8k (N = 8) or 57.6k
     (N = 16) to about 3.6k.
   - Those objects hold 40 / 15 / 7.5 services instead of about 120.
   - Narrow-projection bytes read drop **3–20×** even on unsorted objects:
     traces, rank-10 service, 1.9 → 0.65 GB/h at N = 3 and 5.0 → 0.31 GB/h at
     N = 16.
   - At N = 3 the routed objects are still ~10k rows, and a range split on
     top still cuts reads 1.3–2.5×, depending on the service.
   - At N ≥ 8, sorting on top of routing saves only 5–45%. Routed objects are
     small there (the owner's rate × 1 s), and the 64 KiB footer read
     dominates.
   - The cost is load skew. The busiest publisher carries 36% of a cluster at
     N = 8 (2.9× the mean) and 26% at N = 16 (4.1×). The quietest carries
     0.3%. Routing therefore needs either per-publisher headroom for the hot
     service, or a trace-id split for the few services above ~1/N of the
     traffic.
   - Recommendation: **enable service routing at N ≥ 8 where raw-tier
     service queries matter, sized for the skew, and leave sorting off.**
5. **The ~15 ms fixed central cost per object dominates at large N.** This
   holds with or without routing, because routing does not change the object
   count.
   - At T = 1 s, one object per statement, the fixed cost is 1.2 / 1.8 / 4.8 /
     9.6 vCPU fleet-wide at N = 1 / 3 / 8 / 16 (80 / 120 / 320 / 640 objects/s).
   - The consumer's 32 objects per statement cuts that to 0.17 / 0.25 / 0.67 /
     1.34 vCPU.
   - Keep multi-object statements on, and don't raise N without them.

## Setup

- **Data:** `gen/main.go` (mixgen). One publisher of a cluster's gateway,
  with 120 services (Zipf 1.1), 3,000 pods on 200 nodes and 10k rows per
  batch.
  - Traces run at 30k spans/s per cluster and logs at 10k logs/s, the mid
    scenario over 20 clusters.
  - The run has 32 distinct batches per signal, with 120 services in every
    unrouted batch.
- **Query services** (`services.json`), picked by traffic rank:

  | rank | service | share |
  |---|---|---|
  | 1 | recommend | 22.8% |
  | 10 | gateway-4 | 1.8% |
  | 60 | recommend-2 | 0.25% |
  | 120 | gateway-2 | 0.12% |

- **Configurations** (encbench flags; `edge.sh`). The required four are a, b,
  c and d. e–h are extra comparisons:

  | config | layout |
  |---|---|
  | a-unsorted | arrival order (the default) |
  | b-sorted-1rg | sorted by (ServiceName, Timestamp), 1 row group |
  | c-hash-4rg | sorted, 4 row-group buckets by xxh3(service) mod 4 |
  | d-hash-16rg | sorted, 16 buckets |
  | e-range-4rg | contiguous service ranges cut into ~equal row groups |
  | f-range-16rg | as e, 16 row groups |
  | g-hash-4rg-bloom | c plus a Parquet bloom on ServiceName |
  | h-hash-16rg-bloom | d plus a Parquet bloom on ServiceName |

  Every sorted object records its layout in the footer key `oscope-sort`: the
  split, and the bucket of each row group.

## Step 1 results (full tables: `results.md`)

### Edge CPU and object size

Measurement: encbench does flatten + sort + encode + create-only PUT per
10k-row request. Values are medians over 5 reps of 60 timed batches each.

| config | traces CPU ms/10k | Δ | traces object | Δ | logs CPU ms/10k | Δ | logs object | Δ |
|---|---|---|---|---|---|---|---|---|
| a-unsorted | 66.2 | – | 986 KB | – | 59.8 | – | 707 KB | – |
| b-sorted-1rg | 80.9 | +22% | 942 KB | −4.4% | 71.5 | +20% | 665 KB | −5.9% |
| c-hash-4rg | 82.0 | +24% | 969 KB | −1.7% | 72.1 | +21% | 681 KB | −3.7% |
| d-hash-16rg | 103.2 | +56% | 1,067 KB | +8.2% | 91.4 | +53% | 760 KB | +7.5% |
| e-range-4rg | 81.6 | +23% | 924 KB | −6.2% | 75.6 | +26% | 654 KB | −7.5% |
| f-range-16rg | 102.3 | +55% | 1,031 KB | +4.6% | 89.6 | +50% | 735 KB | +3.9% |
| g-hash-4rg-bloom | 83.0 | +25% | 970 KB | −1.7% | 74.1 | +24% | 682 KB | −3.6% |
| h-hash-16rg-bloom | 105.4 | +59% | 1,068 KB | +8.4% | 92.9 | +55% | 762 KB | +7.7% |

- Min–max spread is within about ±7% of each median.
- 97 gated runs. Steal per run: median 0.35%, max 0.63%. No run was flagged.
- Footers: 4–6 KB at 1 row group, 13–19 KB at 4, and 43–68 KB at 16.

### Central INSERT … SELECT with presorted input

Measurement: the real consumer (`consume --once`) loads each configuration's
32 + 32 objects into a fresh database. CPU is `OSCPUVirtualTimeMicroseconds`
per inserted row, median over 5 reps.

| signal | objects/statement | a | b-sorted-1rg | d-hash-16rg | f-range-16rg |
|---|---|---|---|---|---|
| traces | 1 | 5.78 µs | 5.47 (−5%) | 7.61 (+32%) | 6.74 (+17%) |
| traces | 32 | 4.90 | 4.91 (0%) | 5.79 (+18%) | 6.00 (+22%) |
| logs | 1 | 5.26 | 4.58 (−13%) | 6.32 (+20%) | 5.76 (+10%) |
| logs | 32 | 4.45 | 4.00 (−10%) | 4.87 (+9%) | 4.89 (+10%) |

- The logs gain is the MergeTree writer skipping its sort. With b-sorted-1rg
  at one object per statement, 100% of blocks were already sorted.
- Traces don't get this gain, because their ORDER BY starts with
  (ServiceName, SpanName, …).
- Multi-row-group objects are worse: ClickHouse reads them as 16× more
  smaller chunks.
- 40 databases, 0 consumer errors, steal max 1.26%.

### Reads: one service, one hour, the 32-object set (all in the hour)

**ClickHouse `s3()`** (`reads.py ch`, `ch-events.jsonl`). Rank-10 service,
narrow projection (count and sum). Each value is the total for one query over
the 32-object set: GETs / KB.

| config | default IO (any pruning) | ranged, footer cache off | ranged, footer cache on |
|---|---|---|---|
| traces a-unsorted | 32 / 31,548 | 352 / 5,343 | 320 / 3,246 |
| traces e-range-4rg (minmax) | 32 / 29,574 | 352 / 2,915 | 320 / 818 |
| traces f-range-16rg (minmax) | 32 / 32,998 | 352 / 2,273 | 320 / 176 |
| logs a-unsorted | 32 / 22,631 | 352 / 7,163 | 320 / 5,066 |
| logs e-range-4rg (minmax) | 32 / 20,924 | 352 / 3,437 | 320 / 1,340 |
| logs f-range-16rg (minmax) | 32 / 23,523 | 352 / 2,399 | 320 / 302 |

- **Default IO** (threadpool with prefetch) GETs every object whole. Min/max
  pruning works (`ParquetPrunedRowGroups` = 96 of 128 for e and 417 of 449 for
  f), but it only saves decoding.
- Setting `remote_read_min_bytes_for_seek = 0` alone changes nothing.
- **Ranged IO** is method `read` with seek 0. It makes one GET per column
  chunk, about 10 per object.
- **Hash buckets** (c, d, g, h) prune almost nothing through min/max: 64 of
  512 row groups for d. The `+bloom` variants raised GETs 1.5–2× (the filters
  are fetched) but pruned no extra row group.
- For `SELECT *`, ranged IO costs 74–93 GETs per object. On unsorted
  objects it reads more bytes than the default whole-object GET.

**Direct ranged reads** (`reads.py direct`). This models a reader of our own
exactly from each object's footer: a 64 KiB suffix GET, then the picked row
groups' column chunks. Rank-10 service, narrow projection, per object:

| config | pick | row groups read | GETs (unmerged) | KB, unmerged (1 MiB merge) | unmerged / whole object |
|---|---|---|---|---|---|
| traces a-unsorted | columns only | 1 of 1 | 5 | 180 (986) | 0.18 |
| traces b-sorted-1rg | min/max | 1 of 1 | 5 | 169 (942) | 0.18 |
| traces e-range-4rg | min/max | 1 of 4 | 5 | 94 (308) | 0.10 |
| traces c-hash-4rg | bucket | 1 of 4 | 5 | 82 (202) | 0.08 |
| traces f-range-16rg | min/max | 1 of 14 | 5 | 72 (120) | 0.07 |
| traces d-hash-16rg | bucket | 1 of 16 | 6 | 74 (122) | 0.07 |
| logs a-unsorted | columns only | 1 of 1 | 4 | 226 (419) | 0.32 |
| logs e-range-4rg | min/max | 1 of 4 | 4 | 108 (158) | 0.17 |
| logs f-range-16rg | min/max | 1 of 14 | 4 | 75 (86) | 0.10 |

- object_store's default merge of ranges within 1 MiB reads a ~1 MB object
  nearly whole. A reader that wants the savings has to turn merging down.
- On sorted objects, the 64 KiB suffix read is most of what remains.

## Step 2: routing, N = 1, 3, 8, 16 (full tables: `route.md`)

The model in `route.py` is checked against generated routed objects
(`data/route-*`, 9 owner publishers × 8 batches, encoded with a, b, d and f).

| N | services per object, no routing | with routing (model / generated) | busiest publisher | objects/s fleet, T = 1 s (traces + logs) | rows per object (traces / logs) | fixed insert vCPU, 15 ms/object | at 32 objects/statement |
|---|---|---|---|---|---|---|---|
| 1 | 120 | – | – | 60 + 20 | 10,000 / 10,000 | 1.2 | 0.17 |
| 3 | 120 | 40 / 35–49 | 36% (1.1×) | 60 + 60 | 10,000 / 3,333 | 1.8 | 0.25 |
| 8 | 111–120 | 15 / 12–20 | 36% (2.9×) | 160 + 160 | 3,750 / 1,250 | 4.8 | 0.67 |
| 16 | 95–117 | 7.5 / 4–11 | 26% (4.1×) | 320 + 320 | 1,875 / 625 | 9.6 | 1.34 |

- Routing hardly changes object counts or sizes. Only the busiest publisher
  fills 10k batches sooner.
- The per-object fixed cost therefore grows with N either way. Multi-object
  statements are what contain it.

**One-service, one-hour read in one cluster.** Traces, rank-10 service,
narrow projection, direct ranged path (unmerged). Values are GB/h.

| N | routing | objects/h | unsorted | b-sorted-1rg | d-hash-16rg | f-range-16rg |
|---|---|---|---|---|---|---|
| 3 | none | 10,800 | 1.94 | 1.82 | 0.80 | 0.78 |
| 3 | service | 3,724 | 0.65 | 0.62 | 0.30 | 0.27 |
| 8 | none | 28,800 | 3.12 | 3.00 | 2.03 | 1.96 |
| 8 | service | 3,600 | 0.34 | 0.33 | 0.28 | 0.25 |
| 16 | none | 57,600 | 5.01 | 4.89 | 3.99 | 3.84 |
| 16 | service | 3,600 | 0.31 | 0.31 | 0.28 | 0.25 |

- Logs follow the same pattern: 1.28 → 0.43 GB/h at N = 3 and 3.12 → 0.15 GB/h
  at N = 16 on unsorted objects.
- Without routing, sorting's benefit fades as N grows. Objects get smaller,
  and each one still costs a footer read.
- With routing, the objects are already mostly the one service.

## Step 3: effect on the lake design (lake/DESIGN.md)

- **§2.1, zone maps.** The row said edge sorting costs "about 1 ms per 10k
  rows". It costs 12–16 ms (+20–26% edge CPU). Zone maps inside an edge
  object are also only selective with a range split, and only for readers
  that do ranged IO, which ClickHouse's default path doesn't.
  - The verdict's "sort within the object at the edge" becomes "sorting
    available, off by default".
  - Raw objects live about an hour before the compactor rewrites them sorted
    by ClickStack's key. The compactor, not the edge, is where the sort
    belongs.
- **§2.1, "Search service=X, 1 hour"**. 10.8k candidate objects, about 20k
  GETs. Service routing is the way to cut the candidate set: to about 3.6k
  objects at any N, holding 7–40 services each instead of 120. Its costs are
  publisher load skew (up to 4× the mean at N = 16) and a reader that knows
  the ring.
- **Compaction input.** The compactor's k-way merge could take presorted runs
  if the edge sorted, but at +1.2 vCPU edge-side it is cheaper to sort once
  in the compactor.
- The design doc's zone-map row and verdict were updated to these numbers.

## How it was run, and what went wrong

- **Driver.** `driver.sh` is resumable. Each step writes a marker in
  `state/`, and `PROGRESS.md` is generated.
  - The container restarts about hourly and kills everything. After a
    restart, run `setsid nohup bash driver.sh >> driver.log 2>&1 &` again.
  - The driver starts SeaweedFS and the shared ClickHouse. It also starts a
    private ClickHouse with `query_log` (`chpriv.sh`, :18723), because the
    shared server runs without a config file and has no query_log.
- **Tests.** `tests.log`: otap-rs lib, determinism (including the new sort
  test: every layout is byte-identical on re-encode), otap_view, metrics,
  series, the three MBT suites and the consume binary. All pass. Sorting is
  off by default, so existing outputs are unchanged.
- **Disk.** SeaweedFS keeps deleted objects' space until a vacuum and stops
  accepting writes below 1% free disk (2.56 GB here). In edge-cpu rep 3–4
  that stalled two encbench runs on retries.
  - The stalled runs were dropped and re-run.
  - Stale cargo executables were removed, and `swvac.sh` now vacuums between
    reps.
  - The master's periodic vacuum is disabled during the run, since it had
    fired once inside a stalled run.
  - encbench now has a 300 s timeout, and the driver's disk floor is 2.7 GB.
- **Discarded data.** A first central and ClickHouse-read pass ran against
  the shared server's missing query_log. It was discarded and re-run on the
  private server. `state/central-env.shared-noquerylog.jsonl` keeps its env
  log.
- **What these numbers don't cover.**
  - Objects smaller than 10k rows at the edge. Row-group overhead per row
    grows there, and the N = 8/16 read rows are scaled from 10k-row objects.
  - S3 latency. Everything ran against local SeaweedFS.
  - Why bloom pushdown doesn't prune. It was not investigated; `ServiceName`
    is written as BYTE_ARRAY without a String annotation.

## Files

- **Scripts.**
  - `driver.sh` runs everything; `progress.py` writes `PROGRESS.md`.
  - `prep.sh` builds the data; `gen/` is mixgen.
  - `edge.sh`, `central.sh` and `reads.py` do the measurements.
  - `summarize.py` writes `results.md`; `route.py` writes `route.md`.
  - `chpriv.sh` is the private ClickHouse; `swvac.sh` handles SeaweedFS
    vacuum.
- **Raw results.**
  - Edge: `edge.jsonl`, `edge-env.jsonl`, `edge.log.gz`.
  - Central: `raw/` (per-statement query_log, tables, consumer output),
    `central-env.jsonl`.
  - Reads: `direct.jsonl`, `ch.jsonl`, `ch-events.jsonl`.
  - Routing: `data/` (mixgen stats, ring assignments, routed-set encbench
    output).
  - Query setup: `services.json`, `buckets.json`.
- **Step 0:** `bisect/`.
- **Code** in `otap-rs` (uncommitted):
  - `src/encode.rs`: `SortOptions`, `sort_plan` and `parquet_groups`.
  - `src/batch.rs`: `sort_batch` and the sorted encode path.
  - `src/bin/encbench.rs`: `--sort`, `--row-groups`, `--split` and
    `--bloom-add`.
  - `tests/determinism.rs`: the sort determinism test.

> In git, `direct.jsonl`, `ch.jsonl` and `ch-events.jsonl` are stored gzipped to keep the directory small; `gunzip -k *.jsonl.gz` before rerunning `summarize.py`, `route.py` or `reads.py`.
