# CI

Two GitHub Actions workflows, and the scripts they call, which run the same
way on a workstation.

## What runs when

**`ci.yml`: every push and pull request** (and on demand). Three parallel
jobs; the target is under 15 minutes with warm caches (the first run, which
compiles otel-arrow and its dependencies from nothing, takes longer).

| job | runs |
|---|---|
| `go-vet` | `go vet ./...` in every Go module (`ci/go-modules.sh vet all`); `ci/iam-lint.sh` (the `deploy/iam/` policies, and the IAM compiled from the Cedar grants, `otel-chdb/grants/examples/out/iam`, D38) |
| `go-test` | SeaweedFS + ClickHouse, quint; `go test -race ./...` in the fast modules (`ci/go-modules.sh test fast`) |
| `lakeui` | `otel-chdb/lakeui`: `npm ci`, `npm run vendor:check` (vendor/ matches the pinned packages), `npm test` (node:test + fast-check: the planner client, range reader, completeness math, re-plan state machine, queries over edge Parquet fixtures, SVG) |
| `lakeui-mosaic` | `otel-chdb/lakeui/mosaic` (the Mosaic spike, research/mosaic.md): `npm ci` in `lakeui` and the spike, `npm test` (the columns loaded from edge Parquet fixtures = brute force with lakeui's completeness states; Arrow IPC into DuckDB-WASM in Node keeps every value; property: the completeness band starts at lakeui's first unsettled bucket) |
| `rust` | otap-rs: pinned upstream checkout; `ci/clippy.sh` (`-D warnings` with an allow-list); `cargo test --release --lib --bins` (the consumer's ClickHouse/S3 tests against the services); the otlpgen datasets; `--test determinism otap_view metrics series`; the deterministic simulation tests `--test dst_consumer dst_net` at their fixed seeds (`otap-rs/DST.md`); the Hegel property and stateful tests `--test hegel_props hegel_dst` under `hegel.toml`'s `ci` profile (100 derandomized cases each, `HEGEL_CH=1`; `otap-rs/HEGEL.md`) |

| `traceability` | after the others, even when one failed: joins their trace records with the STPA catalogue (`ci/trace/trace.py check`; "Traceability" below) |

**`nightly.yml`: 03:17 UTC daily and on demand** (`workflow_dispatch`, with
the soak's length and the jobs to run as inputs). `plan` decides which jobs
run; `build` runs next; the others run beside it or after it.

### Running part of the nightly

`workflow_dispatch` takes `jobs`: `all` (the default, and what the schedule
always runs) or a comma list of job names from the table below, on any ref:
`rust-mbt` means its three matrix entries, which can also be named one by one
(`mbt_s3inline`, `mbt_s3inline_metrics`, `mbt_s3inline_consumer`), and
`close_e2e` runs only the orderly-close step of `conformance` (with the
datasets it needs). A job that downloads the `bin` artifact (`conformance`,
`close_e2e`, `query-integration`, `lakeui-e2e`, `lakeui-mosaic-e2e`,
`journeys`, `alerts-integration`, `faults-soak`) brings `build` with it. `plan` fails on
an unknown name rather than run nothing. From the API:

```sh
curl -X POST -H "Authorization: Bearer $GH_TOKEN" \
  https://api.github.com/repos/casselc/observability/actions/workflows/nightly.yml/dispatches \
  -d '{"ref":"claude/brave-pascal-0fecgh","inputs":{"jobs":"kani,dst,mbt_s3inline"}}'
```

| job | runs |
|---|---|
| `plan` | turns `inputs.jobs` into the per-job flags and the `rust-mbt` matrix the other jobs' `if:` read |
| `build` | `cargo build --release` (otap-s3pq, consume), the Go tools, both collectors with ocb (`ci/build-collectors.sh`: otelcol-s3pq, otelcol-chdb); one `bin` artifact |
| `conformance` | `conformance/run.sh` for layout B and for the ClickStack metrics tables (both edges' orderly closes compared), then `conformance/go_faults.sh`, then `otap-rs/scripts/close_e2e.sh` (D35: the Rust edge, the Rust edge with Quiver and a restart on the same buffer, the Go edge; each lane ends with its close, the consumer retires it, nothing quarantined) |
| `query-integration` | `otel-chdb/query/integration`: the query service against the Go edge (two clusters) → SeaweedFS → the Rust consumer → ClickHouse, including late rows published through the edge after their window closed (CAST row 26: partial until `complete_through` ≥ end + `max_lateness`, rows past it counted), and the basis (D30: an answer at a basis unchanged after new data and a late row into the same window, a newer basis with it, the delta exactly that row, the plan at the basis listing the same objects, a fleet basis refused to a one-cluster token), with `QS_IT_BIN` pointing at the `bin` artifact (the unit and property tests run in `go-test`, where the integration test skips) |
| `hdxadapter-integration` | `otel-chdb/query/integration/hdxadapter`: HyperDX 2.39.1's 799 captured statements through the HyperDX adapter and the query service onto ClickHouse (three schema sides, the same synthetic rows), each answer compared with ClickHouse's own; a one-cluster token against the scope filter applied by hand; completeness labels against the derived windows; `@clickhouse/client` (HyperDX's pinned version, npm-installed into the runner's temp) driving the adapter; since D33 the entity rewrite proxy's 82 statements through adapter and service (before/after, fleet and scoped tokens, the dictionary leak probe), a labelled sample, and the key/value rollup migration (`TestKVRollupMigration`). ClickHouse only (`ci/services.sh`); no `bin` artifact |
| `query-kms-emulator` | `ci/kms-emulator.sh` with moto server 5.2.3 (pipx): the query service's KMS basis signer (DECISIONS D30 amendment 2026-09-28) against moto's KMS: the startup check refuses a symmetric key, two replicas built like the service mint and verify each other's tokens, tampering is `basis_invalid`, a verify-only key's tokens verify, mint and verify latency logged. No services, no `bin` artifact |
| `lakeui-e2e` | `otel-chdb/lakeui/e2e`: Playwright (Chromium) on the lake UI against the query service's `/v1/plan` (`query/integration/lakeuirig`: the Go edge, clusters `lui-a`/`lui-b` → SeaweedFS → the Rust consumer → ClickHouse, a code + PKCE front on the test issuer, a counting pass-through in front of SeaweedFS); results equal ClickHouse counts, refusals render as refused, a real expired-URL 403 re-plans, bytes per object; `test-results/` (numbers, screenshots) as the `lakeui-e2e` artifact |
| `lakeui-mosaic-e2e` | `otel-chdb/lakeui/mosaic/e2e`: Playwright on the Mosaic spike, same rig with `LUI_RICH_SPANS=1`; `npm run vendor` first (esbuild bundle, DuckDB-WASM files, the parquet extension checked by SHA-256): range reads vs DuckDB reading the URLs (same tables, bytes by the tap), expired URLs re-plan, the HEAD-shim mode fails visibly, cross-filter totals equal SQL counts, brush latency with/without pre-aggregation up to 11 M rows, stale cubes, unknown watermark, Mosaic's statements on ClickHouse; `test-results/` as the `lakeui-mosaic-e2e` artifact |
| `journeys` | `otel-chdb/lakeui/e2e/journeys`: the six user journeys of [`otel-chdb/docs/journeys`](../otel-chdb/docs/journeys/README.md) (is this data complete, find a trace, not your cluster, links expire, late data through the lake UI; cross-filter through the Mosaic spike, whose `npm run vendor` it runs) on one rig with `LUI_RICH_SPANS`, one Playwright test each, asserting at every step (ClickHouse counts, the pass-through's bytes and 403s, refusals, the basis and its tail); `e2e/journeys/render.sh` takes a picture per step and a GIF per journey (ffmpeg); the pictures are the `journeys` artifact; then one `trace.py record` (IT) per journey, as `trace-journeys`. Fails when a journey's assertion does. The committed pictures are refreshed by running `render.sh` locally, never edited |
| `alerts-integration` | `otel-chdb/alerts/integration`: the alert evaluator (two replicas, state on S3) through the query service (built from `otel-chdb/query`) against the Go edge (clusters `aa`, `ab`) → SeaweedFS → the Rust consumer → ClickHouse, with a fake Alertmanager; a stalled lane pages "cannot evaluate", then the missed windows are evaluated; rows sent into windows already evaluated are found by the late check (D30) and page per `on_late` (a late episode; "late data changed window W"). `ALR_IT_BIN` points at the `bin` artifact; the unit, property and simulation tests run in `go-test` |
| `faults-soak` | `otap-rs/scripts/faults.sh`, then `otap-rs/scripts/consumer_soak.sh` for 300 s |
| `rust-integration` | `tests/series.rs` with the Go prototype's fleet objects, `tests/creds.rs` against credstubs |
| `dst` | the deterministic simulation (`otap-rs/DST.md`): 10,000 new level-1 seeds and 200 new turmoil seeds a night (base = run number × 100,000), the meta tests with 20 seeds, the emulators against the services; failing seeds' traces as the `dst-traces` artifact |
| `hegel` | Hegel (`otap-rs/HEGEL.md`) under `hegel.toml`'s `nightly` profile (`HEGEL_DEFAULT_PROFILE=nightly`: 3,000 cases per test, a fresh seed): the properties, the stateful and swarm machine over the consumer fleet, the eight planted bugs it must find and shrink (`HEGEL_DST_MUTANTS=1`, up to 20,000 cases each), and the concurrent machine (thread races on conditional writes) against the S3 emulator, its racy-store mutant, and SeaweedFS; the database of failing examples is kept in the Actions cache (replayed first the next night) and uploaded as `hegel-db` |
| `rust-mbt` (x3) | the quint-connect model-based tests, one runner per test binary (`mbt_s3inline`, `mbt_s3inline_metrics`, `mbt_s3inline_consumer`), `--test-threads=1`, `QUINT_SEED=0x5eed` |
| `model` | `otap-rs/scripts/consumer_model.sh` through `ci/model-check.sh`; `otel-chdb/model/alert_model.sh` (the alert evaluator's model: design, witnesses, mutants; late data since D30: `noDoubleCount`, `lateNeverResolves`, mutants `lateDouble`, `lateResolves`); `otel-chdb/model/bitemp_model.sh` (the catalog as bitemporal events, D32: design, witnesses, mutants, scripted runs, with a random seed), then `entities/bitemp`'s model test on fresh traces of it; `otel-chdb/model/retirement_model.sh` (dead-lane retirement, FORMAT.md §3.1: design, witnesses, six mutants, the operator's mistake, scripted runs including `closeUnsealed`'s, with a random seed); `parquetgo/modelcheck` |
| `kani` | Kani 0.68.0 (cached); `cargo kani` in `otap-rs/verify`: the proof harnesses for the consumer's lease window and check range (`otap-rs/VERIFY.md`), ~20 min |
| `chdb` | libchdb (the release `chdb-go/update_libchdb.sh` pins, cached); `go test` in chdb-go, chdbexporter (with `PBT_QUINT=1`) and parquetgo/compare |
| `upstream-patches` | our otel-arrow patches' own upstream unit tests in the upstream workspace at the pinned commit, with its toolchain: `cargo test --release -p otel-arrow-dfe-otap --lib custody` and `-p otel-arrow-dfe-core-nodes --features durable-buffer durable_buffer` (patch 0006, Quiver's custody floor); too large for a local disk |

Every job that touches the services uploads their logs on failure; the
end-to-end jobs always upload their output directories (summaries, edge,
proxy and consumer logs).

## Service-gated tests fail where the services are (CAST row 43)

A test that needs ClickHouse, S3 or another service skips without it, so the
suites run on a laptop; but a skip reads as a pass, and two tests skipped
silently for lack of a bucket while broken underneath. So every such gate goes
through one helper per language, and fails instead of skipping where the run
says the service exists:

- Rust: `otap_s3pq::testgate::skip(service, why)` (`otel-chdb/otap-rs/src/testgate.rs`);
  the gate `return`s it.
- Go: `testgate.Skip(t, service, format, args...)` (module `otel-chdb/testgate`,
  no dependencies; a module using it, or building one that does, carries
  `replace .../otel-chdb/testgate => <path>`, as do the two ocb builder configs).
- `OSCOPE_REQUIRE_SERVICES`: unset, empty or `0` nothing is required; `1`,
  `all` or `*` everything; else names separated by commas or spaces. Names:
  `clickhouse`, `s3`, `kms`, `credstubs`, `clickhouse-replicated`.
- `ci/services.sh start` writes `OSCOPE_REQUIRE_SERVICES=clickhouse,s3` to
  `$GITHUB_ENV` (`REQUIRE_SERVICES` overrides), so every job that starts the
  services requires them; `kms-emulator.sh` adds `kms`, and the creds step of
  `rust-integration` adds `credstubs`.
- Opt-in gates are not service gates and still skip: a binaries directory
  (`QS_IT_BIN`, `ALR_IT_BIN`, `FAULTPROXY2`), a dataset (`OTAPRS_DATA`), libchdb
  (`CHDB_LIB_PATH`), a slow or measuring test (`HDXA_IT`, `HEGEL_*`,
  `PBT_*`, `*_MEASURE*`, `OTAP_CENTRAL_BENCH`), and the replicated central
  (`clickhouse-replicated`, `central-replicated/`), which no job starts.

Locally, with the services up: `set -a; . ci/test.env; set +a; export
OSCOPE_REQUIRE_SERVICES=clickhouse,s3` before the commands below.

## Traceability (runtime evidence, not static tags)

A test tagged as verifying a hazard, requirement or CAST row counts only if it
**ran and passed at this commit**: skipped and unrun tests are unknowns (STPA.md
CAST rows 41, 43, 45, 46, 53). The tag is a call that runs with the test and,
when `OSCOPE_TRACE_OUT` is set (both workflows set it to
`$GITHUB_WORKSPACE/_trace/records.jsonl`), appends one JSON record with the IDs,
the technique (a VERIFICATION.md §1 code), the test, file, commit, job and
outcome (`passed`, `failed`, `skipped`):

| language | tag, first in the test | outcome from |
|---|---|---|
| Go | `tracetag.Covers(t, "P", "CAST-52", "H-6")` (`otel-chdb/testgate/tracetag`, in the testgate module: no new `replace`) | `t.Cleanup`: `t.Failed()`, `t.Skipped()` |
| Rust | `let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-42", "LS-5"]);` (`crate::` inside the library) | the guard's drop: `thread::panicking()`; `testgate::skip` (or `oscope_trace::skipped()` before an opt-in gate's return) marks it skipped |
| node:test | `test('…', (t) => { covers(t, 'P2C', 'CAST-26')` (`lakeui/test/trace.js`) | `lakeui/test/trace-reporter.js`, a `--test-reporter` in `npm test` |
| Quint scripts | an entry in `ci/trace/models.txt` (a regex over the script's report) | `trace.py model --script … --report …` after the script |
| a CI step that is evidence (a lint) | `python3 ci/trace/trace.py record --test ci/clippy.sh --technique G --ids CAST-8,H-1` after it | the step ran (it runs only if the check passed) |

Go tests must run with `-count=1` (a cached result runs nothing and records
nothing). Every test job uploads `_trace` as `trace-<job>`; the `traceability`
job (in `ci.yml`, and last in the nightly for the jobs `plan` selected)
downloads them and runs `ci/trace/trace.py check`, which **fails** when

- an ID in a tag or record is not in the catalogue (`trace.py catalog`: every
  L-, H-, SC-, UCA-, LS-, SEC-, TM-, R- ID in STPA.md and the STPA sections of
  research/{langfuse,entra-ingress,grants}.md, and CAST-n from the CAST tables),
  or a technique is not a §1 code (the catalogue's source sits behind one
  interface, `CATALOG_SOURCES`: `OSCOPE_STPA_SOURCE=records` reads the structured
  STPA records under `otel-chdb/stpa/` once they replace the tables; IDs do not change);
  the catalogue itself fails on an ID defined
  twice with different meanings unless `ci/trace/same-meaning.txt` says why they agree;
- a record's outcome is `failed`, or its commit is not the run's;
- a tag in source (listed statically by `trace.py scan`) whose jobs, from
  `ci/trace/scope.txt`, include one this run ran has no **passing** record from
  it: tagged but skipped or unrun;
- a CAST row, or a required (hazard × technique) cell of VERIFICATION.md §3, has
  no passing record, unless `ci/trace/known-gaps.txt` lists it with an owner and a
  reason. Items claimed only by tags of jobs this run did not run are reported as
  *deferred*, not judged (ci.yml defers the model, chdb and conformance evidence to
  the nightly).

The report (hazard → requirements → techniques → tests with outcomes; CAST rows;
every tag; coverage numbers) is the job summary and the `traceability-report`
artifact, and is printed in the log between `BEGIN/END TRACEABILITY REPORT`
markers. `otel-chdb/TRACEABILITY.md` is a snapshot of a green run, written by
`python3 ci/trace/trace.py snapshot <report.md or the job's log>`, never by hand.

Locally: `OSCOPE_TRACE_OUT=/tmp/t.jsonl go test -count=1 ./...` (or `cargo test`,
`npm test`), then `python3 ci/trace/trace.py check --workflow ci --jobs go-test
--records /tmp/t.jsonl --report /tmp/TRACEABILITY.md` (records without a job
stand for any job).

## Scripts

| script | does |
|---|---|
| `go-modules.sh list\|vet\|test GROUP` | finds the Go modules (not `chdb-go/lib/*`, which only embed a libchdb blob) and runs go in each; groups `all`, `fast`, `chdb`, `slow`, or one module's path. `RACE=1` adds `-race`; `LOG_DIR` keeps a log per module; per-module skips are listed in the script with their reason |
| `iam-lint.sh [DIR]` | every `otel-chdb/deploy/iam/*.json` parses, and a role granting `s3:ListBucket` under an `s3:prefix` condition also grants it on the same prefixes under `StringLikeIfExists` (or unconditioned): a HEAD carries no prefix, and S3 answers a missing key 403 without `ListBucket` (DECISIONS D18 amendment, 2026-09-28). Fails on the pre-amendment policies. KMS grants (the query service's basis key, `query-basis-kms*.json`, D30 amendment): one key ARN per resource, named actions, and no key policy letting `*` or the account root call `GenerateMac`/`VerifyMac`/`CreateGrant` |
| `kms-emulator.sh [PORT]` | starts `moto_server` (on PATH: `pip install 'moto[server]'`) and runs `TestKMSEmulator` (`otel-chdb/query/internal/app`) against it; nightly `query-kms-emulator` |
| `clippy.sh` | `cargo clippy --release --all-targets -D warnings`, allowing the 16 lint kinds the crate trips today (a new kind fails; the list can only shrink) |
| `services.sh start\|stop\|logs` | SeaweedFS (`chrislusf/seaweedfs:4.47`, S3 :18333, otel/otelsecret, `-volume.max=64 -master.volumeSizeLimitMB=1024`) and ClickHouse (`clickhouse/clickhouse-server:26.9`, HTTP :18123, TCP :19000) in Docker on the host network, since ClickHouse reads SeaweedFS through `s3()`; creates the buckets; under Actions writes `OSCOPE_REQUIRE_SERVICES=clickhouse,s3` to `$GITHUB_ENV` (see above) |
| `test.env` | every endpoint and key the tests read; appended to `$GITHUB_ENV` |
| `gen-data.sh DATA [BIN]` | builds the Go tools and writes otlpgen's datasets; `VARIANTS`, `CONFORMANCE`, `SERIES`, `FLEET` add the rest |
| `build-collectors.sh BIN [s3pq\|chdb\|all]` | ocb v0.161.0 builds of the two collectors |
| `verdict.sh SUMMARY [MIN]` | fails on any `FAIL` line in an end-to-end summary, or fewer than MIN `PASS` lines (the scripts themselves exit 0) |
| `model-check.sh` | runs `consumer_model.sh`, fails if a design invariant is violated or a `quint test` group fails; a mutant that random simulation misses (its scripted `*BreaksTest` pins it) or a witness it does not reach is a warning |
| `install-quint.sh` | `npm install -g` quint `$QUINT_VERSION` and the Rust evaluator it expects into `$QUINT_HOME` (`~/.quint`), from the release download URL rather than the rate-limited API |

## Running it locally

With the services already up (or `ci/services.sh start` where Docker is):

```sh
set -a; . ci/test.env; set +a
ci/go-modules.sh vet all
RACE=1 ci/go-modules.sh test fast

cd otel-chdb/otap-rs
scripts/fetch-upstream.sh                        # pinned otel-arrow + patches -> .upstream
../../ci/clippy.sh
cargo test --release --locked --lib --bins
SERIES=1 ../../ci/gen-data.sh /tmp/otaprs-data
OTAPRS_DATA=/tmp/otaprs-data OTAPRS_SERIES_GO=/tmp/otaprs-data/series/go \
  cargo test --release --locked --test determinism --test otap_view --test metrics --test series
HEGEL_CH=1 cargo test --release --locked --test hegel_props --test hegel_dst   # HEGEL_DEFAULT_PROFILE=nightly for the nightly budget
for t in mbt_s3inline mbt_s3inline_metrics mbt_s3inline_consumer; do   # one at a time: 2.5-4 GB each
  QUINT_SEED=0x5eed cargo test --release --locked --test $t -- --test-threads=1
done
```

The nightly jobs are the `run:` blocks of `nightly.yml` with `B`, `D`, `OUT`
pointed at local directories (`B` holds the release binaries, the Go tools
and `ci/build-collectors.sh`'s otelcol-s3pq; `D` is `VARIANTS=12
CONFORMANCE=1 ci/gen-data.sh`'s output); `CHDB_LIB_PATH=/path/to/libchdb.so
PBT_QUINT=1 ci/go-modules.sh test chdb` for the libchdb tests.

## Notes

- **Quint.** 0.32.0 from npm, through `ci/install-quint.sh`, which also
  puts the Rust evaluator that version expects in `~/.quint` (cached) from
  the release's download URL. Left to itself quint fetches it through the
  GitHub API unauthenticated, which the runners' shared rate limit turns
  into "Failed to fetch from GitHub: Forbidden". The Go model tests run with
  `QUINTGO_BACKEND=typescript` (`test.env`), the backend the quintgo README
  documents and its results were produced with: with the Rust evaluator,
  `quintgo/examples/edgepublish/mbt` fails at seed 42 (its trace 0 reaches
  a `pushStart` the publisher does not act on within 2 s).
- **ClickHouse.** There is no 26.10 image on Docker Hub yet; 26.9 is the
  closest. Set `CH_IMAGE` to change it.
- **rustfmt** is not checked: the crate is not `cargo fmt`-clean today.
- **s3cas** runs against SeaweedFS without `TestMultipartCreateRace` and
  `TestMultipartCASRace`: SeaweedFS's conditional
  `CompleteMultipartUpload` is not atomic (`UPSTREAM_ISSUES.md` U4), so
  those probes fail there by design. The job log shows the skip.
- **Leftovers.** `consumer_soak.sh` keeps its database
  (`otaprs_consumer_<run>`), and the end-to-end scripts leave their S3
  objects; harmless on a runner, worth cleaning after a local run.
- **Memory.** A quint run under quint-connect takes 2.5-4 GB (node plus the
  Rust evaluator); the three mbt binaries' tests in parallel peaked near
  10 GB, hence one runner per binary and one test at a time. Node's heap
  alone reaches 3.5 GB (`s3inline_metrics_design_simulation`, 300 traces)
  to 4.3 GB (the 1000-trace consumer instances), above its default limit
  on the 8 GB runners (~2 GB), so `nightly.yml` sets
  `NODE_OPTIONS=--max-old-space-size=5120`. Without it quint dies with
  "JavaScript heap out of memory", which quint-connect reports only as
  "Quint returned non-zero code." (it drops quint's stderr).
- **Hegel.** `hegel.toml` (in `otel-chdb/otap-rs`, where cargo runs the
  tests) holds the profiles; the shipped `ci` profile is selected on CI
  automatically. The concurrent machine (`tests/hegel_race.rs`) is
  nondeterministic (the OS schedules its threads; Hegel neither shrinks nor
  replays it), so it runs nightly only. The stateful machine's case is a
  deterministic simulation, so its failures shrink and replay; a failure
  prints a `#[hegel::reproduce_failure("…")]` line.
- **Caches.** Cargo (registry and `target/`) is keyed on the toolchain,
  `Cargo.lock`, `UPSTREAM` and the patches; the upstream checkout on
  `UPSTREAM` and the patches; Go per job on the `go.sum` files; libchdb on
  `update_libchdb.sh`, which holds the pin; Hegel's database per nightly run,
  restored from the newest.
