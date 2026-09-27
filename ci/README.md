# CI

Two GitHub Actions workflows, and the scripts they call, which run the same
way on a workstation.

## What runs when

**`ci.yml`: every push and pull request** (and on demand). Three parallel
jobs; the target is under 15 minutes with warm caches (the first run, which
compiles otel-arrow and its dependencies from nothing, takes longer).

| job | runs |
|---|---|
| `go-vet` | `go vet ./...` in every Go module (`ci/go-modules.sh vet all`) |
| `go-test` | SeaweedFS + ClickHouse, quint; `go test -race ./...` in the fast modules (`ci/go-modules.sh test fast`) |
| `rust` | otap-rs: pinned upstream checkout; `ci/clippy.sh` (`-D warnings` with an allow-list); `cargo test --release --lib --bins` (the consumer's ClickHouse/S3 tests against the services); the otlpgen datasets; `--test determinism otap_view metrics series` |

**`nightly.yml`: 03:17 UTC daily and on demand** (`workflow_dispatch`, with
the soak's length as an input). `build` runs first; the others run beside it
or after it.

| job | runs |
|---|---|
| `build` | `cargo build --release` (otap-s3pq, consume), the Go tools, both collectors with ocb (`ci/build-collectors.sh`: otelcol-s3pq, otelcol-chdb); one `bin` artifact |
| `conformance` | `conformance/run.sh` for layout B and for the ClickStack metrics tables, then `conformance/go_faults.sh` |
| `faults-soak` | `otap-rs/scripts/faults.sh`, then `otap-rs/scripts/consumer_soak.sh` for 300 s |
| `rust-integration` | `tests/series.rs` with the Go prototype's fleet objects, `tests/creds.rs` against credstubs |
| `rust-mbt` (x3) | the quint-connect model-based tests, one runner per test binary (`mbt_s3inline`, `mbt_s3inline_metrics`, `mbt_s3inline_consumer`), `--test-threads=1`, `QUINT_SEED=0x5eed` |
| `model` | `otap-rs/scripts/consumer_model.sh` through `ci/model-check.sh`; `parquetgo/modelcheck` |
| `chdb` | libchdb (the release `chdb-go/update_libchdb.sh` pins, cached); `go test` in chdb-go, chdbexporter (with `PBT_QUINT=1`) and parquetgo/compare |

Every job that touches the services uploads their logs on failure; the
end-to-end jobs always upload their output directories (summaries, edge,
proxy and consumer logs).

## Scripts

| script | does |
|---|---|
| `go-modules.sh list\|vet\|test GROUP` | finds the Go modules (not `chdb-go/lib/*`, which only embed a libchdb blob) and runs go in each; groups `all`, `fast`, `chdb`, `slow`, or one module's path. `RACE=1` adds `-race`; `LOG_DIR` keeps a log per module; per-module skips are listed in the script with their reason |
| `clippy.sh` | `cargo clippy --release --all-targets -D warnings`, allowing the 16 lint kinds the crate trips today (a new kind fails; the list can only shrink) |
| `services.sh start\|stop\|logs` | SeaweedFS (`chrislusf/seaweedfs:4.47`, S3 :18333, otel/otelsecret, `-volume.max=64 -master.volumeSizeLimitMB=1024`) and ClickHouse (`clickhouse/clickhouse-server:26.9`, HTTP :18123, TCP :19000) in Docker on the host network, since ClickHouse reads SeaweedFS through `s3()`; creates the buckets |
| `test.env` | every endpoint and key the tests read; appended to `$GITHUB_ENV` |
| `gen-data.sh DATA [BIN]` | builds the Go tools and writes otlpgen's datasets; `VARIANTS`, `CONFORMANCE`, `SERIES`, `FLEET` add the rest |
| `build-collectors.sh BIN [s3pq\|chdb\|all]` | ocb v0.161.0 builds of the two collectors |
| `verdict.sh SUMMARY [MIN]` | fails on any `FAIL` line in an end-to-end summary, or fewer than MIN `PASS` lines (the scripts themselves exit 0) |
| `model-check.sh` | runs `consumer_model.sh`, fails if a design invariant is violated or a `quint test` group fails; a mutant that random simulation misses (its scripted `*BreaksTest` pins it) or a witness it does not reach is a warning |

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

- **Quint.** 0.32.0 from npm. The Rust evaluator is downloaded by quint on
  first use and cached (`~/.quint`). The Go model tests run with
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
  10 GB, hence one runner per binary and one test at a time.
- **Caches.** Cargo (registry and `target/`) is keyed on the toolchain,
  `Cargo.lock`, `UPSTREAM` and the patches; the upstream checkout on
  `UPSTREAM` and the patches; Go per job on the `go.sum` files; libchdb on
  `update_libchdb.sh`, which holds the pin.
