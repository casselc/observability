# Property-based tests for chdbexporter (hegel-go)

[hegel-go](https://github.com/hegeldev/hegel-go) v0.9.8 (libhegel 0.43.4, the
Hypothesis-derived Rust engine). It runs in process: hegel-go `go:embed`s a
prebuilt `libhegel.so` and loads it with purego, so there is no server, no
Python and no cgo. It needs Go 1.26 (the module already uses it).

The hegel tests carry a `pbt` build tag and use their own module file,
`go.pbt.mod`, in the same way `go.stock.mod` works for stock chdb-go:

```sh
cd chdbexporter
P="-tags pbt -modfile=go.pbt.mod"
go test $P -run 'PBT|Finding' ./...          # the default properties, ~15 s
PBT_SCALE=10 go test $P -run PBT ./...       # 10x the examples
HEGEL_STATISTICS=1 go test $P -run PBT -v .  # per-property event statistics (swarm coverage)
PBT_FINDINGS=1 go test $P -run PBTFinding -v .   # state-machine findings: find and shrink, 5-90 s each
PBT_QUINT=1 go test $P -run 'Model|Conforms|Finding' -v .   # + quint / quintgo (needs quint 0.32)
```

The deterministic reproductions in `publish_findings_test.go`, and the fake
session they use, need no hegel, so they also run in the plain `go test ./...`.

Failing examples persist in `$HEGEL_DB` (default `$TMPDIR/chdbexporter-hegel`,
not the repo) and are replayed first on the next run.

**Why a separate module file.** Every hegel-go release requires purego
`v0.11.0-alpha.6…`. With hegel in `go.mod`, Go's minimal version selection
would move chdb-go's purego from v0.8.2 to that alpha in every build of the
exporter, including the `otelcol` collector and `bench/`. All tests pass on
the alpha (both builds, `-race`), but pulling a production collector onto an
alpha of the library that loads libchdb, just to run tests, is the wrong
trade. `go.pbt.mod` keeps the alpha in test builds only.

## What is tested

| Test | Property | Examples / time |
| --- | --- | --- |
| `TestPBTValueStringMatchesAsString`, `…Doubles` | `valueString` == `pcommon.Value.AsString` for nested values and every float class | 2000 + 3000, 0.2 s |
| `TestPBTEncodersMatchPdata` | For arbitrary traces/logs (NULs, control chars, invalid UTF-8, all value types, zero ids, any uint64 time), with and without envelope, RowBinary decodes with exactly `structure()` and equals an oracle built from pdata; JSONEachRow parses (the way ClickHouse parses) to the same rows, keyed by `columns()` | 1000, 0.7 s |
| `TestPBTFormatsRoundTripThroughChDB` | Same inputs pushed through rowbinary, json, file and rowbinary-without-staging into chDB; `SELECT * FORMAT RowBinary` equals what the encoder wrote, and the trace-id view holds each distinct id | 25, 11 s |
| `TestPBTSchemaConsistent` | For any names, TTL, Buffer, staging, signal, envelope, generation: insert columns = `columns()` = `structure()`; the DDL declares them; every stored/local/target object is created; views drop before their tables; the disk func sees exactly the stored tables; `ReaderDDL` matches | 500, 0.1 s |
| `TestPBTConfigValidateMatchesRules` | `Validate` accepts exactly what an independent restatement of the documented rules accepts, and names every broken rule | 2000, 0.5 s |
| `TestPBTPublisherStateMachine` | Stateful test of the real `publisher` over fake sessions (`fakesession_test.go`): pushes, retries, failing and ambiguous faults in every statement, clock ticks and rotations, retention sweeps, shutdowns, crashes. After every step: commit ⇒ data, success ⇒ commit, batch ids unique, seals count successful pushes, only failed pushes leave rows, nothing dropped from S3 or written after its seal, seals/detaches happen unless their own statement failed | 300 × ≤30 steps, 2.5 s |

Swarm testing is automatic in hegel's state machines: each rule is switched
off for whole test cases, so runs with only manifest faults, or no rotations,
occur (e.g. manifest faults in 39% of cases, crashes 40%, rotations 67%).

## Findings

Rediscovered from the Quint model, against the real Go code (shrunk):

- **F1 duplicate commit** (`TestPBTFindingDuplicateCommit`): push p1 with an
  ambiguous manifest write; retry p1 → two manifests for one request.
- **F2 orphan rows** (`TestPBTFindingOrphanRows`): one push whose Parquet (or
  manifest) write fails → table rows with no manifest.
- **F3 seal undercount** (`TestPBTFindingSealUndercount`, and on real S3
  `TestFindingSealUndercountOnS3`): ambiguous manifest write; shutdown →
  `_sealed.json` says 0 batches, 1 manifest exists.

New, outside what the model can express:

1. **Restart reuses the previous epoch's tables** (persistent `path`, as in
   `config.edge.yaml`). Table names carry the generation, not the epoch, and
   are created `IF NOT EXISTS`, so an incarnation restarted within the same
   generation inserts into its predecessor's table on the predecessor's
   endpoint, after that generation was sealed, while its manifests name its
   own endpoint, which is empty. Shrunk: push; crash; push. Confirmed on
   chDB + SeaweedFS (`TestFindingRestartWritesIntoPreviousEpochTable`: new
   manifest's endpoint has 0 objects; the old table holds both epochs' rows).
   The old incarnation's generations are also never detached.
2. **Generation computed from a clock read taken before the lock.** A clock
   step back reopens a sealed generation (shrunk: push; +1 gen; push; −1 gen;
   push). Without any clock step, a push that read the time just before a
   boundary and takes the lock after one that read it just after does the same
   (`TestFindingStaleClockReadReopensGeneration`), and `acquire`'s recursive
   re-read then rotates forward again: the old generation is re-sealed from an
   empty struct (`_sealed.json` overwritten with 0 batches) and the current
   one is sealed while current, which can drop its staging table under live
   pushes. On S3 (`TestFindingClockRegressionOnS3`) the final seal of g10 says
   1 batch [3..3] while manifests 1 and 3 exist.
3. **Failed seal / DETACH is never retried** (`TestPBTFindingFailedSealIsNotRetried`):
   the generation stays unsealed (or attached) in a live epoch.
4. **insert_format json rejects early timestamps**
   (`TestPBTFindingJSONRejectsEarlyTimestamps`): any time before 1973-03-03
   (1e8 s), e.g. a log with Timestamp and ObservedTimestamp both 0, fails the
   whole batch (`CANNOT_PARSE_DATETIME`); past 2262 it fails too. Shrunk: one
   log record at time 0.
5. **Manifest event-time range** (`TestPBTFindingManifestEventTimeRange`):
   `envelope.write` treats `minTS == 0` as unset; rows at [0, 1] give
   `min_event_time` 1. Times ≥ 2^63 ns wrap to 1677 (`…Overflow`).
6. **TTL below a second** (`TestPBTFindingSubSecondTTL`): accepted by
   `Validate`, rendered `toIntervalSecond(0)` (1ns ⇒ immediate expiry); 1500ms
   ⇒ 1 s.
7. **Parquet-only generation** (`TestPBTFindingParquetGenerationUnchecked`):
   `generation: 1ns` passes `Validate` without object storage, although
   `generation()` still rotates by it.

Finding tests pass while the defect exists and fail once it is fixed.

## PBT and the Quint model (quintgo)

Both directions work, through files only (no import of quintgo):

- **PBT → trace validation.** The fake world records every publisher write as
  an `edgePublish` step (`pushStart`, `writeTable/Parquet/Manifest` with
  Ok/Fail/Ambiguous, `rotateGen`, `sealGen` with `obs.batches`; process =
  epoch) in quintgo's `quint.step` schema as OTLP/JSON, and `quintgo validate`
  replays it with `examples/edgepublish/binding.yaml`. Results on the shrunk
  counterexamples: F1, F3 conform and the model's own
  `committedNoDuplicatePayload` / `sealMatchesManifests` fail at the same step;
  the clock regression does not conform (quint pinpoints the `pushStart` into
  the old generation); the restart bug conforms with every invariant passing:
  the model abstracts table identity. `TestPBTPublisherConformsToModel`
  validates random runs (conformance must hold). It also caught a bug in the
  step recorder (batch ids keyed without the signal).
- **Model → PBT seeds.** `quint run --mbt` traces of `currentDesign` are
  linearized into publisher commands (`quintseed_test.go`).
  `TestModelTracesDriveThePublisher` runs them and compares the end state with
  the model's (manifests, table batches per request, Parquet, seal counts): 20
  of 20 agree, and a mutant (manifest before Parquet) is caught.
  `TestPBTSeededByModelTraces` replays a drawn prefix of a model trace, then
  continues with random commands from that state, all invariants on.

## Limitations

- The state machine runs against a fake of chDB and S3; the S3 repros above
  confirm the semantics the findings depend on, but the fake does not model
  Buffer tables, merges, or a detached table's CREATE behaviour beyond an
  error.
- Sequential only. hegel's `WithConcurrency` would drive concurrent pushes,
  but failures would be nondeterministic; the race is covered by a forced
  interleaving instead.
- Shrinking state-machine counterexamples is the expensive part (10k–50k
  replays), hence `PBT_FINDINGS`.
- Model traces from `quint run` rarely complete a push (outcomes are uniform
  over Ok/Fail/Ambiguous), so they seed failure-heavy states.
