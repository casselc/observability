# Quint models of the edge-publishing design

Two executable [Quint](https://quint-lang.org) models of the chdb exporter's
publishing mode (`../chdbexporter/publish.go`) and the central consumer it is
meant for. They were built with the skills in
[quint-llm-kit](https://github.com/quint-co/quint-llm-kit): `quint-modeling`
(the from-code flow) and `quint-lang`. Each model is checked two ways:

- `quint run`, which simulates random traces and finds counterexamples;
- `quint verify`, which runs the Apalache model checker: an exhaustive,
  bounded check of every execution up to a given number of steps.

| File | What it models |
| --- | --- |
| `edgePublish.qnt` | The commit protocol end to end. The collector's persistent queue retries a payload after a failed export. The writer crashes and restarts with a new epoch. S3 writes succeed, fail, or fail **ambiguously** (the object lands but the writer sees an error). A batch goes to the table, then Parquet, then its manifest. Generations rotate under a lock and are sealed. Central workers claim manifested batches, insert them with a dedup token, and record them in a ledger; a worker can crash between insert and ledger. |
| `partLifetime.qnt` | Object lifetime on one published table. The writer inserts, merges, and cleans up after `old_parts_lifetime`, and may exit. Readers refresh their list of parts on an interval (or stop refreshing) and run queries of bounded length. The consumer garbage-collects a generation, with or without reader leases. Time is an integer tick, because the timing bounds are the point. |
| `*_test.qnt` | Deterministic scenarios: the minimal story behind each counterexample, and the same story against each fix. |
| `trace.py` | Summarises an ITF trace from `quint run --mbt --out-itf`: the action taken at each step and what changed. |

Design choices are `const` flags, and each configuration is an instance module
(`currentDesign`, `fixedDesign`, `recommended`, `noRotationLock`, …). That lets
one spec show a counterexample and its fix side by side.

```sh
npm i -g @informalsystems/quint     # 0.32; Apalache is fetched on first `quint verify` (needs Java)
quint test edgePublish_test.qnt --main currentTest --backend typescript
quint run  edgePublish.qnt --main recommended --invariant payloadIngestedAtMostOnce \
           --max-steps 40 --max-samples 5000 --backend typescript
quint verify edgePublish.qnt --main fixedDesign --invariant sealMatchesManifests --max-steps 10
```

(`--backend typescript` is only needed where the Rust evaluator can't be
downloaded, as in this sandbox.)

RESULTS_PLACEHOLDER
