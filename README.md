# observability

Design and spike work on shipping telemetry from edge OpenTelemetry collectors to
object storage (S3 or S3-compatible, as Parquet) and from there into a central
ClickHouse / ClickStack cluster queried by HyperDX, reliably and cheaply.

| Directory | What it is |
| --- | --- |
| [`otel-chdb/`](otel-chdb/README.md) | The spike: edge publishers (Go `parquetgo`, Rust `otap-rs`), the S3-native consumer, Quint models, benchmarks, the metrics layout, the lake exploration, and the live-environment acceptance kit. Start with [`DECISIONS.md`](otel-chdb/DECISIONS.md). |
| [`quintgo/`](quintgo/README.md) | Go tooling for Quint: loading, validating and model-based testing against Quint specs, used by the `otel-chdb` models. |

## History

This work began under `spike/otel-chdb` and `spike/quintgo` in
[chucklehead-dev/oscope](https://github.com/chucklehead-dev/oscope) and was
moved here with its history intact (`git filter-repo`). Go module paths changed
from `github.com/chucklehead-dev/oscope/spike/...` to
`github.com/casselc/observability/...`. Recorded benchmark logs and results
still show the old `spike/...` paths, which were correct when they were
captured.
