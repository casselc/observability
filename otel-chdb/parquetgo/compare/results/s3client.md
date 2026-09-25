# S3 client: minio-go v7.0.91 vs aws-sdk-go-v2 (service/s3 v1.113.4)

`pubbench -count-s3 -batches 30` (parquet-go engine, 10k spans/batch, zstd,
blooms on) to SeaweedFS 4.47 on localhost, alternating old/new, 3 runs each.
Shared 4-vCPU box.

| client | median ms/batch | CPU ms/batch | Go allocs/batch | max RSS MB | RSS after start MB | PUTs/batch |
| --- | --- | --- | --- | --- | --- | --- |
| minio-go | 78.9 / 81.2 / 78.2 | 72.4 / 74.3 / 73.4 | 1537 / 1534 / 1527 | 110 / 110 / 117 | 58.6 / 58.8 / 59.0 | 2 |
| aws-sdk-go-v2 | 75.8 / 80.4 / 79.4 | 71.6 / 73.7 / 73.0 | 1764 / 1774 / 1768 | 111 / 126 / 107 | 62.8 / 62.9 / 63.1 | 2 |

Same within noise: +~240 allocations and +4 MB resident at start for the SDK,
no measurable CPU or latency difference. Parquet encoding dominates.

## One PUT, client side only (`compare/s3put_bench_test.go`)

A stub server that discards the body, so this is signing, checksums,
middleware and allocations only. 300 PUTs × 3 runs; medians. [M]

| body | scheme | client | µs/PUT | allocs | B/op |
| --- | --- | --- | --- | --- | --- |
| 1 MiB | https | minio-go | 1,607 | 248 | 52 K |
| 1 MiB | https | SDK default | 1,772 | 555 | 76 K |
| 1 MiB | https | SDK + explicit unsigned payload | 1,693 | 557 | 76 K |
| 1 MiB | https | SDK, no retryer, checksums only when required | 1,434 | 555 | 76 K |
| 1 MiB | https | SigV4 signer + net/http (no SDK stack) | 1,346 | 191 | 48 K |
| 1 MiB | http | minio-go | 5,025 | 1,027 | 406 K |
| 1 MiB | http | SDK default | 4,163 | 506 | 73 K |
| 1 MiB | http | SDK + explicit unsigned payload | 864 | 502 | 73 K |
| 1 MiB | http | SigV4 signer + net/http | 743 | 135 | 45 K |
| 1.2 KB | https | minio-go | 250 | 198 | 18 K |
| 1.2 KB | https | SDK default | 419 | 506 | 41 K |
| 1.2 KB | https | SigV4 signer + net/http | 160 | 138 | 12 K |

- **Over https the SDK already sends `UNSIGNED-PAYLOAD`** (the body is covered
  by its CRC32 checksum and TLS), so SigV4 does not SHA-256 the body; the
  explicit unsigned option changes nothing there (`TestPayloadSigning` pins
  this). The ~3 ms/MiB of SHA-256 appears only over plain http, which is what
  local SeaweedFS uses, and there both clients pay it (minio-go more).
- **Allocations are a fixed ~500–555 per SDK PUT** (its per-call middleware
  stack); retry and checksum settings move them by < 10. minio-go allocates
  about half as many over https. Either is tens of µs per PUT against a
  ~72 ms batch, i.e. ~0.1% of CPU.
- **The floor** is the SDK's own signer on a plain `net/http` request:
  ~135–190 allocations and the lowest latency, at the cost of owning retries,
  endpoint handling and error parsing (a 412 is just a status code there).
  Worth it only if allocations matter in a memory-constrained collector.
