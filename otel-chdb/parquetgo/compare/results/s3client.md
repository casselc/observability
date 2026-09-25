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
