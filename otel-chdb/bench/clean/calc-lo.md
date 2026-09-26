
### Mid scenario, metrics layout B (series table, the default)

| output | current constants | clean constants | Δ |
|---|---|---|---|
| rows/s | 2,066,667 | 2,066,667 | +0% |
| insert vCPU per replica | 6.9 | 5.0 | -28% |
|   of it fixed per-object | 0.17 | 0.12 | -32% |
| merge vCPU per replica | 17.0 | 11.2 | -34% |
| headroom vCPU per replica | 17.9 | 12.1 | -32% |
| query vCPU | 31.4 | 21.2 | -32% |
| vCPU per replica | 57.6 | 38.9 | -32% |
| shards (32 vCPU) | 2 | 2 | +0% |
| nodes (x2 replicas) | 4 | 4 | +0% |
| central vCPU, total | 115 | 78 | -32% |
| compressed TB/day, one copy | 5.87 | 5.92 | +1% |
| storage TB, all copies | 991 | 992 | +0% |
| edge vCPU, region, Rust | 5.9 | 5.5 | -7% |
| edge vCPU, region, Go | 8.0 | 7.2 | -9% |

### Mid scenario, metrics layout A (ClickStack tables)

| output | current constants | clean constants | Δ |
|---|---|---|---|
| rows/s | 2,066,667 | 2,066,667 | +0% |
| insert vCPU per replica | 27.4 | 20.3 | -26% |
|   of it fixed per-object | 0.17 | 0.12 | -32% |
| merge vCPU per replica | 46.8 | 30.8 | -34% |
| headroom vCPU per replica | 55.7 | 38.3 | -31% |
| query vCPU | 97.4 | 67.1 | -31% |
| vCPU per replica | 178.6 | 123.0 | -31% |
| shards (32 vCPU) | 6 | 4 | -33% |
| nodes (x2 replicas) | 12 | 8 | -33% |
| central vCPU, total | 357 | 246 | -31% |
| compressed TB/day, one copy | 8.03 | 8.07 | +0% |
| storage TB, all copies | 1116 | 1117 | +0% |
| edge vCPU, region, Rust | 10.3 | 10.8 | +4% |
| edge vCPU, region, Go | 15.3 | 13.9 | -9% |
