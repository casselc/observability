
### Mid scenario, metrics layout B (series table, the default)

| output | current constants | clean constants | Δ |
|---|---|---|---|
| rows/s | 2,066,667 | 2,066,667 | +0% |
| insert vCPU per replica | 6.9 | 6.4 | -8% |
|   of it fixed per-object | 0.17 | 0.19 | +9% |
| merge vCPU per replica | 17.0 | 13.3 | -22% |
| headroom vCPU per replica | 17.9 | 14.8 | -18% |
| query vCPU | 31.4 | 25.8 | -18% |
| vCPU per replica | 57.6 | 47.4 | -18% |
| shards (32 vCPU) | 2 | 2 | +0% |
| nodes (x2 replicas) | 4 | 4 | +0% |
| central vCPU, total | 115 | 95 | -18% |
| compressed TB/day, one copy | 5.87 | 5.92 | +1% |
| storage TB, all copies | 991 | 992 | +0% |
| edge vCPU, region, Rust | 5.9 | 5.8 | -2% |
| edge vCPU, region, Go | 8.0 | 7.9 | -1% |

### Mid scenario, metrics layout A (ClickStack tables)

| output | current constants | clean constants | Δ |
|---|---|---|---|
| rows/s | 2,066,667 | 2,066,667 | +0% |
| insert vCPU per replica | 27.4 | 22.7 | -17% |
|   of it fixed per-object | 0.17 | 0.19 | +9% |
| merge vCPU per replica | 46.8 | 33.0 | -29% |
| headroom vCPU per replica | 55.7 | 41.8 | -25% |
| query vCPU | 97.4 | 73.2 | -25% |
| vCPU per replica | 178.6 | 134.1 | -25% |
| shards (32 vCPU) | 6 | 5 | -17% |
| nodes (x2 replicas) | 12 | 10 | -17% |
| central vCPU, total | 357 | 268 | -25% |
| compressed TB/day, one copy | 8.03 | 8.07 | +0% |
| storage TB, all copies | 1116 | 1117 | +0% |
| edge vCPU, region, Rust | 10.3 | 11.0 | +6% |
| edge vCPU, region, Go | 15.3 | 15.0 | -2% |
