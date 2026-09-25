load average 0.44–1.98, 6 runs

| config | edge CPU ms / 10k-span request | disk written KB / request | buffer dir after 30 requests, KB | client ack median ms | send start to all 30 committed, s |
|---|---|---|---|---|---|
| edge.yaml | 31.7 [31.0–32.7] | 0 [0–0] | 0 [0–0] | 36.2 [35.9–36.8] | 1.27 [1.23–1.35] |
| edge-durable.yaml | 42.7 [41.3–45.3] | 6299 [6299–6300] | 28052 [28052–28052] | 19.0 [10.1–21.3] | 1.67 [1.66–1.86] |
