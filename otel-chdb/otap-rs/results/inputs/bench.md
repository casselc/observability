load average 0.53–0.94, 27 runs

| signal (10k items / request) | transport | edge process CPU ms / request | peak RSS MB | client ack median ms | sender encode ms (OTAP producer) |
|---|---|---|---|---|---|
| traces | otlp-http | 31.3 [30.7–32.0] | 59 [59–59] | 36.2 [35.5–37.0] | – |
| traces | otlp-grpc | 33.3 [32.3–33.7] | 59 [59–60] | 42.0 [40.9–42.7] | – |
| traces | otap-grpc | 42.3 [42.0–44.0] | 63 [63–64] | 47.1 [46.5–48.2] | 79.1 [78.1–81.9] |
| logs | otlp-http | 26.0 [26.0–26.7] | 52 [52–53] | 30.7 [29.9–30.7] | – |
| logs | otlp-grpc | 26.7 [25.3–26.7] | 53 [52–53] | 31.9 [31.5–32.8] | – |
| logs | otap-grpc | 29.7 [29.7–30.3] | 54 [54–54] | 33.3 [32.8–33.7] | 50.9 [50.7–52.1] |
| metrics | otlp-http | 15.3 [15.0–15.3] | 57 [57–58] | 13.7 [13.5–13.8] | – |
| metrics | otlp-grpc | 15.7 [15.3–16.0] | 58 [57–58] | 17.1 [16.3–17.1] | – |
| metrics | otap-grpc | 23.7 [22.3–23.7] | 62 [60–62] | 23.5 [20.8–27.6] | 55.9 [55.1–58.1] |
