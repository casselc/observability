| table | rows | B/row, replayed pool | B/row, no replay | earlier (loaded box) | top columns, B/row (no replay where measured) |
|---|---|---|---|---|---|
| traces-random-ids | 5,000,000 | 39.08 | – | 40.9 | TraceId 16.268, SpanId 8.265, ParentSpanId 6.697 |
| logs-random-ids | 5,000,000 | 18.82 | – | 18.9 | TraceId 8.168, SpanId 4.165, LogAttributes 2.452 |
| traces-testgen-ids | 5,000,000 | 9.63 | – | 9.4 | SpanAttributes 3.633, ResourceAttributes 2.033, ParentSpanId 0.523 |
| B-gauge | 3,360,000 | 0.72 | 1.66 | 1.45 | Value 1.378, series_id 0.09, TimeUnix 0.06 |
| B-sum | 6,240,000 | 0.71 | 1.51 | 1.36 | Value 1.202, series_id 0.089, TimeUnix 0.054 |
| B-number (gauge+sum) | 9,600,000 | 0.71 | – | – | Value 0.452, series_id 0.089, TimeUnix 0.045 |
| B-histogram | 1,920,000 | 8.38 | 15.23 | 13.8 | BucketCounts 6.433, Sum 6.256, Count 0.862 |
| B-exponential_histogram | 240,000 | 25.41 | 38.75 | 38.1 | PositiveBucketCounts 14.121, Sum 7.389, Max 7.357 |
| B-summary | 240,000 | 10.77 | 12.34 | 11.7 | ValueAtQuantiles.Value 6.347, Sum 4.511, Count 0.902 |
| B-series (5 cycles) | 100,000 | 38.44 | – | 39.6 per series row | ResourceAttributes 24.263, series_id 8.035, Attributes 3.788 |
| A-sum | 6,240,000 | 19.42 | 20.62 | 20.4 | ResourceAttributes 15.114, Value 1.854, Attributes 1.811 |
| A-gauge | 3,360,000 | 18.42 | 19.43 | 19.1 | ResourceAttributes 15.096, Value 1.489, Attributes 0.834 |
| A-histogram | 1,920,000 | 27.63 | 38.83 | 38.1 | ResourceAttributes 15.149, BucketCounts 9.419, Sum 6.891 |
| A-exponential_histogram | 240,000 | 37.94 | 57.18 | 57.3 | ResourceAttributes 15.205, PositiveBucketCounts 14.393, Sum 7.429 |
| A-summary | 240,000 | 21.47 | 33.33 | 33.8 | ResourceAttributes 15.183, ValueAtQuantiles.Value 7.249, Sum 6.26 |
