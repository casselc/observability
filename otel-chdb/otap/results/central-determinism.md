| signal | layout | settings | identical content on 2 runs |
| traces | ref | default | true |
| traces | ref | single-thread | true |
| traces | star | default | true |
| traces | star | single-thread | true |
| traces | flat-parquet | default | true |
| traces | flat-parquet | single-thread | true |
| traces | flat-arrow | default | true |
| traces | flat-arrow | single-thread | true |
| traces | raw | single-thread | true |
| logs | ref | default | true |
| logs | ref | single-thread | true |
| logs | star | default | true |
| logs | star | single-thread | true |
| logs | flat-parquet | default | true |
| logs | flat-parquet | single-thread | true |
| logs | flat-arrow | default | true |
| logs | flat-arrow | single-thread | true |
| logs | raw | single-thread | true |
| signal | layout | retry | rows after 2 attempts (want 100000) |
| traces | ref | default settings both times | 100000 |
| traces | ref | single-thread both times | 100000 |
| traces | ref | max_threads 1 then default | 100000 |
| traces | star | default settings both times | 100000 |
| traces | star | single-thread both times | 100000 |
| traces | star | max_threads 1 then default | 100000 |
| traces | flat-parquet | default settings both times | 100000 |
| traces | flat-parquet | single-thread both times | 100000 |
| traces | flat-parquet | max_threads 1 then default | 100000 |
| traces | flat-arrow | default settings both times | 100000 |
| traces | flat-arrow | single-thread both times | 100000 |
