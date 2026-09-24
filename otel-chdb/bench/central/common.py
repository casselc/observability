"""Sources and the central target for the ingest benchmark."""
import os
from chlib import *

# Every generator run publishes under $REGION (default "ce"); the generation
# id is whatever day the data was generated on, so it is discovered.
REGION = os.environ.get("REGION", "ce")


def _gen():
    if os.environ.get("GEN"):
        return os.environ["GEN"]
    p = q(f"SELECT _path FROM s3('{S3}/{REGION}/traces/v1/*/*/manifests/*/00000000000000000001.json', '{KEY}', '{SECRET}', 'One') LIMIT 1")
    return p.split("/")[7]


GEN = _gen()
TABLE = "otel_traces_" + GEN

TRACE_COLS = ["Timestamp", "TraceId", "SpanId", "ParentSpanId", "TraceState", "SpanName", "SpanKind",
              "ServiceName", "ResourceAttributes", "ScopeName", "ScopeVersion", "SpanAttributes",
              "Duration", "StatusCode", "StatusMessage",
              "Events.Timestamp", "Events.Name", "Events.Attributes",
              "Links.TraceId", "Links.SpanId", "Links.TraceState", "Links.Attributes",
              "producer_id", "producer_epoch", "batch_id", "row_ordinal", "received_at", "schema_version"]
COLS = ", ".join("`%s`" % c for c in TRACE_COLS)

# The Parquet's own (plain-typed) structure: what the exporter writes.
STRUCTURE = ("Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String, "
             "SpanName String, SpanKind String, ServiceName String, ResourceAttributes Map(String, String), "
             "ScopeName String, ScopeVersion String, SpanAttributes Map(String, String), Duration UInt64, "
             "StatusCode String, StatusMessage String, `Events.Timestamp` Array(DateTime64(9)), "
             "`Events.Name` Array(String), `Events.Attributes` Array(Map(String, String)), "
             "`Links.TraceId` Array(String), `Links.SpanId` Array(String), `Links.TraceState` Array(String), "
             "`Links.Attributes` Array(Map(String, String)), producer_id String, producer_epoch String, "
             "batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16")

# Central target: the same columns and skip indexes, a different sort key
# (service, hour, span name, time) and dedup window for insert tokens, on the
# server's local disk.
CENTRAL_DDL = """CREATE TABLE IF NOT EXISTS central.otel_traces (
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    ParentSpanId String CODEC(ZSTD(1)),
    TraceState String CODEC(ZSTD(1)),
    SpanName LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration UInt64 CODEC(ZSTD(1)),
    StatusCode LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage String CODEC(ZSTD(1)),
    Events Nested (Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(Delta, ZSTD(1)),
    schema_version UInt16,
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, toStartOfHour(Timestamp), SpanName, Timestamp)
SETTINGS index_granularity = 8192, non_replicated_deduplication_window = 1000"""


def sealed(producer):
    """(epoch, sealed-manifest URL) of a producer's generation."""
    rows = q(f"SELECT _path FROM s3('{S3}/{REGION}/traces/v1/{producer}/*/manifests/{GEN}/*.json', '{KEY}', '{SECRET}', 'One') ORDER BY _path LIMIT 1")
    parts = rows.split("/")  # otel/{region}/traces/v1/{producer}/{epoch}/manifests/...
    epoch = parts[5]
    return epoch, f"{S3}/{REGION}/traces/v1/{producer}/{epoch}/manifests/{GEN}/00000000000000000001.json"


def parquet_url(producer, epoch, ids):
    base = f"{S3}/parquet/{REGION}/traces/v1/{producer}/{epoch}/{GEN}/"
    if len(ids) == 1:
        return base + "%020d.parquet" % ids[0]
    return base + "{" + ",".join("%020d" % i for i in ids) + "}.parquet"


def parquet_multi_url(pairs):
    """One s3() URL naming explicit objects across producers: {p/e/g/id.parquet,...}."""
    items = [f"{p}/{e}/{GEN}/%020d.parquet" % i for p, e, i in pairs]
    return f"{S3}/parquet/{REGION}/traces/v1/{{" + ",".join(items) + "}"


def ids_sql(ids):
    return ",".join(str(i) for i in ids)


def native_select(db, epoch, ids):
    return f"SELECT {COLS} FROM {db}.{TABLE} WHERE producer_epoch = '{epoch}' AND batch_id IN ({ids_sql(ids)})"


def parquet_select(url, epoch=None, ids=None, structure=True):
    st = f", '{STRUCTURE}'" if structure else ""
    w = f" WHERE producer_epoch = '{epoch}' AND batch_id IN ({ids_sql(ids)})" if ids else ""
    return f"SELECT {COLS} FROM s3('{url}', '{KEY}', '{SECRET}', 'Parquet'{st}){w}"
