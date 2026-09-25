"""Create fastpath_test, the central-typed target, and the committed batch
objects under s3://otel/fastpath/. Idempotent: recreates everything."""
from fp import *
from common import CENTRAL_DDL, STRUCTURE, COLS  # bench/central (reads S3 once at import)

q(f"CREATE DATABASE IF NOT EXISTS {DB}")


def make_target(name, extra_settings="", partition="toDate(Timestamp)"):
    ddl = CENTRAL_DDL.replace("central.otel_traces", f"{DB}.{name}")
    ddl = ddl.replace("PARTITION BY toDate(Timestamp)", f"PARTITION BY {partition}")
    if extra_settings:
        ddl = ddl + ", " + extra_settings
    q(f"DROP TABLE IF EXISTS {DB}.{name} SYNC")
    q(ddl)


def put_batch(key, producer, epoch, batch, shift_sql="0", two_days=False):
    """Write one committed batch object: the seed's 8,000 rows re-stamped with
    this envelope. two_days puts odd rows one day later, so the batch spans
    two partitions of a toDate(Timestamp) table."""
    ts = "Timestamp + toIntervalSecond(%s)" % shift_sql
    if two_days:
        ts = f"if(row_ordinal % 2 = 1, {ts} + toIntervalDay(1), {ts})"
    cols = [c.strip("`") for c in COLS.split(", ")]
    sel = []
    for c in cols:
        if c == "Timestamp":
            sel.append(f"{ts} AS Timestamp")
        elif c == "producer_id":
            sel.append(f"'{producer}' AS producer_id")
        elif c == "producer_epoch":
            sel.append(f"'{epoch}' AS producer_epoch")
        elif c == "batch_id":
            sel.append(f"toUInt64({batch}) AS batch_id")
        else:
            sel.append(f"`{c}`")
    q(f"INSERT INTO FUNCTION s3('{PREFIX}/{key}', '{KEY}', '{SECRET}', 'Parquet', '{STRUCTURE}') "
      f"SELECT {', '.join(sel)} FROM s3('{SEED}', '{KEY}', '{SECRET}', 'Parquet', '{STRUCTURE}')",
      settings={"s3_truncate_on_insert": 1, "output_format_parquet_row_group_size": 1000000})


if __name__ == "__main__":
    for i in range(1, 9):
        put_batch(f"edge-1/e1/{i:020d}.parquet", "edge-1", "e1", i, shift_sql=str(10 * i))
    put_batch(f"edge-1/e1/{100:020d}.parquet", "edge-1", "e1", 100, two_days=True)
    print(q(f"SELECT _path, count(), uniqExact(toDate(Timestamp)) FROM s3('{PREFIX}/edge-1/e1/*.parquet', '{KEY}', '{SECRET}', 'Parquet') GROUP BY _path ORDER BY _path"))
