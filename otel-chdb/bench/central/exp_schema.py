"""Schema-evolution and type checks: Parquet read with a newer (wider)
structure, the Parquet's inferred types, writes to a read-only attached
table, and that native and Parquet hold the same rows."""
import json, os, sys
from common import *
out = open(sys.argv[1], "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
ep = meta["r_nat"]["epoch"]
url = parquet_url("natural", ep, [30])


def t(label, sql, **s):
    txt, _, _, code = ch(sql, s, check=False)
    line = f"{label}: HTTP {code}: {txt.strip()[:240]}"
    print(line); out.write(line + "\n")


t("extra column in structure (added in a newer schema)", f"SELECT count(), any(NewCol), countIf(NewCol = '') FROM s3('{url}','{KEY}','{SECRET}','Parquet','{STRUCTURE}, NewCol String')")
t("extra column, input_format_parquet_allow_missing_columns=0", f"SELECT count(), any(NewCol) FROM s3('{url}','{KEY}','{SECRET}','Parquet','{STRUCTURE}, NewCol String')", input_format_parquet_allow_missing_columns=0)
t("column types widened (Duration Int128, schema_version UInt32)", f"SELECT count(), toTypeName(any(Duration)), sum(Duration) FROM s3('{url}','{KEY}','{SECRET}','Parquet','{STRUCTURE.replace('Duration UInt64', 'Duration Int128').replace('schema_version UInt16', 'schema_version UInt32')}')")
t("inferred Parquet schema", f"DESCRIBE TABLE s3('{url}','{KEY}','{SECRET}','Parquet') FORMAT TSV")
t("native: ALTER on the read-only attached table", f"ALTER TABLE r_nat.{TABLE} ADD COLUMN NewCol String")
t("native: INSERT into the read-only attached table", f"INSERT INTO r_nat.{TABLE} (batch_id) VALUES (1)")
ids = list(range(1, 61))
h = ("count(), sum(cityHash64(Timestamp, TraceId, SpanId, toString(SpanName), toString(ServiceName), "
     "mapSort(CAST(SpanAttributes, 'Map(String,String)')), Duration, `Events.Name`, batch_id, row_ordinal))")
t("native r_nat batches 1-60 hash", f"SELECT {h} FROM ({native_select('r_nat', ep, ids)})", use_query_condition_cache=0)
t("parquet batches 1-60 hash", f"SELECT {h} FROM ({parquet_select(parquet_url('natural', ep, ids), ep, ids)})")
