#!/usr/bin/env python3
"""The schemas compared (../README.md §Schemas), derived from the consumer's
DDL as committed at HEAD (../../otap-rs/sql/otel_{traces,logs}.sql: ClickStack
2.39.1's tables, option 2 = without the idx_*_attr_key indexes) by text
transformation, so that (b) and (c) are exactly "(a) with ResourceAttributes
replaced":

  a      HEAD's DDL: table + key-value rollup + its materialized view
  full   ClickStack 2.39.1's full DDL (../../hyperdx/sql/clickstack_full_*.sql,
         with idx_*_attr_key; the consumer's DDL before option 2); insert only
  b1     ResourceAttributes -> resource_id UInt64 + ResourceResidual Map;
         ResourceAttributeItems, idx_res_attr_items and (logs) the eight
         __hdx_materialized_k8s.* / deployment.environment columns and their
         rollup branches removed; ClickStack's sort key; a bloom filter on
         resource_id (for rewritten filters); base table <sig>_rid + view
  b2     b1 with resource_id in the sort key, no bloom filter:
         traces (ServiceName, resource_id, SpanName, toDateTime(Timestamp))
         logs   (toStartOfFiveMinutes(Timestamp), ServiceName, resource_id, Timestamp)
  c      b2's table with ResourceAttributes as an ALIAS column on the table
         itself (no view): HyperDX keeps seeing a MergeTree with its text
         indexes
  ann    the edge-announced variant: resource_id = hash of the WHOLE resource
         map, no residual column; a view over an announced-resources
         dictionary (the D7 series-table shape for traces and logs)

Views return (a)'s ordinary columns, in (a)'s order, with ResourceAttributes
rebuilt (recon.py). The b/c/ann key-value rollups also take the catalog's
ResourceAttributes rows (discovery.py) through `resource_kv_in` (Null) and a
materialized view whose name sorts first and whose SELECT names every column
the rollup serves: HyperDX 2.39.1 decides whether a column's values come from
the rollup by reading the FIRST materialized view into it
(common-utils/src/core/metadata.ts doMetadataMVsAggregateColumn).

  schemas.py create VARIANT DB [--cat ent_cat]    create one variant's tables in DB
  schemas.py write                                write ../sql/generated/*.sql for reading
"""
import os, re, sys
import chlib as c
import recon

SIGS = ("traces", "logs")
CAT = "ent_cat"
NATIVE = {"traces": ["ServiceName", "SpanName", "SpanKind", "StatusCode", "ScopeName", "ScopeVersion"],
          "logs": ["SeverityText", "ServiceName", "ScopeName", "ScopeVersion", "ResourceSchemaUrl", "ScopeSchemaUrl"]}


def head(sig):
    return c.head_sql(f"otel_{sig}.sql")


def split(text, **subs):
    body = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--"))
    for k, v in subs.items():
        body = body.replace("{" + k + "}", v)
    return [x.strip().rstrip(";").strip() for x in body.split(";\n") if x.strip()]


def base_ddl(variant, sig, table):
    """The table, rollup table and native rollup MV of a variant; table = the fully qualified base table."""
    if variant == "full":
        return split(open(f"{c.HERE}/../../hyperdx/sql/clickstack_full_{sig}.sql").read(), table=table)
    st = split(head(sig), table=table)
    if variant == "a":
        return st
    t, roll, mv = st
    db, name = table.split(".")
    pub = f"{db}.otel_{sig}"  # the name HyperDX sees (view, or the table itself for c)
    lines = []
    for l in t.split("\n"):
        s = l.strip()
        if s.startswith("`ResourceAttributes` Map"):
            lines.append("    `resource_id` UInt64 CODEC(ZSTD(1)),")
            if variant != "ann":
                lines.append("    `ResourceResidual` Map(LowCardinality(String), String) CODEC(ZSTD(1)),")
            if variant == "c":
                lines.append(f"    `ResourceAttributes` Map(LowCardinality(String), String) ALIAS {recon.resource_attributes(CAT)},")
            continue
        if s.startswith("`ResourceAttributeItems`") or s.startswith("INDEX idx_res_attr_items"):
            continue
        if s.startswith("`__hdx_materialized_k8s.") or s.startswith("`__hdx_materialized_deployment.environment.name`"):
            continue
        if s.startswith("`__hdx_materialized_rum.sessionId`"):
            if variant == "ann":
                continue
            l = l.replace("ResourceAttributes['rum.sessionId']", "ResourceResidual['rum.sessionId']")
        if s.startswith("INDEX idx_rum_session_id") and variant == "ann":
            continue
        if s.startswith("INDEX idx_trace_id") and variant == "b1":
            lines.append(l)
            lines.append("    INDEX idx_resource_id resource_id TYPE bloom_filter(0.01) GRANULARITY 1,")
            continue
        lines.append(l)
    t = "\n".join(lines)
    if variant in ("b2", "c", "ann"):
        t = t.replace("ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))", "ORDER BY (ServiceName, resource_id, SpanName, toDateTime(Timestamp))")
        t = t.replace("ORDER BY (toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)", "ORDER BY (toStartOfFiveMinutes(Timestamp), ServiceName, resource_id, Timestamp)")
    roll = roll.replace(f"{table}_kv_rollup_15m", f"{pub}_kv_rollup_15m")
    # the native rollup MV: from the base table into the public name's rollup, minus the per-row k8s branches
    mv = mv.replace(f"TO {table}_kv_rollup_15m", f"TO {pub}_kv_rollup_15m")
    mv = re.sub(r"CREATE MATERIALIZED VIEW IF NOT EXISTS \S+", f"CREATE MATERIALIZED VIEW IF NOT EXISTS {pub}_kv_rollup_15m_mv", mv, count=1)
    body, tail = mv.rsplit("\n)\nSELECT", 1)
    parts = [p for p in body.split("UNION ALL") if "__hdx_materialized_k8s." not in p and "__hdx_materialized_deployment" not in p]
    mv = "UNION ALL".join(parts).rstrip() + "\n)\nSELECT" + tail
    if "__hdx_materialized" in mv:
        raise SystemExit("rollup MV rewrite left a materialized branch")
    catalog_in = (f"CREATE TABLE IF NOT EXISTS {db}.resource_kv_in_{sig} (`Timestamp` DateTime, `ColumnIdentifier` LowCardinality(String), "
                  f"`Key` LowCardinality(String), `Value` String, `count` UInt64) ENGINE = Null")
    served = ", ".join(f"'{x}'" for x in ["ResourceAttributes"] + NATIVE[sig])
    catalog_mv = (f"CREATE MATERIALIZED VIEW IF NOT EXISTS {pub}_kv_rollup_15m_0_catalog_mv TO {pub}_kv_rollup_15m AS "
                  f"SELECT Timestamp, ColumnIdentifier, Key, Value, count FROM {db}.resource_kv_in_{sig} WHERE ColumnIdentifier IN ({served})")
    return [t, roll, catalog_in, catalog_mv, mv]


def ordinary_columns(db, table):
    return [r["name"] for r in c.rows(f"SELECT name FROM system.columns WHERE database = '{db}' AND table = '{table}' "
                                      f"AND default_kind = '' ORDER BY position")]


def view_ddl(variant, sig, db, a_cols):
    rid = "resource_id"
    if variant == "ann":
        ra = f"CAST(JSONExtract(dictGet('{CAT}.d_ann', 'attrs', resource_id), 'Map(String, String)'), 'Map(LowCardinality(String), String)')"
    else:
        ra = recon.resource_attributes(CAT, rid)
    sel = ",\n    ".join(f"{ra} AS `ResourceAttributes`" if col == "ResourceAttributes" else f"`{col}`" for col in a_cols)
    return f"CREATE VIEW IF NOT EXISTS {db}.otel_{sig} AS\nSELECT\n    {sel}\nFROM {db}.otel_{sig}_rid"


A_COLS = {}


def a_columns(sig):
    """(a)'s ordinary columns in order, from a scratch table with HEAD's DDL."""
    if sig not in A_COLS:
        c.q("CREATE DATABASE IF NOT EXISTS ent_scratch")
        c.q(f"DROP TABLE IF EXISTS ent_scratch.otel_{sig} SYNC")
        c.q(split(head(sig), table=f"ent_scratch.otel_{sig}")[0])
        A_COLS[sig] = ordinary_columns("ent_scratch", f"otel_{sig}")
        c.q(f"DROP TABLE ent_scratch.otel_{sig} SYNC")
    return A_COLS[sig]


def create(variant, db):
    c.q(f"CREATE DATABASE IF NOT EXISTS {db}")
    out = []
    for sig in SIGS:
        base = f"{db}.otel_{sig}" if variant in ("a", "full", "c") else f"{db}.otel_{sig}_rid"
        st = base_ddl(variant, sig, base)
        if variant in ("b1", "b2", "ann"):
            st.append(view_ddl(variant, sig, db, a_columns(sig)))
        for s in st:
            c.q(s)
        out += st
    return out


def write():
    os.makedirs(f"{c.SQL}/generated", exist_ok=True)
    for v in ("b1", "b2", "c", "ann"):
        for sig in SIGS:
            base = f"{{db}}.otel_{sig}" if v == "c" else f"{{db}}.otel_{sig}_rid"
            st = base_ddl(v, sig, base)
            if v != "c":
                st.append(view_ddl(v, sig, "{db}", a_columns(sig)))
            with open(f"{c.SQL}/generated/{v}_{sig}.sql", "w") as f:
                f.write(f"-- GENERATED by ../../scripts/schemas.py from otap-rs/sql/otel_{sig}.sql at HEAD {c.head_commit()}\n"
                        f"-- (variant {v}; see the script's docstring). {{db}} is the database; the catalog is {CAT}.\n")
                for s in st:
                    f.write(s.replace("ent_cat.", "{cat}.") + ";\n\n")


if __name__ == "__main__":
    if sys.argv[1] == "create":
        for s in create(sys.argv[2], sys.argv[3]):
            pass
        print("created", sys.argv[2], "in", sys.argv[3])
    elif sys.argv[1] == "write":
        write()
