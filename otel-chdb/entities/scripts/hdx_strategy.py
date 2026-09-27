#!/usr/bin/env python3
"""Which path HyperDX 2.39.1 takes for a column's value discovery on a
database, re-implemented from hyperdx@885d30c
packages/common-utils/src/core/metadata.ts (doMetadataMVsAggregateColumn and
columnAppearsInMvSelect): the FIRST MaterializedView in `SELECT * FROM
system.tables WHERE database = db` whose create query starts `CREATE
MATERIALIZED VIEW db.<mv> TO db.<kvRollupTable>` decides, by whether the
column name appears in its as_select. (Map keys need no such view: getMapKeys
reads the rollup by ColumnIdentifier whenever the source has
metadataMaterializedViews and no text index on the column.)

  hdx_strategy.py DB   -> results/hdx_strategy.json
"""
import json, re, sys
import chlib as c

db = sys.argv[1] if len(sys.argv) > 1 else "ent_b2"
tables = c.rows(f"SELECT name, engine, create_table_query, as_select FROM system.tables WHERE database = '{db}'")


def ident(i):
    return f"(?:`{re.escape(i)}`|{re.escape(i)})"


def appears(sql, col):
    esc = re.escape(col)
    bare = re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col)
    return re.search(f"`{esc}`|(?<![A-Za-z0-9_`-]){esc}(?![A-Za-z0-9_`-])" if bare else f"`{esc}`", sql) is not None


def strategy(rollup, col):
    for t in tables:
        if t["engine"] == "MaterializedView" and re.match(f"^CREATE MATERIALIZED VIEW {ident(db)}\\.{ident(t['name'])} TO {ident(db)}\\.{ident(rollup)}", t["create_table_query"]):
            return {"decided_by": t["name"], "rollup": appears(t["as_select"], col)}
    return {"decided_by": None, "rollup": False}


out = {"db": db, "tables_in_order": [t["name"] for t in tables]}
for sig, cols in (("logs", ["ResourceAttributes", "SeverityText", "ServiceName", "ScopeName", "ScopeVersion", "LogAttributes", "ScopeAttributes"]),
                  ("traces", ["ResourceAttributes", "ServiceName", "SpanName", "SpanKind", "StatusCode", "SpanAttributes"])):
    for col in cols:
        out[f"{sig}.{col}"] = strategy(f"otel_{sig}_kv_rollup_15m", col)
print(json.dumps(out, indent=1))
json.dump(out, open(f"{c.HERE}/../results/hdx_strategy.json", "w"), indent=1)
