"""Unit tests of compare.py's query builders (no services): python3 -m unittest compare_test

Regression (verify 2026-09-29): D33 added a `cluster` column to the key/value
rollups; rollup_source still listed the rollup's columns by hand, without it,
so the comparison's hash over every column named a column its subquery did not
select and the whole conformance run died in ClickHouse (UNKNOWN_IDENTIFIER).
"""
import os
import re
import unittest

import compare

SQL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "otap-rs", "sql")


def rollup_columns(ddl_file):
    """The columns of the `{table}_kv_rollup_15m` CREATE TABLE in a DDL file."""
    ddl = open(os.path.join(SQL, ddl_file)).read()
    body = ddl.split("CREATE TABLE IF NOT EXISTS {table}_kv_rollup_15m", 1)[1].split("ENGINE", 1)[0]
    return re.findall(r"^\s*`([^`]+)`\s", body, re.M)


class RollupSource(unittest.TestCase):
    def test_every_ddl_column_is_selected_and_grouped(self):
        for f in ("otel_traces.sql", "otel_logs.sql"):
            cols = rollup_columns(f)
            self.assertIn("cluster", cols, f)
            sql = compare.rollup_source("db.t", cols)
            select, group = sql.split(" GROUP BY ", 1)
            for c in cols:
                self.assertIn(f"`{c}`", select, (f, c))
                if c != "count":
                    self.assertIn(f"`{c}`", group, (f, c))
            self.assertIn("sum(`count`) AS `count`", select)
            self.assertNotIn("`count`", group)

    def test_span_kind_normalized_and_old_tables_accepted(self):
        sql = compare.rollup_source("db.t", ["Timestamp", "ColumnIdentifier", "Key", "Value", "count"])
        self.assertIn(compare.ROLLUP_VALUE + " AS `Value`", sql)
        self.assertNotIn("cluster", sql)


if __name__ == "__main__":
    unittest.main()
