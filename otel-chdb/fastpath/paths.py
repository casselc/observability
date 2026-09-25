"""The two insert paths, as the fast path and the importer would issue them."""
from fp import *
from common import STRUCTURE, COLS
from setup import make_target

KEYS = {i: f"edge-1/e1/{i:020d}.parquet" for i in list(range(1, 9)) + [100]}


def obj_url(i):
    return f"{PREFIX}/{KEYS[i]}"


_body = {}


def body(i, fmt="Parquet"):
    """The bytes the exporter holds in memory. Parquet: the committed object
    itself, byte for byte (RawBLOB). Native: the same rows re-encoded."""
    k = (i, fmt)
    if k not in _body:
        if fmt == "Parquet":
            sql = f"SELECT * FROM s3('{obj_url(i)}', '{KEY}', '{SECRET}', 'RawBLOB') FORMAT RawBLOB"
        else:
            sql = f"SELECT {COLS} FROM s3('{obj_url(i)}', '{KEY}', '{SECRET}', 'Parquet', '{STRUCTURE}') FORMAT {fmt}"
        r = requests.post(CH + "/", data=sql.encode(), timeout=120)
        r.raise_for_status()
        _body[k] = r.content
    return _body[k]


def fresh(t="t", **kw):
    make_target(t, **kw)
    q(f"SYSTEM STOP MERGES {DB}.{t}")
    return t


def fast(t, i, tok, wait=1, fmt="Parquet", extra=None, check=True):
    """The fast path: the exporter posts its in-memory batch as an async insert."""
    s = {"async_insert": 1, "wait_for_async_insert": wait}
    if tok:
        s["insert_deduplication_token"] = tok
    s.update(extra or {})
    return ch(f"INSERT INTO {DB}.{t} ({COLS}) FORMAT {fmt}", settings=s, data=body(i, fmt), check=check)


def importer(t, i, tok, one_block=True, extra=None, check=True):
    """The importer: central pulls the committed object by its exact key."""
    s = dict(ONE_BLOCK) if one_block else {}
    s["async_insert"] = 0
    if tok:
        s["insert_deduplication_token"] = tok
    s.update(extra or {})
    return ch(f"INSERT INTO {DB}.{t} ({COLS}) SELECT {COLS} FROM s3('{obj_url(i)}', '{KEY}', '{SECRET}', 'Parquet', '{STRUCTURE}')",
              settings=s, check=check)


def parts(t):
    return q(f"SELECT groupArray(rows) FROM (SELECT rows FROM system.parts WHERE database='{DB}' AND table='{t}' AND active ORDER BY name)")


def rec(log, case, t, expect, **kw):
    n = count(t)
    log(case=case, rows=n, expect=expect, ok=(n == expect), parts=parts(t), **kw)


