#!/usr/bin/env python3
"""The orderly close on S3 (../../FORMAT.md §3.1, DECISIONS.md D35), after an
edge ran once and shut down in order:

  close_check.py S3_ROOT CLUSTER PRODUCER [--consumed]

- every lane directory of the producer has, in each of its epochs, a last
  slot of `oscope-kind: close` (zero bytes, rows 0), and nothing after it;
- the close's `oscope-low` is at or above every `oscope-received` and every
  other `oscope-low` in its lane (its custody was empty when it was sent);
- with --consumed (after `consume` and `consume watermark` ran on the root):
  each lane's checkpoint says retired by its close at R = that low (the
  newest epoch's), the cluster's watermark document names the lane retired
  with R, its complete_through above R, and nothing was quarantined (a
  restarted edge's replay of what it had committed is copies).

Keys in AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (curl --aws-sigv4).
Prints PASS/FAIL lines; exits 1 on any FAIL.
"""
import json, os, subprocess, sys

KEY = os.environ.get("AWS_ACCESS_KEY_ID", "otel")
SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "otelsecret")
fails = 0


def check(name, ok, detail=""):
    global fails
    fails += 0 if ok else 1
    print(f"{'PASS' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}", flush=True)


def s3(args, url):
    return subprocess.run(["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", f"{KEY}:{SECRET}"] + args + [url],
                          capture_output=True, check=True).stdout


def list_keys(root, sub):
    scheme, rest = root.split("://", 1)
    host, bucket, prefix = rest.split("/", 2)
    keys, token = [], None
    while True:
        q = f"{scheme}://{host}/{bucket}?list-type=2&prefix={prefix}/{sub}"
        if token:
            q += "&continuation-token=" + token.replace("+", "%2B").replace("/", "%2F").replace("=", "%3D")
        body = s3([], q).decode()
        keys += [k.split("</Key>")[0] for k in body.split("<Key>")[1:]]
        if "<IsTruncated>true</IsTruncated>" not in body:
            break
        token = body.split("<NextContinuationToken>")[1].split("</NextContinuationToken>")[0]
    return f"{scheme}://{host}/{bucket}", keys


def head(url):
    out, size = {}, None
    for line in s3(["-I"], url).decode(errors="replace").splitlines():
        k, _, v = line.partition(":")
        if k.lower().startswith("x-amz-meta-"):
            out[k.lower()[len("x-amz-meta-"):]] = v.strip()
        if k.lower() == "content-length":
            size = int(v.strip())
    return out, size


def get_json(url):
    body = s3([], url)
    try:
        return json.loads(body)
    except ValueError:
        return None


def main():
    root, cluster, producer = sys.argv[1:4]
    consumed = "--consumed" in sys.argv
    base, keys = list_keys(root, f"{cluster}/{producer}/")
    lanes = {}
    for k in keys:
        parts = k.split("/")
        signal, epoch, slot = parts[-3], parts[-2], parts[-1]
        if not slot.endswith(".parquet"):
            continue
        lanes.setdefault(signal, {}).setdefault(epoch, []).append((int(slot.split(".")[0]), k))
    check("lanes listed", len(lanes) > 0, f"{sorted(lanes)}")
    r_of = {}
    for signal, epochs in sorted(lanes.items()):
        top_low, received, lows = 0, 0, 0
        for epoch, slots in sorted(epochs.items()):
            top_low = 0  # R: the newest epoch's close (an earlier incarnation's closes are below it)
            slots.sort()
            metas = [(s, head(f"{base}/{k}")) for s, k in slots]
            check(f"{signal}/{epoch} slots consecutive", [s for s, _ in metas] == list(range(len(metas))), f"{[s for s, _ in metas]}")
            last, (m, size) = metas[-1]
            ok = m.get("oscope-kind") == "close" and size == 0 and m.get("oscope-rows") == "0"
            check(f"{signal}/{epoch} ends with its close", ok, f"slot {last}: {m.get('oscope-kind')} ({size} B)")
            closes = [s for s, (mm, _) in metas if mm.get("oscope-kind") == "close"]
            check(f"{signal}/{epoch} one close, last", closes == [last], f"{closes}")
            low = int(m.get("oscope-low", "0"))
            top_low = max(top_low, low)
            for s, (mm, _) in metas[:-1]:
                received = max(received, int(mm.get("oscope-received", "0")))
                lows = max(lows, int(mm.get("oscope-low", "0")))
        check(f"{signal} close low above the lane's received and lows", top_low >= received and top_low >= lows,
              f"close {top_low}, max received {received}, max low {lows}")
        r_of[signal] = top_low
    if consumed:
        ctl = f"{root}/_consumer"
        wm = get_json(f"{ctl}/watermark/{cluster}.json") or {}
        for signal, r in sorted(r_of.items()):
            ck = get_json(f"{ctl}/ckpt/{cluster}/{producer}/{signal}.json") or {}
            check(f"{signal} checkpoint retired by its close at R", ck.get("retired_by") == "close" and ck.get("retired_ns") == r,
                  f"retired_by {ck.get('retired_by')!r}, retired_ns {ck.get('retired_ns')}, want {r}")
            got = (wm.get("retired") or {}).get(f"{producer}/{signal}")
            check(f"{signal} named retired in watermark/{cluster}.json", got == r, f"{got}")
            q = get_json(f"{ctl}/quarantine/{cluster}/{producer}/{signal}.json")
            check(f"{signal} nothing quarantined", not q or not q.get("objects"), f"{q}")
        ct = wm.get("complete_through_ns", 0)
        check(f"{cluster} complete_through past the retired lanes", r_of and ct > max(r_of.values()), f"{ct} vs R {max(r_of.values(), default=0)}")
    print(f"close_check: {fails} failed", flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
