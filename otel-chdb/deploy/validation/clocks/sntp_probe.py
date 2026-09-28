#!/usr/bin/env python3
"""Clock offset of this host (a pod shares its node's clock) against
reference NTP servers, and optionally against a ClickHouse server's clock:
one JSON line per sample on stdout. Stdlib only, so it runs in any python
image, on a node, or on the consumer's and ClickHouse's hosts.

  REFS=169.254.169.123,time.aws.com EVERY=10 [CH_URL=http://clickhouse:8123 NODE=name] sntp_probe.py

offset_ms > 0 means this clock is AHEAD of the reference (SNTP, RFC 4330:
offset = ((t1 - t0) + (t2 - t3)) / 2, sign flipped to "local minus reference").
delay_ms is the round trip; a sample's error is at most delay/2.
CH: offset of this clock against ClickHouse's now64(6), from the midpoint of
an HTTP request (error at most half its round trip, which is logged).
"""
import json, os, socket, struct, sys, time, urllib.request

NTP_EPOCH = 2208988800
REFS = [r for r in os.environ.get("REFS", "169.254.169.123").split(",") if r]
EVERY = float(os.environ.get("EVERY", "10"))
CH = os.environ.get("CH_URL", "")
NODE = os.environ.get("NODE", socket.gethostname())


def sntp(host, timeout=2.0):
    pkt = bytearray(48)
    pkt[0] = 0x23  # LI 0, version 4, mode 3 (client)
    t0 = time.time()
    tx = t0 + NTP_EPOCH
    struct.pack_into("!II", pkt, 40, int(tx), int((tx % 1) * 2**32))
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        s.sendto(pkt, (host, 123))
        data, _ = s.recvfrom(512)
    t3 = time.time()
    stratum = data[1]
    ts = lambda o: struct.unpack_from("!I", data, o)[0] + struct.unpack_from("!I", data, o + 4)[0] / 2**32 - NTP_EPOCH
    t1, t2 = ts(32), ts(40)  # server receive, server transmit
    offset_ref_minus_local = ((t1 - t0) + (t2 - t3)) / 2
    return {"offset_ms": round(-offset_ref_minus_local * 1000, 3), "delay_ms": round(((t3 - t0) - (t2 - t1)) * 1000, 3),
            "stratum": stratum}


def ch_offset():
    t0 = time.time()
    body = urllib.request.urlopen(urllib.request.Request(CH + "/", data=b"SELECT toUnixTimestamp64Micro(now64(6))"), timeout=5).read()
    t1 = time.time()
    server = int(body.strip()) / 1e6
    return {"offset_ms": round(((t0 + t1) / 2 - server) * 1000, 3), "delay_ms": round((t1 - t0) * 1000, 3)}


while True:
    now = time.time()
    for ref in REFS:
        try:
            r = sntp(ref)
            r.update(ref=ref)
        except Exception as e:  # a lost packet is a sample lost, not a stop
            r = {"ref": ref, "error": str(e)[:120]}
        print(json.dumps(dict(t=round(now, 3), node=NODE, **r)), flush=True)
    if CH:
        try:
            r = ch_offset()
        except Exception as e:
            r = {"error": str(e)[:120]}
        print(json.dumps(dict(t=round(now, 3), node=NODE, ref="clickhouse", **r)), flush=True)
    if os.environ.get("ONCE"):
        sys.exit(0)
    time.sleep(max(0.0, EVERY - (time.time() - now)))
