#!/usr/bin/env python3
"""Tiny authoritative DNS server for the GOAWAY e2e test: answers A queries for
NAME with the addresses listed (one per line) in FILE, re-read per query, TTL 1.
Everything else gets NXDOMAIN. Stands in for a headless Service's endpoints.
  dns.py 127.0.0.1 15353 ga-pubs.test addrs.txt"""
import socket, struct, sys
host, port, name, path = sys.argv[1], int(sys.argv[2]), sys.argv[3].lower().rstrip('.'), sys.argv[4]
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.bind((host, port))
log = open(path + '.log', 'a', buffering=1)
while True:
    data, peer = s.recvfrom(512)
    if len(data) < 12: continue
    qid, flags, qd = struct.unpack('>HHH', data[:6])
    i, labels = 12, []
    while data[i]:
        labels.append(data[i+1:i+1+data[i]].decode()); i += 1 + data[i]
    qend = i + 5
    qtype = struct.unpack('>H', data[i+1:i+3])[0]
    q = '.'.join(labels).lower()
    try: addrs = [l.strip() for l in open(path) if l.strip()]
    except OSError: addrs = []
    ans = b''
    rcode = 0
    if q == name:
        if qtype == 1:
            for a in addrs:
                ans += struct.pack('>HHHIH', 0xC00C, 1, 1, 1, 4) + socket.inet_aton(a)
    else:
        rcode = 3
    n = len(ans) // 16
    hdr = struct.pack('>HHHHHH', qid, 0x8400 | (flags & 0x0100) | rcode, 1, n, 0, 0)
    s.sendto(hdr + data[12:qend] + ans, peer)
    import time; log.write(f"{time.time():.1f} {q} type={qtype} -> {addrs if q == name and qtype == 1 else rcode}\n")
