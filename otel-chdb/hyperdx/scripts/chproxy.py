#!/usr/bin/env python3
"""A logging HTTP proxy between HyperDX and ClickHouse, standing in for
system.query_log (the shared server runs without a config file, so it has
none). HyperDX's connection points here; every request is forwarded to
ClickHouse unchanged and one JSON line is appended to the log:

  {"t": start epoch s, "ms": wall ms, "status": 200, "query": "...", "params": {...},
   "settings": {...}, "summary": X-ClickHouse-Summary, "error": first 2 kB of an error body,
   "bytes": response bytes, "tag": the current tag}

The tag is whatever was last PUT to /__tag (the UI driver tags each scenario).

  chproxy.py [--listen 127.0.0.1:18124] [--ch 127.0.0.1:18123] [--log FILE]
"""
import argparse, http.client, json, threading, time, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ap = argparse.ArgumentParser()
ap.add_argument("--listen", default="127.0.0.1:18124")
ap.add_argument("--ch", default="127.0.0.1:18123")
ap.add_argument("--log", default="chproxy.jsonl")
a = ap.parse_args()
lock = threading.Lock()
state = {"tag": ""}
out = open(a.log, "a", buffering=1)
HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "proxy-authorization", "proxy-authenticate"}


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def handle_one(self, method):
        if "chunked" in (self.headers.get("transfer-encoding") or "").lower():
            # the Node client (HyperDX's API and alert task) streams request bodies
            body = b""
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    break
                body += self.rfile.read(size)
                self.rfile.readline()
        else:
            n = int(self.headers.get("content-length") or 0)
            body = self.rfile.read(n) if n else b""
        if self.path.startswith("/__tag"):
            state["tag"] = body.decode()
            self.send_response(204)
            self.send_header("content-length", "0")
            self.end_headers()
            return
        u = urllib.parse.urlsplit(self.path)
        qs = urllib.parse.parse_qs(u.query)
        query = qs.get("query", [""])[0]
        if body and method == "POST":
            query = (query + "\n" + body.decode(errors="replace")) if query else body.decode(errors="replace")
        params = {k[6:]: v[0] for k, v in qs.items() if k.startswith("param_")}
        settings = {k: v[0] for k, v in qs.items() if k not in ("query", "database", "query_id", "default_format") and not k.startswith("param_")}
        t0 = time.time()
        c = http.client.HTTPConnection(*a.ch.split(":"), timeout=600)
        hdrs = {k: v for k, v in self.headers.items() if k.lower() not in HOP and k.lower() not in ("host", "accept-encoding")}
        # uncompressed responses, so that error bodies are readable in the log
        path = self.path.replace("enable_http_compression=1", "enable_http_compression=0")
        c.request(method, path, body=body, headers=hdrs)
        r = c.getresponse()
        data = r.read()
        ms = (time.time() - t0) * 1000
        self.send_response(r.status)
        for k, v in r.getheaders():
            if k.lower() not in HOP and k.lower() != "content-length":
                self.send_header(k, v)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        c.close()
        if query.strip():
            rec = {"t": round(t0, 3), "ms": round(ms, 1), "status": r.status, "tag": state["tag"], "query": query,
                   "params": params, "settings": settings, "summary": r.getheader("X-ClickHouse-Summary"),
                   "bytes": len(data), "ua": self.headers.get("user-agent", "")}
            ex = r.getheader("X-ClickHouse-Exception-Code")
            if r.status >= 400 or ex:
                rec["error"] = data[:2000].decode(errors="replace")
            with lock:
                out.write(json.dumps(rec) + "\n")

    def do_GET(self):
        self.handle_one("GET")

    def do_POST(self):
        self.handle_one("POST")

    def do_PUT(self):
        self.handle_one("PUT")


host, port = a.listen.split(":")
ThreadingHTTPServer((host, int(port)), H).serve_forever()
