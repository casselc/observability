#!/usr/bin/env python3
"""A fake Prometheus /api/v1/query for testing collect.py: answers from
fixtures.json by the first fixture whose "match" is a substring of the query.
Variants are recognisable from the query text: max_over_time(...) is the peak
(×1.6), avg_over_time(...) or a [1d] rate window the average (×0.9).

    testdata/fake_prom.py PORT
"""
import json
import os
import sys
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

FIX = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures.json")))
LOG = []


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        q = urllib.parse.parse_qs(self.rfile.read(n).decode()).get("query", [""])[0]
        self.answer(q)

    def do_GET(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("query", [""])[0]
        self.answer(q)

    def answer(self, q):
        if not self.path.startswith("/api/v1/query"):
            self.send_error(404)
            return
        if q.count("(") != q.count(")") or q.count("{") != q.count("}") or "{," in q:
            body = {"status": "error", "errorType": "bad_data", "error": "parse error: " + q[:80]}
            self.reply(400, body)
            return
        mult = 1.6 if q.startswith("max_over_time") else (0.9 if q.startswith("avg_over_time") or "[1d]" in q else 1.0)
        res = []
        for f in FIX:
            if f["match"] in q:
                if "vector" in f:
                    res = [{"metric": s["labels"], "value": [time.time(), str(s["value"] * mult)]} for s in f["vector"]]
                elif f.get("value") is not None:
                    res = [{"metric": {}, "value": [time.time(), str(f["value"] * mult)]}]
                break
        self.reply(200, {"status": "success", "data": {"resultType": "vector", "result": res}})

    def reply(self, code, body):
        b = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
