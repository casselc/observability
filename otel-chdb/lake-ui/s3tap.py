#!/usr/bin/env python3
"""A logging pass-through in front of the S3 endpoint, for counting what the
browser really fetches (method, Range, status, body bytes). Presign against
this port: SigV4 signs the Host header, and it is forwarded unchanged.
  s3tap.py LISTEN_PORT UPSTREAM_HOST:PORT LOG_FILE"""
import http.client, http.server, json, sys, time

PORT, UP, LOG = int(sys.argv[1]), sys.argv[2], sys.argv[3]


class T(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _do(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else None
        c = http.client.HTTPConnection(UP, timeout=60)
        c.request(self.command, self.path, body, dict(self.headers))
        r = c.getresponse()
        data = r.read()
        self.send_response(r.status)
        for k, v in r.getheaders():
            if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)
        with open(LOG, "a") as f:
            f.write(json.dumps({"t": time.time(), "m": self.command, "range": self.headers.get("Range"),
                                "status": r.status, "bytes": len(data) if self.command != "HEAD" else 0,
                                "key": self.path.split("?")[0]}) + "\n")

    do_GET = do_HEAD = do_PUT = do_OPTIONS = do_POST = do_DELETE = _do

    def log_message(self, *a):
        pass


http.server.ThreadingHTTPServer(("127.0.0.1", PORT), T).serve_forever()
