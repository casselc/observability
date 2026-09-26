#!/usr/bin/env python3
# webhook sink for HyperDX alerts: appends each POST body to hooks.jsonl
import json, time
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        b = self.rfile.read(int(self.headers.get('content-length') or 0)).decode(errors='replace')
        open('hooks.jsonl', 'a').write(json.dumps({'t': time.time(), 'path': self.path, 'body': b}) + '\n')
        self.send_response(200); self.send_header('content-length', '0'); self.end_headers()
    def log_message(self, *a): pass
HTTPServer(('127.0.0.1', 18890), H).serve_forever()
