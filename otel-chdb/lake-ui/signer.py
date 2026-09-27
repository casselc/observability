#!/usr/bin/env python3
"""Tiny SigV4 presigner for the lake-ui spike (stdlib only).

The browser page never sees keys: it asks this process for presigned GET
URLs (`GET /presign?key=...`), the way a query service would hand out
per-role, short-lived URLs after resolving scope. Keys come from the
environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY). It also serves the
static page with COOP/COEP headers, and has CLI helpers to list a prefix and
set bucket CORS.

  signer.py serve  [port]          # static page + /plan (the share endpoint)
  signer.py mint   sub cluster,...  # a dev "IdP": an HS256 JWT with a clusters claim

/plan is the share endpoint: with LAKEUI_JWT_SECRET set it requires
`Authorization: Bearer <jwt>` (HS256 here, standing in for the IdP's RS256 +
JWKS), refuses a scope outside the token's `clusters` claim, filters the object
list to it, and writes one audit line per request (LAKEUI_AUDIT, JSON lines).
  signer.py list   bucket prefix
  signer.py cors   bucket origin
"""
import datetime, hashlib, hmac, http.server, json, os, sys, urllib.parse, urllib.request
import xml.etree.ElementTree as ET

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:18333")
REGION = os.environ.get("AWS_REGION", "us-east-1")
AK = os.environ["AWS_ACCESS_KEY_ID"]
SK = os.environ["AWS_SECRET_ACCESS_KEY"]
HOST = urllib.parse.urlparse(ENDPOINT).netloc


def _q(s, safe="-_.~"):
    return urllib.parse.quote(s, safe=safe)


def _key(date):
    k = hmac.new(("AWS4" + SK).encode(), date.encode(), hashlib.sha256).digest()
    for part in (REGION, "s3", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    return k


def presign(method, path, query=None, expires=300):
    """Query-string SigV4 (path-style URL). path = /bucket/key."""
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, date = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    scope = f"{date}/{REGION}/s3/aws4_request"
    q = dict(query or {})
    q.update({"X-Amz-Algorithm": "AWS4-HMAC-SHA256", "X-Amz-Credential": f"{AK}/{scope}",
              "X-Amz-Date": amz, "X-Amz-Expires": str(expires), "X-Amz-SignedHeaders": "host"})
    cq = "&".join(f"{_q(k)}={_q(v)}" for k, v in sorted(q.items()))
    creq = "\n".join([method, _q(path, "/-_.~"), cq, f"host:{HOST}\n", "host", "UNSIGNED-PAYLOAD"])
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(creq.encode()).hexdigest()])
    sig = hmac.new(_key(date), sts.encode(), hashlib.sha256).hexdigest()
    return f"{ENDPOINT}{_q(path, '/-_.~')}?{cq}&X-Amz-Signature={sig}"


def s3(method, path, query=None, body=None, headers=None):
    req = urllib.request.Request(presign(method, path, query), data=body, method=method,
                                 headers=headers or {})
    with urllib.request.urlopen(req) as r:
        return r.read()


def list_prefix(bucket, prefix, limit=1000):
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    x = ET.fromstring(s3("GET", f"/{bucket}", {"list-type": "2", "prefix": prefix,
                                               "max-keys": str(limit)}))
    return [(c.find(ns + "Key").text, int(c.find(ns + "Size").text)) for c in x.iter(ns + "Contents")]


def set_cors(bucket, origin):
    body = (f"<CORSConfiguration><CORSRule><AllowedOrigin>{origin}</AllowedOrigin>"
            "<AllowedMethod>GET</AllowedMethod><AllowedMethod>HEAD</AllowedMethod>"
            "<AllowedHeader>*</AllowedHeader><ExposeHeader>Content-Range</ExposeHeader>"
            "<ExposeHeader>Content-Length</ExposeHeader><ExposeHeader>ETag</ExposeHeader>"
            "<MaxAgeSeconds>3600</MaxAgeSeconds></CORSRule></CORSConfiguration>").encode()
    md5 = __import__("base64").b64encode(hashlib.md5(body).digest()).decode()
    return s3("PUT", f"/{bucket}", {"cors": ""}, body, {"Content-MD5": md5})


_CAT = {}
JWT_SECRET = os.environ.get("LAKEUI_JWT_SECRET")


def _b64(b):
    return __import__("base64").urlsafe_b64encode(b).rstrip(b"=").decode()


def _unb64(s):
    return __import__("base64").urlsafe_b64decode(s + "=" * (-len(s) % 4))


def mint(sub, clusters, ttl=900):
    h = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    import time
    c = _b64(json.dumps({"sub": sub, "aud": "lake-ui", "clusters": clusters,
                         "exp": int(time.time()) + ttl}).encode())
    sig = hmac.new(JWT_SECRET.encode(), f"{h}.{c}".encode(), hashlib.sha256).digest()
    return f"{h}.{c}.{_b64(sig)}"


def verify(auth):
    """-> claims, or raise PermissionError. Checks signature, aud and exp."""
    import time
    if not auth or not auth.startswith("Bearer "):
        raise PermissionError("no bearer token")
    h, c, sig = auth[7:].split(".")
    want = hmac.new(JWT_SECRET.encode(), f"{h}.{c}".encode(), hashlib.sha256).digest()
    if json.loads(_unb64(h)).get("alg") != "HS256" or not hmac.compare_digest(want, _unb64(sig)):
        raise PermissionError("bad signature")
    claims = json.loads(_unb64(c))
    if claims.get("aud") != "lake-ui" or claims.get("exp", 0) < time.time():
        raise PermissionError("wrong audience or expired")
    return claims


def audit(rec):
    rec["t"] = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")
    with open(os.environ.get("LAKEUI_AUDIT", "audit.jsonl"), "a") as f:
        f.write(json.dumps(rec) + "\n")


def catalog():
    """Snapshot manifest + trace maplet + a service -> cluster map: the three
    things the query service knows before any data read (gen.sh writes them;
    in the design they come from the sealer's snapshot, lake P2 and the
    entity catalog)."""
    if not _CAT:
        d = os.environ.get("LAKEUI_DATA", ".")
        man = [json.loads(l) for l in open(os.path.join(d, "manifest.jsonl"))]
        man += [{"path": k, "bytes": n, "signal": k.split("/")[1], "cluster": "cluster-%d" % (int(k.split("/")[0][4:]) // 2),
                 "services": [], "ts_min": 0, "ts_max": 2**62} for k, n in list_prefix("lakeui", "")
                if "/metrics_gauge/" in k]
        svc = {}
        for m in man:
            for sv in m["services"]:
                svc[sv] = m["cluster"]
        _CAT.update(manifest=man, service_cluster=svc,
                    maplet={j["TraceId"]: j["paths"] for j in map(json.loads, open(os.path.join(d, "maplet.jsonl")))})
    return _CAT


def plan(q, claims=None):
    """Resolve scope -> object list, then presign only those objects. With
    claims, the viewer's clusters bound the list before anything else."""
    c = catalog()
    sig = q.get("signal", ["traces"])[0]
    objs = [m for m in c["manifest"] if m["signal"] == sig]
    steps = {"snapshot objects": len(objs)}
    if claims is not None:
        allowed = set(claims.get("clusters", []))
        if "service" in q and c["service_cluster"].get(q["service"][0]) not in allowed:
            raise PermissionError("service outside the token's clusters")
        objs = [m for m in objs if m["cluster"] in allowed]
        steps["after token clusters %s" % sorted(allowed)] = len(objs)
    if "trace" in q and q.get("maplet", ["1"])[0] == "1":
        hit = set(c["maplet"].get(q["trace"][0], []))
        objs = [m for m in objs if m["path"] in hit]
        steps["after maplet"] = len(objs)
    if "service" in q:
        cl = c["service_cluster"].get(q["service"][0])
        objs = [m for m in objs if m["cluster"] == cl]
        steps["after entity catalog (cluster %s)" % cl] = len(objs)
    if "t0" in q:
        t0, t1 = int(q["t0"][0]), int(q["t1"][0])
        objs = [m for m in objs if m["ts_max"] >= t0 and m["ts_min"] < t1]
        steps["after zone map (time)"] = len(objs)
    return {"steps": steps, "bytes": sum(m["bytes"] for m in objs), "sizes": [m["bytes"] for m in objs],
            "urls": [presign("GET", "/lakeui/" + m["path"]) for m in objs]}


class H(http.server.SimpleHTTPRequestHandler):
    def translate_path(self, path):
        v = os.environ.get("LAKEUI_VENDOR")
        if v and path.startswith("/vendor/"):
            return os.path.join(v, urllib.parse.urlparse(path).path[len("/vendor/"):])
        return super().translate_path(path)

    def guess_type(self, path):
        return "application/wasm" if str(path).endswith(".wasm") else (
            "text/javascript" if str(path).endswith((".mjs", ".js")) else super().guess_type(path))

    def end_headers(self):
        # cross-origin isolation, so DuckDB-WASM / chdb-wasm can use threads
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
        self.send_header("Cross-Origin-Resource-Policy", "cross-origin")
        super().end_headers()

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if u.path == "/plan":
            claims = None
            try:
                if JWT_SECRET:
                    claims = verify(self.headers.get("Authorization"))
                out = plan(q, claims)
            except PermissionError as e:
                audit({"sub": (claims or {}).get("sub"), "q": u.query, "decision": "deny", "why": str(e)})
                b = str(e).encode()
                self.send_response(401 if claims is None and JWT_SECRET else 403)
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            audit({"sub": (claims or {}).get("sub"), "q": u.query, "decision": "allow",
                   "objects": len(out["urls"]), "bytes": out["bytes"]})
            b = json.dumps(out).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return
        super().do_GET()

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "serve":
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[2]) if len(sys.argv) > 2 else 18190),
                                        H).serve_forever()
    elif cmd == "list":
        for k, n in list_prefix(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ""):
            print(n, k)
    elif cmd == "cors":
        print(set_cors(sys.argv[2], sys.argv[3]) or "ok")
    elif cmd == "mint":
        print(mint(sys.argv[2], sys.argv[3].split(",")))
    elif cmd == "presign":
        print(presign("GET", sys.argv[2]))
