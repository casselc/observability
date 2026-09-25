"""Drop fastpath_test and delete every object under s3://otel/fastpath/.
A minimal SigV4 client, so no SDK is needed."""
import datetime, hashlib, hmac, re, urllib.parse
import requests
from fp import q, DB

ENDPOINT, BUCKET, REGION = "http://127.0.0.1:18333", "otel", "us-east-1"
AK, SK = "otel", "otelsecret"


def _sign(method, path, query=""):
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    host = urllib.parse.urlparse(ENDPOINT).netloc
    ph = hashlib.sha256(b"").hexdigest()
    canon = "\n".join([method, path, query, f"host:{host}\nx-amz-content-sha256:{ph}\nx-amz-date:{amz}\n",
                       "host;x-amz-content-sha256;x-amz-date", ph])
    scope = f"{day}/{REGION}/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canon.encode()).hexdigest()])
    k = ("AWS4" + SK).encode()
    for part in (day, REGION, "s3", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    return {"x-amz-date": amz, "x-amz-content-sha256": ph,
            "Authorization": f"AWS4-HMAC-SHA256 Credential={AK}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"}


def list_keys(prefix):
    qs = "list-type=2&prefix=" + urllib.parse.quote(prefix, safe="")
    r = requests.get(f"{ENDPOINT}/{BUCKET}?{qs}", headers=_sign("GET", f"/{BUCKET}", qs), timeout=30)
    r.raise_for_status()
    return re.findall(r"<Key>([^<]+)</Key>", r.text)


def delete(key):
    path = f"/{BUCKET}/" + urllib.parse.quote(key)
    r = requests.delete(ENDPOINT + path, headers=_sign("DELETE", path), timeout=30)
    return r.status_code


if __name__ == "__main__":
    q(f"DROP DATABASE IF EXISTS {DB} SYNC")
    keys = list_keys("fastpath/")
    print("deleting", len(keys), "objects:", {k: delete(k) for k in keys})
    print("left:", list_keys("fastpath/"))
