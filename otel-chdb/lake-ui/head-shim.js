// A worker-side shim for presigned object URLs. A SigV4 query-string
// signature binds the HTTP method, so a URL presigned for GET answers HEAD
// with 403 (SeaweedFS and AWS alike). Browser engines HEAD a file for its size
// before range-reading it (chdb-wasm's url(), DuckDB-WASM's HTTP reader), and
// fail or fall back to whole-object reads. This turns such a HEAD into
// GET Range: bytes=0-0 and reports the size from Content-Range.
// Loaded first by chdb-worker.js (chdb-wasm's workerUrl), so it is in place
// before chdb's own worker module evaluates.
export function installHeadShim(scope = self) {
  const X = scope.XMLHttpRequest;
  if (!X || X.__headShim) return;
  class H extends X {
    open(m, u, ...a) {
      this.__h = String(m).toUpperCase() === 'HEAD' && /X-Amz-Signature=/.test(String(u));
      return super.open(this.__h ? 'GET' : m, u, ...a);
    }
    setRequestHeader(k, v) {
      if (this.__h && k.toLowerCase() === 'range') { this.__r = 1; v = 'bytes=0-0'; }
      return super.setRequestHeader(k, v);
    }
    send(b) { if (this.__h && !this.__r) super.setRequestHeader('Range', 'bytes=0-0'); return super.send(b); }
    get status() { const s = super.status; return this.__h && s === 206 ? 200 : s; }
    get response() { return this.__h ? new ArrayBuffer(0) : super.response; }
    getResponseHeader(k) {
      const cr = this.__h && super.getResponseHeader('Content-Range');
      if (cr && k.toLowerCase() === 'content-length') return cr.split('/')[1];
      if (cr && k.toLowerCase() === 'content-range') return null;
      return super.getResponseHeader(k);
    }
    getAllResponseHeaders() {
      const all = super.getAllResponseHeaders();
      const cr = this.__h && super.getResponseHeader('Content-Range');
      if (!cr) return all;
      return all.split('\r\n').filter((l) => !/^content-(length|range):/i.test(l)).join('\r\n')
        + `content-length: ${cr.split('/')[1]}\r\n`;
    }
  }
  H.__headShim = true;
  scope.XMLHttpRequest = H;
}
if (typeof WorkerGlobalScope !== 'undefined' && self instanceof WorkerGlobalScope) installHeadShim(self);
