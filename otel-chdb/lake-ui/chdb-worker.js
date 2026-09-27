// chdb-wasm's worker with the HEAD shim installed first (static imports
// evaluate in order). Served next to /vendor/chdb/ (the package's dist/).
import './head-shim.js';
import '/vendor/chdb/worker.js';
