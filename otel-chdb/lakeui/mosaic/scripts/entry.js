// The one browser bundle of the spike (vendor/mosaic.js): vgplot (with
// mosaic-core, mosaic-sql, mosaic-plot, Observable Plot and d3), the
// DuckDB-WASM browser API and flechette (Arrow IPC). Built by
// scripts/vendor.mjs from the pinned node_modules; nothing is fetched from a
// CDN at runtime.
export * as vg from '@uwdata/vgplot'
export * as duckdb from '@duckdb/duckdb-wasm'
export * as flechette from '@uwdata/flechette'
