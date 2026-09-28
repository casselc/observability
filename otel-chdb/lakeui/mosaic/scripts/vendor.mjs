// Builds vendor/ from the pinned node_modules (npm ci first) so the page
// loads nothing from a CDN:
//
//   vendor/mosaic.js                  vgplot + mosaic-core/sql/plot + Plot + d3
//                                     + the DuckDB-WASM browser API + flechette,
//                                     one ES module (esbuild, minified)
//   vendor/duckdb/                    DuckDB-WASM's eh worker and wasm
//   vendor/ext/v1.5.4/wasm_eh/        the parquet extension (only the "DuckDB
//                                     reads the URLs" mode needs it), fetched
//                                     once from extensions.duckdb.org and
//                                     checked against a pinned SHA-256
//   vendor/head-shim.js               ../../lake-ui/head-shim.js (the HEAD shim)
//   vendor/sizes.json                 raw and gzip sizes, per file and per package
//
// vendor/ is not committed (40 MB of wasm); CI and the e2e run this.
//
//   node scripts/vendor.mjs [--ext-cache DIR]
import { build } from 'esbuild'
import { createHash } from 'node:crypto'
import { copyFileSync, existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { gzipSync } from 'node:zlib'

const here = dirname(fileURLToPath(import.meta.url))
const root = join(here, '..')
const out = join(root, 'vendor')
const nm = join(root, 'node_modules')

// DuckDB-WASM 1.33.1-dev57.0 (the version mosaic-core 0.31.0 pins) is DuckDB v1.5.4.
export const DUCKDB_CORE = 'v1.5.4'
const EXT = {
  parquet: { sha256: '4845705bbd69fc9ad52878d96a505c73cae4a6c509822079cc2413e5eb437f95', bytes: 3218307 },
}

const argv = process.argv.slice(2)
const extCache = argv.includes('--ext-cache') ? argv[argv.indexOf('--ext-cache') + 1] : null

mkdirSync(join(out, 'duckdb'), { recursive: true })
const res = await build({
  entryPoints: [join(here, 'entry.js')], bundle: true, format: 'esm', platform: 'browser', target: 'es2022',
  minify: true, outfile: join(out, 'mosaic.js'), metafile: true, legalComments: 'eof', logLevel: 'warning',
})

const dd = join(nm, '@duckdb', 'duckdb-wasm', 'dist')
for (const f of ['duckdb-browser-eh.worker.js', 'duckdb-eh.wasm']) copyFileSync(join(dd, f), join(out, 'duckdb', f))
copyFileSync(join(root, '..', '..', 'lake-ui', 'head-shim.js'), join(out, 'head-shim.js'))

const extDir = join(out, 'ext', DUCKDB_CORE, 'wasm_eh')
mkdirSync(extDir, { recursive: true })
for (const [name, want] of Object.entries(EXT)) {
  const file = `${name}.duckdb_extension.wasm`
  const dst = join(extDir, file)
  let bytes = existsSync(dst) ? readFileSync(dst) : null
  if (!bytes && extCache && existsSync(join(extCache, file))) bytes = readFileSync(join(extCache, file))
  if (!bytes) {
    const url = `https://extensions.duckdb.org/${DUCKDB_CORE}/wasm_eh/${file}`
    const r = await fetch(url)
    if (!r.ok) throw new Error(`${url}: ${r.status}`)
    bytes = new Uint8Array(await r.arrayBuffer())
  }
  const got = createHash('sha256').update(bytes).digest('hex')
  if (got !== want.sha256) throw new Error(`${file}: sha256 ${got}, pinned ${want.sha256}`)
  writeFileSync(dst, bytes)
}

// sizes: what a browser downloads, raw and gzip (-6, what a static server sends)
const sizeOf = p => {
  const b = readFileSync(p)
  return { raw: b.length, gzip: gzipSync(b).length }
}
const files = {}
for (const f of ['mosaic.js', 'duckdb/duckdb-browser-eh.worker.js', 'duckdb/duckdb-eh.wasm', `ext/${DUCKDB_CORE}/wasm_eh/parquet.duckdb_extension.wasm`, 'head-shim.js']) {
  files[f] = sizeOf(join(out, f))
}
// the bundle by package (input bytes that survived into the output, minified)
const byPkg = {}
for (const [path, o] of Object.entries(res.metafile.outputs[Object.keys(res.metafile.outputs)[0]].inputs)) {
  const m = /node_modules\/((?:@[^/]+\/)?[^/]+)/.exec(path)
  const k = m ? m[1] : path
  byPkg[k] = (byPkg[k] ?? 0) + o.bytesInOutput
}
const sorted = Object.fromEntries(Object.entries(byPkg).sort((a, b) => b[1] - a[1]))
const versions = {}
for (const p of ['@uwdata/vgplot', '@uwdata/mosaic-core', '@uwdata/mosaic-sql', '@uwdata/mosaic-plot', '@uwdata/mosaic-inputs',
  '@uwdata/flechette', '@observablehq/plot', 'd3', '@duckdb/duckdb-wasm', 'apache-arrow', 'esbuild']) {
  versions[p] = JSON.parse(readFileSync(join(nm, p, 'package.json'), 'utf8')).version
}
const sizes = { versions, duckdb_core: DUCKDB_CORE, files, bundle_by_package_minified: sorted }
writeFileSync(join(out, 'sizes.json'), JSON.stringify(sizes, null, 1))
const total = Object.values(files).reduce((a, f) => ({ raw: a.raw + f.raw, gzip: a.gzip + f.gzip }), { raw: 0, gzip: 0 })
console.log(JSON.stringify({ files, total, top: Object.entries(sorted).slice(0, 12) }, null, 1))
void statSync
