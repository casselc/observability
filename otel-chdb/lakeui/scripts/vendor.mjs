// Copies the pinned browser dependencies out of node_modules into vendor/,
// so the page loads nothing from a CDN (research/lake-ui.md §2.2: the
// air-gapped mode is the default). `--check` fails if vendor/ differs from
// what package-lock.json pins (CI runs it).
import { createHash } from 'node:crypto'
import { cpSync, existsSync, readdirSync, readFileSync, rmSync, statSync } from 'node:fs'
import { dirname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const nm = join(root, 'node_modules')
const out = join(root, 'vendor')
const pkgs = [
  { name: 'hyparquet', files: ['src', 'LICENSE', 'package.json'] },
  { name: 'fzstd', files: ['esm', 'LICENSE', 'package.json'] },
]

function walk(dir) {
  const acc = []
  for (const e of readdirSync(dir)) {
    const p = join(dir, e)
    if (statSync(p).isDirectory()) acc.push(...walk(p))
    else acc.push(p)
  }
  return acc
}

const digest = dir => {
  const h = createHash('sha256')
  for (const f of walk(dir).filter(f => !f.endsWith('.d.ts')).sort()) {
    h.update(relative(dir, f))
    h.update(readFileSync(f))
  }
  return h.digest('hex')
}

const check = process.argv.includes('--check')
let bad = 0
for (const p of pkgs) {
  const src = join(nm, p.name)
  const dst = join(out, p.name)
  if (!existsSync(src)) throw new Error(`${p.name} is not installed: npm ci first`)
  if (check) {
    const tmp = join(root, '.vendor-check', p.name)
    rmSync(tmp, { recursive: true, force: true })
    for (const f of p.files) cpSync(join(src, f), join(tmp, f), { recursive: true, filter: s => !s.endsWith('.d.ts') })
    const same = existsSync(dst) && digest(tmp) === digest(dst)
    rmSync(join(root, '.vendor-check'), { recursive: true, force: true })
    if (!same) {
      console.error(`vendor/${p.name} differs from node_modules/${p.name}: run npm run vendor`)
      bad++
    }
    continue
  }
  rmSync(dst, { recursive: true, force: true })
  for (const f of p.files) cpSync(join(src, f), join(dst, f), { recursive: true, filter: s => !s.endsWith('.d.ts') })
  const v = JSON.parse(readFileSync(join(src, 'package.json'), 'utf8')).version
  console.log(`vendor/${p.name} ${v}`)
}
if (bad) process.exit(1)
