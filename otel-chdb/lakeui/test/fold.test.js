// The term index's tokenizer must agree with the page's text search
// (queries.js: jsLower(Body).includes(jsLower(text))). The query service
// folds in Go (query/internal/lakeidx/fold.go) and needs no Unicode tables
// because only two non-ASCII code points lowercase to anything ASCII. This
// test checks that claim against THIS engine's toLowerCase, and that the
// shared vector file (Go's test reads it too) still says what this engine
// computes. Regenerate the file with LAKEIDX_WRITE_VECTORS=1 npm test.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import fc from 'fast-check'

const VECTORS = fileURLToPath(new URL('../../query/internal/lakeidx/testdata/fold_vectors.json', import.meta.url))

export const tokens = s => s.toLowerCase().split(/[^a-z0-9_]+/).filter(Boolean)

const CASES = [
  '', ' ', 'timeout', 'Timeout ACCT-7731 failed', 'GET /api/v1/users?id=42&x=Y', 'under_score and-dash', 'İSTANBUL İd',
  '\u212aelvin 5\u212a', 'café CAFÉ naïve', 'ΣΑΣ σας', 'emoji😀between', 'tab\tnew\nline\r\n', 'Ⅻ roman ⅻ', 'ﬁne ligature',
  'ǅ titlecase', 'ß STRASSE', 'full\uff21width', 'a\u0301 combining', '\u00a0nbsp\u2028ls', 'trailing sep ', ' leading', 'x',
  'ALLCAPS_WITH_123', '0xDEADBEEF', 'k8s.pod.name=api-7d9f', '\ufffd replacement', '日本語 text 中文', 'mixedİcase', 'zero\u200bwidth',
]

test('only U+0130 and U+212A lowercase to something with an ASCII character', () => {
  const found = []
  for (let cp = 0x80; cp <= 0x10ffff; cp++) {
    if (cp >= 0xd800 && cp <= 0xdfff) continue
    if (/[\x00-\x7f]/.test(String.fromCodePoint(cp).toLowerCase())) found.push(cp)
  }
  assert.deepEqual(found, [0x130, 0x212a])
  assert.equal('İ'.toLowerCase(), 'i\u0307')
  assert.equal('\u212a'.toLowerCase(), 'k')
})

test('the shared vector file is what this engine computes', () => {
  const want = { comment: 'tokens = s.toLowerCase().split(/[^a-z0-9_]+/) in Node (V8); written by lakeui/test/fold.test.js; Go lakeidx.Tokens must agree',
    cases: CASES.map(text => ({ text, tokens: tokens(text) })) }
  if (process.env.LAKEIDX_WRITE_VECTORS) writeFileSync(VECTORS, JSON.stringify(want, null, 1) + '\n')
  assert.deepEqual(JSON.parse(readFileSync(VECTORS, 'utf8')), want)
})

// the constraint rule (lakeidx.Constraints), restated: a body that contains
// the text holds, for every run of the folded text, a token that matches
// the run as full / prefix / suffix / infix
function constraints(text) {
  const f = text.toLowerCase().replace(/[^a-z0-9_]/g, ' ')
  const runs = [...f.matchAll(/[a-z0-9_]+/g)].map(m => ({ t: m[0], a: m.index, b: m.index + m[0].length }))
  return runs.map((r, i) => {
    const open0 = i === 0 && r.a === 0
    const open1 = i === runs.length - 1 && r.b === f.length
    const m = open0 && open1 ? 'infix' : open0 ? 'suffix' : open1 ? 'prefix' : 'full'
    return { t: r.t, m }
  })
}
const holds = (c, tok) => c.m === 'full' ? tok === c.t : c.m === 'prefix' ? tok.startsWith(c.t) : c.m === 'suffix' ? tok.endsWith(c.t) : tok.includes(c.t)

test('property: every body the page matches satisfies every constraint of the text', () => {
  const ch = fc.constantFrom(...'aZ9_ -.:/', 'İ', '\u212a', 'É', 'é', '😀', 'Σ', 'ß', '\u0307', 'I', 'K')
  fc.assert(fc.property(fc.array(ch, { maxLength: 40 }), fc.nat(), fc.nat(), (chars, i, j) => {
    const body = chars.join('')
    const a = Math.min(i % (body.length + 1), j % (body.length + 1))
    const b = Math.max(i % (body.length + 1), j % (body.length + 1))
    const text = body.slice(a, b)
    if (!body.toLowerCase().includes(text.toLowerCase())) return true // a split surrogate or a context-dependent sigma
    const toks = tokens(body)
    for (const c of constraints(text)) assert.ok(toks.some(t => holds(c, t)), JSON.stringify({ body, text, c, toks }))
    return true
  }), { numRuns: 3000 })
})
