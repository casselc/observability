// Global setup for the journeys: brings up the lake UI rig once, exactly as
// ../lakeui.spec.mjs does (query/integration/lakeuirig: its own bucket and
// database, the Go edge, the Rust consumer, the query service, the IdP
// front, the counting pass-through, the page), and hands its
// LAKEUI_RIG_READY document to the tests in JNY_RIG. The returned function
// is the teardown: SIGTERM, on which the rig removes what it created.
import { spawn } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const queryDir = join(here, '..', '..', '..', 'query')

export default async function rigSetup() {
  const cmd = process.env.LAKEUI_RIG_BIN ? [process.env.LAKEUI_RIG_BIN, []] : ['go', ['run', './integration/lakeuirig']]
  // LUI_RICH_SPANS: span durations with a tail and spans in the late batch,
  // for the Mosaic spike's latency chart (journey 6); the same rig for all
  const env = { ...process.env, LUI_PREFIX: process.env.LUI_PREFIX || 'jny', LUI_RICH_SPANS: '1' }
  const rig = spawn(cmd[0], cmd[1], { cwd: queryDir, stdio: ['ignore', 'pipe', 'pipe'], env })
  const lines = []
  rig.stderr.on('data', d => lines.push(String(d)))
  const info = await new Promise((resolve, reject) => {
    let buf = ''
    const timer = setTimeout(() => {
      rig.kill('SIGTERM') // not left running (and holding its bucket) when setup gives up
      reject(new Error('the rig did not get ready in 300 s (a read-only SeaweedFS, e.g. disk below its minFreeSpace, hangs the edges):\n' + lines.join('')))
    }, 300_000)
    rig.stdout.on('data', d => {
      buf += d
      const m = /LAKEUI_RIG_READY (.*)\n/.exec(buf)
      if (m) {
        clearTimeout(timer)
        resolve(JSON.parse(m[1]))
      }
    })
    rig.on('exit', c => { clearTimeout(timer); reject(new Error(`the rig exited ${c}:\n` + lines.join(''))) })
  })
  process.env.JNY_RIG = JSON.stringify(info)
  return async () => {
    if (rig.exitCode === null) {
      const done = new Promise(r => rig.on('exit', r))
      rig.kill('SIGTERM')
      await done
    }
  }
}
