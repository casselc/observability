// Run only by test/trace.test.js, in a child `node --test`: one tagged test per outcome.
import { test } from 'node:test'
import { covers } from '../trace.js'

test('fixture passes', (t) => { covers(t, 'P', 'H-2', 'CAST-1') })
test('fixture fails', (t) => { covers(t, 'DST', 'H-1'); throw new Error('planned') })
test('fixture skips', (t) => { covers(t, 'IT', 'H-6'); t.skip('planned') })
test('fixture untagged', () => {})
