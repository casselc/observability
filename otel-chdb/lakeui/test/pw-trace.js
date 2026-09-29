// Runtime traceability tags for Playwright tests (the node:test helper is
// test/trace.js; the Go twin otel-chdb/testgate/tracetag; ci/trace/trace.py
// joins the records, ci/README.md "Traceability").
//
//   test('stale cubes: ...', async ({ browser }) => {
//     const t = test.info()
//     covers(t, 'IT', 'CAST-33', 'H-5')
//
// The tag is an annotation on the running test; test/pw-trace-reporter.js
// (a Playwright reporter the config lists) writes one record per tagged test
// when it ends, with the outcome Playwright gives it. No reporter, no record:
// the traceability job reports the tag as a test that did not run.
export const ANNOTATION = 'oscope-covers'

export function covers (t, technique, ...ids) {
  t.annotations.push({ type: ANNOTATION, description: JSON.stringify({ technique, ids }) })
}
