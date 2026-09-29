"""Tests of ci/trace/trace.py against the repository's own catalogue and tags.

    python3 -m unittest -v ci/trace/trace_test.py

The traceability job runs these first: a check that cannot fail is no check.
"""
import argparse
import contextlib
import io
import json
import os
import tempfile
import unittest

import importlib.util

# ci/trace/trace.py by path: `import trace` would find the standard library's module.
_spec = importlib.util.spec_from_file_location('oscope_trace_py', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'trace.py'))
tr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tr)

CI_JOBS = 'go-vet,go-test,lakeui,lakeui-mosaic,rust'
NIGHTLY_JOBS = 'conformance,dst,hegel,model,chdb'
SHA = 'c0ffee'


def passing(tags, wf):
    recs = []
    for t in tags:
        job = next((j.split(':', 1)[1] for j in t['jobs'] if j.startswith(wf + ':')), None)
        if job is None:
            continue
        recs.append({'ids': t['ids'], 'technique': t['technique'], 'test': t['func'], 'func': t['func'],
                     'file': t['file'], 'commit': SHA, 'job': job, 'outcome': 'passed'})
    return recs


def run_check(recs, wf='ci', jobs=CI_JOBS):
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, 'r.jsonl')
        with open(p, 'w') as f:
            for r in recs:
                f.write(json.dumps(r) + '\n')
        a = argparse.Namespace(records=[d], workflow=wf, jobs=jobs, commit=SHA, report=os.path.join(d, 'T.md'), summary=None)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = tr.check(a)
        report = '\n'.join(tr.lines_of(a.report))
    return rc, out.getvalue(), report


class Catalogue(unittest.TestCase):
    def test_the_catalogue_parses_and_has_no_conflicting_ids(self):
        ids, dups, _ = tr.build_catalog()
        self.assertEqual(tr.conflicting(dups, tr.load_same_meaning()), [])
        for i in ['L-1', 'H-7', 'SC-7', 'UCA-16', 'LS-10', 'SEC-8', 'TM-8', 'R-S10', 'H-L8', 'R-L12', 'H-E9', 'R-E8',
                  'UCA-E11', 'H-G8', 'R-G1', 'CAST-1', 'CAST-49', 'CAST-55']:
            self.assertIn(i, ids)
        self.assertEqual(ids['CAST-52']['kind'], 'CAST row')
        self.assertIn('H-6', ids['CAST-52']['cites'])

    def test_both_sources_give_the_same_ids(self):
        """The records (otel-chdb/stpa) and the tables generated from them name the same IDs,
        including the labels that moved (UCA-10 and UCA-12 are loss scenarios now, D39) and
        those still defined only in the research notes' hand-kept tables."""
        md, _, _ = tr.markdown_catalog()
        rec, rdups, _ = tr.records_catalog()
        self.assertEqual(sorted(md), sorted(rec))
        self.assertEqual(rdups, [])
        for moved, now in (('UCA-10', 'LS-6'), ('UCA-12', 'LS-8')):
            self.assertIn(moved, rec)
            self.assertIn(now, rec[moved]['cites'])
        self.assertIn('UCA-E11', rec)

    def test_a_restated_id_with_another_meaning_fails(self):
        a = {'id': 'H-1', 'title': 'Acknowledged telemetry is in no store', 'source': 'otel-chdb/STPA.md:1'}
        b = {'id': 'H-1', 'title': 'Something else entirely', 'source': 'otel-chdb/research/grants.md:9'}
        self.assertEqual(len(tr.conflicting([(a, b)], [])), 1)
        self.assertEqual(tr.conflicting([(a, dict(b, title='Acknowledged  telemetry is in no store.'))], []), [])
        self.assertEqual(tr.conflicting([(a, b)], [['H-*', 'otel-chdb/STPA.md', 'otel-chdb/research/grants.md', 'r']]), [])

    def test_techniques_and_required_cells(self):
        tech = tr.techniques()
        for c in ['M', 'MN', 'P2C', 'DST', 'K', 'G', 'MU', 'IT']:
            self.assertIn(c, tech)
        cells = tr.required_cells(tech)
        self.assertIn('H-1:ML', cells)
        self.assertIn('H-3:DST|FI', cells)  # "DST / FI": one cell, either
        self.assertIn('H-6:FZ', cells)
        self.assertFalse(any(c.endswith(':I') for c in cells), 'class letters are not techniques')


class Check(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tags = tr.scan_tags()

    def test_every_tag_names_known_ids_and_a_job(self):
        ids, _, _ = tr.build_catalog()
        tech = tr.techniques()
        for t in self.tags:
            self.assertTrue(t['jobs'], t['key'])
            self.assertTrue(t['ids'], t['key'])
            self.assertEqual([x for x in t['ids'] if x not in ids], [], t['key'])
            self.assertEqual([x for x in tr.tech_list(t['technique']) if x not in tech], [], t['key'])

    def test_all_passing_is_green(self):
        rc, out, report = run_check(passing(self.tags, 'ci'))
        self.assertEqual(rc, 0, out[-3000:])
        self.assertIn('**Verdict: PASS**', report)

    def test_the_nightly_with_its_jobs_is_green(self):
        rc, out, _ = run_check(passing(self.tags, 'nightly'), 'nightly', NIGHTLY_JOBS)
        self.assertEqual(rc, 0, out[-3000:])

    def test_a_failed_record_fails(self):
        recs = passing(self.tags, 'ci')
        recs[0] = dict(recs[0], outcome='failed')
        rc, out, _ = run_check(recs)
        self.assertEqual(rc, 1)
        self.assertIn('FAILED', out)

    def test_skipped_only_or_missing_fails(self):
        recs = passing(self.tags, 'ci')
        r0 = recs[0]
        rc, out, _ = run_check([dict(r0, outcome='skipped')] + recs[1:])
        self.assertEqual(rc, 1)
        self.assertIn('but skipped', out)
        rc, out, _ = run_check(recs[1:])
        self.assertEqual(rc, 1)
        self.assertIn('but no record', out)

    def test_a_record_from_another_commit_or_job_is_not_evidence(self):
        recs = passing(self.tags, 'ci')
        rc, out, _ = run_check([dict(recs[0], commit='deadbeef')] + recs[1:])
        self.assertEqual(rc, 1)
        self.assertIn('not this run', out)
        rc, out, _ = run_check([dict(recs[0], job='some-other-job')] + recs[1:])
        self.assertEqual(rc, 1)

    def test_an_unknown_id_fails(self):
        recs = passing(self.tags, 'ci')
        rc, out, _ = run_check([dict(recs[0], ids=recs[0]['ids'] + ['H-99'])] + recs[1:])
        self.assertEqual(rc, 1)
        self.assertIn('unknown ID H-99', out)

    def test_nightly_evidence_is_deferred_in_ci_and_judged_in_the_nightly(self):
        _, _, report = run_check(passing(self.tags, 'ci'))
        self.assertRegex(report, r'\| CAST-3 \|[^\n]*\| deferred \|')
        rc, out, _ = run_check([], 'nightly', 'model')
        self.assertEqual(rc, 1)
        self.assertIn('releaseInFlightBreaksTest', out)


class Model(unittest.TestCase):
    def test_report_lines_become_outcomes(self):
        report = '\n'.join([
            'runs  releaseInFlight            releaseInFlightBreaksTest  passed 1 failed 0 (expect all-pass)',
            'runs  keeperOverrun              keeperOverrunBreaksTest    passed 0 failed 1 (expect all-pass)',
            'runs  gcReopens                  gcReopensBreaksTest        passed 1 failed 0 (expect all-pass)',
        ])
        with tempfile.TemporaryDirectory() as d:
            rp, out = os.path.join(d, 'r.txt'), os.path.join(d, 't.jsonl')
            with open(rp, 'w') as f:
                f.write(report)
            old = os.environ.get(tr.ENV)
            os.environ[tr.ENV] = out
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    tr.model(argparse.Namespace(script='consumer_model', report=rp))
            finally:
                if old is None:
                    del os.environ[tr.ENV]
                else:
                    os.environ[tr.ENV] = old
            got = {json.loads(l)['test']: json.loads(l)['outcome'] for l in tr.lines_of(out) if l}
        self.assertEqual(got['consumer_model: releaseInFlightBreaksTest'], 'passed')
        self.assertEqual(got['consumer_model: keeperOverrunBreaksTest'], 'failed')
        self.assertNotIn('consumer_model: errorSettlesBreaksTest', got, 'no line, no record')

    def test_open_model_rows_alternate_and_unknowns_are_skipped(self):
        report = '\n'.join([
            'sim   fastPath.qnt  recommended  safety  2000 x 40  ok       (expect ok)',
            'sim   fastPath.qnt  happyPath    batchIngestedAtMostOnce  2000 x 40  ok       (expect ok)',
            'sim   fastPath.qnt  tokenOnly    batchIngestedAtMostOnce  2000 x 40  ok       (expect VIOLATED)',
            'sim   retention.qnt retentionDesign safety 20000 x 50 UNKNOWN  (expect ok) 0s',
            'runs  sealer.qnt    blindCommit  blindCommitBreaksTest  UNKNOWN (expect all-pass) budget',
        ])
        got = self.outcomes('model_open', report)
        self.assertEqual(got['model_open: fastPath designs'], 'passed', 'an alternation in the regex, mutant rows not matched')
        self.assertEqual(got['model_open: retentionDesign safety'], 'skipped')
        self.assertEqual(got['model_open: sealer mutants (scripted)'], 'skipped')

    def outcomes(self, script, report):
        with tempfile.TemporaryDirectory() as d:
            rp, out = os.path.join(d, 'r.txt'), os.path.join(d, 't.jsonl')
            with open(rp, 'w') as f:
                f.write(report)
            old = os.environ.get(tr.ENV)
            os.environ[tr.ENV] = out
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    tr.model(argparse.Namespace(script=script, report=rp))
            finally:
                if old is None:
                    del os.environ[tr.ENV]
                else:
                    os.environ[tr.ENV] = old
            return {json.loads(l)['test']: json.loads(l)['outcome'] for l in tr.lines_of(out) if l}


if __name__ == '__main__':
    unittest.main()
