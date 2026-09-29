"""Tests of ci/mutants.py (no tools needed): python3 -m unittest ci/mutants_test.py"""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mutants  # noqa: E402


class Keys(unittest.TestCase):
    def test_rust_keys_drop_positions_and_merge_repeats(self):
        with tempfile.TemporaryDirectory() as d:
            open(os.path.join(d, 'missed.txt'), 'w').write(
                'src/gc.rs:12:9: replace < with <= in doomed\nsrc/gc.rs:40:3: replace < with <= in doomed\n')
            open(os.path.join(d, 'timeout.txt'), 'w').write('src/plan.rs:7:1: replace found -> Found with Default::default()\n')
            self.assertEqual(mutants.rust(d), ['rust src/gc.rs: replace < with <= in doomed',
                                               'rust src/plan.rs: replace found -> Found with Default::default()'])

    def test_go_keys_name_the_function_not_the_line(self):
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, 'commit'))
            open(os.path.join(d, 'commit', 'lane.go'), 'w').write(
                'package commit\n\nfunc (l *Lane) Append() {\n\tif a < b {\n\t}\n}\n\nfunc next[T any](x T) {\n\tif x {\n\t}\n}\n')
            rep = {'files': [{'file_name': 'lane.go', 'mutations': [
                {'line': 4, 'column': 7, 'type': 'CONDITIONALS_BOUNDARY', 'status': 'LIVED'},
                {'line': 4, 'column': 7, 'type': 'CONDITIONALS_NEGATION', 'status': 'KILLED'},
                {'line': 9, 'column': 5, 'type': 'CONDITIONALS_NEGATION', 'status': 'NOT COVERED'}]}]}
            p = os.path.join(d, 'r.json')
            json.dump(rep, open(p, 'w'))
            # gremlins names files relative to the package it ran on (./commit, from the module root)
            cwd = os.getcwd()
            os.chdir(d)
            try:
                got = mutants.go(p, './commit')
            finally:
                os.chdir(cwd)
            self.assertEqual(got, ['go commit/lane.go (*Lane).Append CONDITIONALS_BOUNDARY #1',
                                   'go commit/lane.go next CONDITIONALS_NEGATION #1'])


class Check(unittest.TestCase):
    def run_check(self, baseline, survivors):
        with tempfile.TemporaryDirectory() as d:
            b, s = os.path.join(d, 'b.txt'), os.path.join(d, 's.txt')
            open(b, 'w').write(baseline)
            open(s, 'w').write(survivors)
            return mutants.check(b, [s], None)

    def test_a_new_survivor_fails_an_accepted_one_passes(self):
        self.assertEqual(self.run_check('# c\nrust a: x | equivalent mutant\n', 'rust a: x\n'), 0)
        self.assertEqual(self.run_check('rust a: x | equivalent mutant\n', 'rust a: x\nrust a: y\n'), 1)

    def test_a_baseline_line_needs_a_reason(self):
        self.assertEqual(self.run_check('rust a: x\n', 'rust a: x\n'), 1)
        self.assertEqual(self.run_check('rust a: x | \n', 'rust a: x\n'), 1)

    def test_a_killed_baseline_entry_only_warns(self):
        self.assertEqual(self.run_check('rust a: x | reason\n', ''), 0)


if __name__ == '__main__':
    unittest.main()
