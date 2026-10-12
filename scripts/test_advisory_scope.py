"""Unit tests for advisory-scope.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import os
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("advisory_scope", HERE / "advisory-scope.py")
asc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(asc)

DENY = {"advisories": {"ignore": [{"id": "RUSTSEC-2026-0194", "reason": "x"}]}}


def lock(*versions):
    return {"package": [{"name": "quick-xml", "version": v} for v in versions]}


class Violations(unittest.TestCase):
    def test_triaged_and_fixed_versions_pass(self):
        self.assertEqual(asc.violations(lock("0.31.0", "0.39.4", "0.41.0", "0.42.0"), DENY), [])

    def test_an_untriaged_affected_copy_fails(self):
        found = asc.violations(lock("0.39.4", "0.38.1"), DENY)
        self.assertEqual(len(found), 1)
        self.assertIn("quick-xml 0.38.1", found[0])

    def test_nothing_is_checked_once_the_ignore_is_dropped(self):
        self.assertEqual(asc.violations(lock("0.38.1"), {"advisories": {"ignore": []}}), [])


class Workspace(unittest.TestCase):
    def test_the_workspace_passes(self):
        cwd = os.getcwd()
        os.chdir(HERE.parent)
        try:
            self.assertEqual(asc.main(), 0)
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
