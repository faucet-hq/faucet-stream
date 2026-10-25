"""Unit tests for demo_catalog.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("demo_catalog", HERE / "demo_catalog.py")
dc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dc)


class Register(unittest.TestCase):
    def fake(self, replies):
        calls = []

        def request(method, path, body=None):
            calls.append(path)
            for suffix, reply in replies.items():
                if path.endswith(suffix):
                    return reply
            return 200, {"version": 1}

        dc.request = request
        return calls

    def test_launched_and_deprecated_are_reported_when_both_succeed(self):
        calls = self.fake({})
        self.assertEqual(dc.register("acme/x", "doc", "deprecated", set()), "deprecated")
        self.assertEqual([c.rsplit("/", 1)[-1] for c in calls], ["templates", "launch", "deprecate"])

    def test_a_refused_launch_is_a_failure(self):
        self.fake({"/launch": (409, {"error": {"message": "tests required"}})})
        outcome = dc.register("acme/x", "doc", "launched", set())
        self.assertTrue(outcome.startswith("failed (launch 409"), outcome)

    def test_a_refused_deprecate_is_a_failure(self):
        self.fake({"/deprecate": (403, {})})
        self.assertTrue(dc.register("acme/x", "doc", "deprecated", set()).startswith("failed (deprecate 403"))

    def test_an_existing_template_is_kept(self):
        self.assertEqual(dc.register("acme/x", "doc", "launched", {"acme/x"}), "kept")


if __name__ == "__main__":
    unittest.main()
