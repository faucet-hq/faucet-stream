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


class FakeServer(unittest.TestCase):
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


class Register(FakeServer):
    def test_a_new_template_is_registered_with_its_version(self):
        calls = self.fake({"/templates": (201, {"version": 3})})
        self.assertEqual(dc.register("acme/x", "doc", set()), ("registered", 3))
        self.assertEqual(calls, ["/v1/templates"])

    def test_a_refused_registration_is_a_failure(self):
        self.fake({"/templates": (422, {"error": {"message": "bad config"}})})
        outcome, version = dc.register("acme/x", "doc", set())
        self.assertTrue(outcome.startswith("failed (422"), outcome)
        self.assertIsNone(version)

    def test_an_existing_template_is_kept(self):
        calls = self.fake({})
        self.assertEqual(dc.register("acme/x", "doc", {"acme/x"}), ("kept", None))
        self.assertEqual(calls, [])


class Settle(FakeServer):
    def test_launched_and_deprecated_are_reported_when_both_succeed(self):
        calls = self.fake({})
        self.assertEqual(dc.settle("acme/x", 1, "deprecated"), "deprecated")
        self.assertEqual([c.rsplit("/", 1)[-1] for c in calls], ["launch", "deprecate"])

    def test_a_draft_needs_no_request(self):
        calls = self.fake({})
        self.assertEqual(dc.settle("acme/x", 1, "draft"), "draft")
        self.assertEqual(calls, [])

    def test_a_refused_launch_is_a_failure(self):
        self.fake({"/launch": (409, {"error": {"message": "tests required"}})})
        outcome = dc.settle("acme/x", 1, "launched")
        self.assertTrue(outcome.startswith("failed (launch 409"), outcome)

    def test_a_refused_deprecate_is_a_failure(self):
        self.fake({"/deprecate": (403, {})})
        self.assertTrue(dc.settle("acme/x", 1, "deprecated").startswith("failed (deprecate 403"))

if __name__ == "__main__":
    unittest.main()
