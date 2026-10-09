"""Unit tests for full-feature-coverage.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location(
    "full_feature_coverage", HERE / "full-feature-coverage.py"
)
ffc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ffc)


class Missing(unittest.TestCase):
    def test_a_feature_reached_through_an_aggregate_is_covered(self):
        features = {
            "full": ["serve", "dep:x"],
            "serve": ["templates", "faucet-core/serve"],
            "templates": [],
        }
        self.assertEqual(ffc.missing(features), [])

    def test_a_feature_nothing_reaches_is_reported(self):
        features = {"full": ["serve"], "serve": [], "secrets": [], "arrow": []}
        self.assertEqual(ffc.missing(features), ["arrow", "secrets"])

    def test_dependency_forwards_do_not_count_as_features(self):
        features = {"full": ["faucet-core/arrow", "dep:arrow"], "arrow": []}
        self.assertEqual(ffc.missing(features), ["arrow"])

    def test_exemptions_match_names_and_prefixes(self):
        features = {"full": [], "default": [], "transform-cast": [], "otel": []}
        self.assertEqual(ffc.missing(features, ("default", "transform-*")), ["otel"])

    def test_a_manifest_without_full_is_reported(self):
        self.assertEqual(ffc.missing({"default": []}), ["(no `full` feature)"])

    def test_cycles_terminate(self):
        features = {"full": ["a"], "a": ["b"], "b": ["a"]}
        self.assertEqual(ffc.missing(features), [])


class Dockerfile(unittest.TestCase):
    def test_reads_the_default_features_arg(self):
        text = 'ARG FOO=1\nARG DEFAULT_FEATURES="full"\nRUN true\n'
        self.assertEqual(ffc.dockerfile_default_features(text), "full")

    def test_a_missing_arg_is_none(self):
        self.assertIsNone(ffc.dockerfile_default_features("FROM scratch\n"))


class Repository(unittest.TestCase):
    def test_the_checked_in_manifests_pass(self):
        import os

        cwd = os.getcwd()
        os.chdir(HERE.parent)
        try:
            self.assertEqual(ffc.main(), 0)
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
