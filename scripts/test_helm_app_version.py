"""Unit tests for helm-app-version.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import contextlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("helm_app_version", HERE / "helm-app-version.py")
hav = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hav)

CHART = """apiVersion: v2
name: faucet-stream
# Chart version
version: 0.1.0
# The faucet-cli / image version this chart tracks.
appVersion: "1.7.0"
home: https://example.invalid/
"""

MANIFEST = '[package]\nname = "faucet-cli"\nversion = "1.13.2"\n'


class Parse(unittest.TestCase):
    def test_reads_quoted_and_bare_values(self):
        self.assertEqual(hav.app_version(CHART), "1.7.0")
        self.assertEqual(hav.app_version("appVersion: 2.0.1 # tracked\n"), "2.0.1")
        self.assertEqual(hav.app_version("appVersion: '3.1.0'\n"), "3.1.0")

    def test_indented_keys_are_not_the_chart_app_version(self):
        with self.assertRaises(ValueError):
            hav.app_version("dependencies:\n  - appVersion: 1.0.0\n")

    def test_duplicate_app_version_is_refused(self):
        with self.assertRaises(ValueError):
            hav.app_version('appVersion: "1"\nappVersion: "2"\n')

    def test_cli_version(self):
        self.assertEqual(hav.cli_version(MANIFEST), "1.13.2")

    def test_set_keeps_everything_else(self):
        out = hav.set_app_version(CHART, "1.13.2")
        self.assertEqual(out, CHART.replace('appVersion: "1.7.0"', 'appVersion: "1.13.2"'))


class Main(unittest.TestCase):
    def run_main(self, chart_text, argv):
        with tempfile.TemporaryDirectory() as d:
            chart = Path(d) / "Chart.yaml"
            manifest = Path(d) / "Cargo.toml"
            chart.write_text(chart_text)
            manifest.write_text(MANIFEST)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = hav.main(argv, chart=str(chart), manifest=str(manifest))
            return code, chart.read_text(), out.getvalue()

    def test_drift_fails_the_check_and_leaves_the_chart(self):
        code, text, out = self.run_main(CHART, [])
        self.assertEqual(code, 1)
        self.assertEqual(text, CHART)
        self.assertIn("appVersion is 1.7.0, but faucet-cli is 1.13.2", out)

    def test_write_syncs_then_check_passes(self):
        code, text, _ = self.run_main(CHART, ["--write"])
        self.assertEqual(code, 0)
        self.assertEqual(hav.app_version(text), "1.13.2")
        code, _, _ = self.run_main(text, [])
        self.assertEqual(code, 0)

    def test_missing_app_version_fails(self):
        code, _, out = self.run_main("apiVersion: v2\n", ["--write"])
        self.assertEqual(code, 1)
        self.assertIn("found 0", out)

    def test_repo_chart_matches_cli(self):
        root = HERE.parent
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = hav.main(
                [], chart=str(root / hav.CHART), manifest=str(root / hav.CLI_MANIFEST)
            )
        self.assertEqual(code, 0, out.getvalue())


if __name__ == "__main__":
    unittest.main()
