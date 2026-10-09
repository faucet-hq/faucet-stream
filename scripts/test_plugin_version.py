"""Unit tests for plugin-version.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("plugin_version", HERE / "plugin-version.py")
pv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pv)

MANIFEST = '[package]\nname = "faucet-cli"\nversion = "1.14.0"\n'


def plugin(version):
    return json.dumps({"name": "faucet", "version": version, "skills": "./"}, indent=2) + "\n"


class Run(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        (self.dir / "Cargo.toml").write_text(MANIFEST)
        (self.dir / "market.json").write_text('{"name": "faucet", "plugins": []}')

    def run_main(self, argv, *plugins):
        paths = []
        for i, text in enumerate(plugins):
            p = self.dir / f"plugin{i}.json"
            p.write_text(text)
            paths.append(str(p))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = pv.main(argv, str(self.dir / "Cargo.toml"), paths, [str(self.dir / "market.json")])
        return code, out.getvalue(), paths

    def test_matching_versions_pass(self):
        code, out, _ = self.run_main([], plugin("1.14.0"), plugin("1.14.0"))
        self.assertEqual(code, 0)
        self.assertIn("1.14.0", out)

    def test_a_stale_manifest_fails_and_names_it(self):
        code, out, paths = self.run_main([], plugin("1.14.0"), plugin("1.13.3"))
        self.assertEqual(code, 1)
        self.assertIn(paths[1], out)
        self.assertIn("1.13.3", out)

    def test_write_syncs_only_the_version(self):
        code, _, paths = self.run_main(["--write"], plugin("1.13.3"))
        self.assertEqual(code, 0)
        self.assertEqual(Path(paths[0]).read_text(), plugin("1.14.0"))

    def test_missing_version_is_refused(self):
        code, out, _ = self.run_main([], '{"name": "faucet"}')
        self.assertEqual(code, 1)
        self.assertIn("no string `version`", out)

    def test_invalid_marketplace_json_fails(self):
        (self.dir / "market.json").write_text("{not json")
        code, out, _ = self.run_main([], plugin("1.14.0"))
        self.assertEqual(code, 1)
        self.assertIn("invalid JSON", out)


if __name__ == "__main__":
    unittest.main()
