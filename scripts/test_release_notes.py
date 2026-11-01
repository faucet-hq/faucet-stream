"""Unit tests for release-notes.py.

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
_spec = importlib.util.spec_from_file_location("release_notes", HERE / "release-notes.py")
rn = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rn)

CHANGELOG = """# Changelog

Header text.
## [1.16.0](https://github.com/o/r/compare/faucet-cli-v1.15.1...faucet-cli-v1.16.0) - 2026-10-29

### Features

- New thing ([#1](https://github.com/o/r/pull/1))

### Dependencies

- Bumped with: faucet-core 1.14.1 → 1.15.0

## [1.15.1](https://github.com/o/r/compare/faucet-cli-v1.15.0...faucet-cli-v1.15.1) - 2026-10-22

### Bug Fixes

- Old fix
"""

META = {"workspace_root": "/ws", "packages": [
    {"name": "faucet-cli", "manifest_path": "/ws/cli/Cargo.toml"},
    {"name": "faucet-core", "manifest_path": "/ws/crates/core/Cargo.toml"},
    {"name": "faucet-registry", "manifest_path": "/ws/crates/registry/Cargo.toml"},
]}


class Pure(unittest.TestCase):
    def test_released_ignores_non_release_tags(self):
        self.assertEqual(rn.released(["faucet-core-v1.15.0", "v1.0.0", "faucet-cli-v1.16.0", "nightly"]),
                         {"faucet-core": (1, 15, 0), "faucet-cli": (1, 16, 0)})

    def test_previous_is_the_highest_earlier_version(self):
        tags = ["faucet-core-v1.9.0", "faucet-core-v1.14.1", "faucet-core-v1.15.0", "faucet-core-v1.2.0",
                "faucet-cli-v1.15.1", "faucet-registry-v1.0.0", "faucet-cored-v9.0.0"]
        current = {"faucet-core": (1, 15, 0), "faucet-registry": (1, 0, 0)}
        self.assertEqual(rn.previous_versions(tags, current),
                         {"faucet-core": (1, 14, 1), "faucet-registry": None})

    def test_section_drops_the_dependencies_block(self):
        self.assertEqual(rn.changelog_section(CHANGELOG, "1.16.0"),
                         "### Features\n\n- New thing ([#1](https://github.com/o/r/pull/1))")
        self.assertEqual(rn.changelog_section(CHANGELOG, "1.15.1"), "### Bug Fixes\n\n- Old fix")
        self.assertEqual(rn.changelog_section(CHANGELOG, "9.9.9"), "")

    def test_render(self):
        current = {"faucet-core": (1, 15, 0), "faucet-cli": (1, 16, 0), "faucet-registry": (1, 0, 0)}
        previous = {"faucet-core": (1, 14, 1), "faucet-cli": (1, 15, 1), "faucet-registry": None}
        body = rn.render("o/r", "1.16.0", "### Features\n\n- x", current, previous, rn.crate_dirs(META))
        self.assertTrue(body.startswith("### Features\n\n- x\n\n## Crates in this release\n\nThis release published 3 crates."))
        rows = [l for l in body.splitlines() if l.startswith("| `")]
        self.assertEqual([r.split("`")[1] for r in rows], ["faucet-cli", "faucet-core", "faucet-registry"])
        self.assertIn("| `faucet-core` | 1.14.1 → 1.15.0 | [CHANGELOG.md](https://github.com/o/r/blob/"
                      "faucet-core-v1.15.0/crates/core/CHANGELOG.md) | [1.15.0](https://crates.io/crates/faucet-core/1.15.0) |",
                      rows)
        self.assertIn("1.0.0 (new)", rows[2])

    def test_empty_section_gets_a_line(self):
        body = rn.render("o/r", "1.0.1", "", {"faucet-cli": (1, 0, 1)}, {"faucet-cli": (1, 0, 0)}, {})
        self.assertIn("No changes in faucet-cli itself", body)
        self.assertIn("published 1 crate.", body)
        self.assertIn("blob/faucet-cli-v1.0.1/CHANGELOG.md", body)


class Main(unittest.TestCase):
    def fake_git(self, at_commit):
        def run(*args):
            if args[:2] == ("tag", "--points-at"):
                return "\n".join(at_commit) + "\n"
            if args[:2] == ("tag", "-l"):
                return "faucet-cli-v1.15.1\nfaucet-core-v1.14.1\n" + "\n".join(at_commit) + "\n"
            if args[0] == "show":
                self.assertEqual(args[1], "abc:cli/CHANGELOG.md")
                return CHANGELOG
            raise AssertionError(args)
        return run

    def run_main(self, tags, *extra):
        d = Path(tempfile.mkdtemp())
        (d / "meta.json").write_text(json.dumps(META))
        err, out = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(out):
            code = rn.main(["--sha", "abc", "--metadata", str(d / "meta.json"), "--repo", "o/r", *extra],
                           self.fake_git(tags))
        return code, out.getvalue(), err.getvalue(), d

    def test_writes_the_body_and_is_repeatable(self):
        tags = ["faucet-cli-v1.16.0", "faucet-core-v1.15.0"]
        first = self.run_main(tags)
        second = self.run_main(tags)
        self.assertEqual(first[0], 0)
        self.assertEqual(first[1], second[1])
        self.assertIn("- New thing", first[1])
        self.assertNotIn("Bumped with", first[1])
        self.assertIn("1.15.1 → 1.16.0", first[1])

    def test_out_file(self):
        code, out, _, d = self.run_main(["faucet-cli-v1.16.0"], "--out", "/dev/null")
        self.assertEqual((code, out), (0, ""))

    def test_no_cli_tag_writes_nothing(self):
        code, out, err, _ = self.run_main(["faucet-core-v1.15.0"])
        self.assertEqual((code, out), (0, ""))
        self.assertIn("no faucet-cli tag", err)


if __name__ == "__main__":
    unittest.main()
