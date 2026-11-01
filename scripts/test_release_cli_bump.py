"""Unit tests for release-cli-bump.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import contextlib
import importlib.util
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("release_cli_bump", HERE / "release-cli-bump.py")
rcb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rcb)


def dep(name, optional=False, features=(), default=True, kind=None, rename=None):
    return {"name": name, "optional": optional, "features": list(features),
            "uses_default_features": default, "kind": kind, "rename": rename}


def pkg(name, deps=(), features=None, version="1.0.0"):
    return {"name": name, "version": version, "dependencies": list(deps),
            "features": features or {}, "manifest_path": f"/ws/{name}/Cargo.toml"}


WORKSPACE = [
    pkg("faucet-cli", [
        dep("faucet-core"),
        dep("faucet-source-rest", optional=True),
        dep("faucet-sink-kafka", optional=True),
        dep("faucet-registry", features=["x"]),
        dep("faucet-conformance", kind="dev"),
        dep("serde"),
    ], {"full": ["source-rest", "kafka-extra"], "default": [], "source-rest": ["dep:faucet-source-rest"],
        "kafka-extra": ["faucet-sink-kafka?/sr"]}),
    pkg("faucet-core"),
    pkg("faucet-source-rest", [dep("faucet-common-http", optional=True), dep("faucet-core")],
        {"default": [], "http": ["dep:faucet-common-http"]}),
    pkg("faucet-common-http"),
    pkg("faucet-sink-kafka", [dep("faucet-common-kafka", optional=True)],
        {"default": [], "sr": ["dep:faucet-common-kafka"]}),
    pkg("faucet-common-kafka"),
    pkg("faucet-registry", [dep("faucet-lineage", optional=True), dep("faucet-auth", optional=True),
                            dep("faucet-codegen", kind="build")],
        {"default": [], "x": ["faucet-lineage/y"], "lineage-ish": ["dep:faucet-auth"]}),
    pkg("faucet-lineage", features={"y": []}),
    pkg("faucet-auth"),
    pkg("faucet-codegen"),
    pkg("faucet-conformance"),
    pkg("faucet-stream", [dep("faucet-core")]),
]


class Closure(unittest.TestCase):
    def test_follows_features_normal_and_build_deps_only(self):
        self.assertEqual(rcb.build_closure(WORKSPACE), {
            "faucet-core", "faucet-source-rest", "faucet-registry", "faucet-lineage", "faucet-codegen",
        })

    def test_a_weak_feature_enables_nothing_until_its_dep_is_on(self):
        ws = [dict(p) for p in WORKSPACE]
        ws[0] = dict(ws[0], features=dict(ws[0]["features"], full=["source-rest", "kafka-extra", "dep:faucet-sink-kafka"]))
        closure = rcb.build_closure(ws)
        self.assertIn("faucet-sink-kafka", closure)
        self.assertIn("faucet-common-kafka", closure)

    def test_an_implicit_optional_dependency_feature(self):
        ws = [pkg("faucet-cli", [dep("faucet-x", optional=True)], {"full": ["faucet-x"]}), pkg("faucet-x")]
        self.assertEqual(rcb.build_closure(ws), {"faucet-x"})

    def test_a_renamed_dependency_is_addressed_by_its_rename(self):
        ws = [pkg("faucet-cli", [dep("faucet-x", optional=True, rename="x")], {"full": ["dep:x"]}), pkg("faucet-x")]
        self.assertEqual(rcb.build_closure(ws), {"faucet-x"})

    def test_unknown_root_is_refused(self):
        with self.assertRaises(ValueError):
            rcb.build_closure([pkg("faucet-core")])


class Versions(unittest.TestCase):
    def test_bump_levels(self):
        self.assertIsNone(rcb.bump_level("1.2.3", "1.2.3"))
        self.assertEqual(rcb.bump_level("1.2.3", "1.2.4"), "patch")
        self.assertEqual(rcb.bump_level("1.2.3", "1.3.0"), "minor")
        self.assertEqual(rcb.bump_level("1.2.3", "2.0.0"), "major")
        self.assertEqual(rcb.bump_level(None, "1.0.0"), "minor")

    def test_backwards_and_prerelease_are_refused(self):
        with self.assertRaises(ValueError):
            rcb.bump_level("1.3.0", "1.2.9")
        with self.assertRaises(ValueError):
            rcb.parse_version("1.2.0-rc.1")

    def test_next_version(self):
        self.assertEqual(rcb.next_version("1.15.1", "patch"), "1.15.2")
        self.assertEqual(rcb.next_version("1.15.1", "minor"), "1.16.0")
        self.assertEqual(rcb.next_version("1.15.1", "major"), "2.0.0")

    def test_compute_bumps(self):
        self.assertEqual(
            rcb.compute_bumps({"a": "1.0.0", "b": "1.1.0"}, {"a": "1.0.1", "b": "1.1.0", "c": "1.0.0"}),
            {"a": ("1.0.0", "1.0.1"), "c": (None, "1.0.0")},
        )


class Plan(unittest.TestCase):
    closure = {"faucet-core", "faucet-source-rest", "faucet-new"}

    def test_highest_bump_in_the_closure_wins(self):
        bumps = {"faucet-core": ("1.14.0", "1.14.1"), "faucet-source-rest": ("1.2.0", "1.3.0"),
                 "faucet-stream": ("1.10.0", "2.0.0")}
        new, crates = rcb.plan(self.closure, bumps, "1.15.1", "1.15.1")
        self.assertEqual(new, "1.16.0")
        self.assertEqual([c[0] for c in crates], ["faucet-core", "faucet-source-rest"])

    def test_patch_only(self):
        new, _ = rcb.plan(self.closure, {"faucet-core": ("1.14.0", "1.14.1")}, "1.15.1", "1.15.1")
        self.assertEqual(new, "1.15.2")

    def test_a_new_crate_counts_as_minor(self):
        new, _ = rcb.plan(self.closure, {"faucet-new": (None, "1.0.0")}, "1.15.1", "1.15.1")
        self.assertEqual(new, "1.16.0")

    def test_an_already_sufficient_cli_bump_is_kept(self):
        bumps = {"faucet-core": ("1.14.0", "1.14.1"), "faucet-cli": ("1.15.1", "1.16.0")}
        self.assertEqual(rcb.plan(self.closure, bumps, "1.15.1", "1.16.0")[0], "1.16.0")

    def test_a_too_small_cli_bump_is_raised(self):
        bumps = {"faucet-source-rest": ("1.2.0", "1.3.0"), "faucet-cli": ("1.15.1", "1.15.2")}
        self.assertEqual(rcb.plan(self.closure, bumps, "1.15.1", "1.15.2")[0], "1.16.0")

    def test_rerun_after_write_is_a_no_op(self):
        bumps = {"faucet-core": ("1.14.0", "1.14.1"), "faucet-cli": ("1.15.1", "1.15.2")}
        self.assertEqual(rcb.plan(self.closure, bumps, "1.15.1", "1.15.2")[0], "1.15.2")

    def test_crates_outside_the_closure_bump_nothing(self):
        self.assertEqual(rcb.plan(self.closure, {"faucet-stream": ("1.0.0", "1.1.0")}, "1.15.1", "1.15.1"),
                         ("1.15.1", []))

    def test_a_major_in_the_closure_stops(self):
        with self.assertRaises(SystemExit) as e:
            rcb.plan(self.closure, {"faucet-core": ("1.14.0", "2.0.0")}, "1.15.1", "1.15.1")
        self.assertIn("faucet-core 1.14.0 -> 2.0.0", str(e.exception))


HEADER = "# Changelog\n\nAll notable changes.\n"
RELEASED = ("## [1.15.1](https://github.com/o/r/compare/faucet-cli-v1.15.0...faucet-cli-v1.15.1) - 2026-10-22\n\n"
            "### Bug Fixes\n\n- Old fix\n")
CRATES = [("faucet-core", "1.14.0", "1.14.1"), ("faucet-x", None, "1.0.0")]


class Changelog(unittest.TestCase):
    def test_adds_a_section_when_release_plz_did_not(self):
        out = rcb.update_changelog(HEADER + RELEASED, "1.15.1", "1.15.2", CRATES, "2026-10-30", "o/r")
        self.assertEqual(out, HEADER
                         + "## [1.15.2](https://github.com/o/r/compare/faucet-cli-v1.15.1...faucet-cli-v1.15.2) - 2026-10-30\n\n"
                         "### Dependencies\n\n- Bumped with: faucet-core 1.14.0 → 1.14.1, faucet-x 1.0.0 (new)\n\n"
                         + RELEASED)

    def test_renames_release_plz_section_and_keeps_its_entries_and_date(self):
        ours = ("## [1.15.2](https://github.com/o/r/compare/faucet-cli-v1.15.1...faucet-cli-v1.15.2) - 2026-10-29\n\n"
                "### Bug Fixes\n\n- New fix\n\n")
        out = rcb.update_changelog(HEADER + ours + RELEASED, "1.15.1", "1.16.0", CRATES, "2026-10-30", "o/r")
        self.assertIn("## [1.16.0](https://github.com/o/r/compare/faucet-cli-v1.15.1...faucet-cli-v1.16.0) - 2026-10-29\n\n"
                      "### Bug Fixes\n\n- New fix\n\n### Dependencies\n\n- Bumped with:", out)
        self.assertTrue(out.endswith(RELEASED))

    def test_is_idempotent(self):
        once = rcb.update_changelog(HEADER + RELEASED, "1.15.1", "1.15.2", CRATES, "2026-10-30", "o/r")
        twice = rcb.update_changelog(once, "1.15.1", "1.15.2", CRATES, "2026-10-31", "o/r")
        self.assertEqual(once, twice)

    def test_regenerates_the_dependencies_block(self):
        once = rcb.update_changelog(HEADER + RELEASED, "1.15.1", "1.15.2", CRATES[:1], "2026-10-30", "o/r")
        again = rcb.update_changelog(once, "1.15.1", "1.16.0", CRATES, "2026-10-30", "o/r")
        self.assertEqual(again.count("### Dependencies"), 1)
        self.assertIn("faucet-x 1.0.0 (new)", again)
        self.assertIn("## [1.16.0]", again)
        self.assertNotIn("## [1.15.2]", again)


class Edits(unittest.TestCase):
    def test_manifest_version_only_in_package(self):
        text = '[package]\nname = "faucet-cli"\nversion = "1.15.1"\n\n[dependencies]\nx = { version = "1" }\n'
        self.assertEqual(rcb.set_manifest_version(text, "1.16.0"), text.replace("1.15.1", "1.16.0"))
        with self.assertRaises(ValueError):
            rcb.set_manifest_version("[dependencies]\n", "1.0.0")

    def test_lock_version(self):
        text = ('[[package]]\nname = "faucet-cli"\nversion = "1.15.1"\n\n'
                '[[package]]\nname = "faucet-cli-extra"\nversion = "1.15.1"\n')
        out = rcb.set_lock_version(text, "faucet-cli", "1.16.0")
        self.assertIn('name = "faucet-cli"\nversion = "1.16.0"', out)
        self.assertIn('name = "faucet-cli-extra"\nversion = "1.15.1"', out)
        with self.assertRaises(ValueError):
            rcb.set_lock_version(text, "missing", "1.0.0")


class Main(unittest.TestCase):
    def test_end_to_end_in_a_git_repo(self):
        d = Path(tempfile.mkdtemp())
        run = lambda *a: subprocess.run(["git", "-C", str(d), *a], check=True, capture_output=True)
        (d / "cli").mkdir()
        (d / "core").mkdir()
        (d / "cli/Cargo.toml").write_text('[package]\nname = "faucet-cli"\nversion = "1.15.1"\n')
        (d / "core/Cargo.toml").write_text('[package]\nname = "faucet-core"\nversion = "1.14.0"\n')
        (d / "cli/CHANGELOG.md").write_text(HEADER + RELEASED)
        (d / "Cargo.lock").write_text('[[package]]\nname = "faucet-cli"\nversion = "1.15.1"\n')
        run("init", "-q")
        run("add", ".")
        run("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base")
        base = subprocess.run(["git", "-C", str(d), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
        meta = {"workspace_root": str(d), "packages": [
            dict(pkg("faucet-cli", [dep("faucet-core")], {"full": []}, "1.15.1"), manifest_path=str(d / "cli/Cargo.toml")),
            dict(pkg("faucet-core", version="1.14.1"), manifest_path=str(d / "core/Cargo.toml")),
        ]}
        (d / "meta.json").write_text(json.dumps(meta))
        cwd = os.getcwd()
        os.chdir(d)
        try:
            for _ in range(2):
                with contextlib.redirect_stdout(io.StringIO()) as out:
                    self.assertEqual(rcb.main(["--base", base, "--metadata", "meta.json", "--write",
                                               "--today", "2026-10-30", "--repo", "o/r"]), 0)
                meta["packages"][0]["version"] = (d / "cli/Cargo.toml").read_text().split('"')[3]
                (d / "meta.json").write_text(json.dumps(meta))
        finally:
            os.chdir(cwd)
        self.assertIn('version = "1.15.2"', (d / "cli/Cargo.toml").read_text())
        self.assertIn('version = "1.15.2"', (d / "Cargo.lock").read_text())
        log = (d / "cli/CHANGELOG.md").read_text()
        self.assertEqual(log.count("## [1.15.2]"), 1)
        self.assertIn("faucet-core 1.14.0 → 1.14.1", log)
        self.assertIn("faucet-cli 1.15.1 -> 1.15.2", out.getvalue())


if __name__ == "__main__":
    unittest.main()
