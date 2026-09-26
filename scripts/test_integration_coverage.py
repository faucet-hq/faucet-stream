"""Unit tests for integration-coverage.py and the lcov parser it shares.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("integration_coverage", HERE / "integration-coverage.py")
ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ic)
pc = ic.patch_coverage


def lcov(records):
    out = []
    for path, lines in records:
        out.append(f"SF:{path}")
        out += [f"DA:{n},{c}" for n, c in lines.items()]
        out.append("end_of_record")
    return "\n".join(out) + "\n"


def write(tmp, name, text):
    p = Path(tmp) / name
    p.write_text(text)
    return str(p)


class ScopeTests(unittest.TestCase):
    def test_io_paths_are_in_scope_and_pure_ones_are_not(self):
        for p in [
            "crates/sink/postgres/src/sink.rs",
            "crates/source/gcs/src/stream.rs",
            "crates/common/gcs/src/json_control.rs",
            "crates/state/redis/src/lib.rs",
            "cli/src/serve/handlers/runs.rs",
            "cli/src/executor.rs",
            "cli/src/templates/store.rs",
            "cli/src/hub/compose.rs",
        ]:
            self.assertTrue(ic.in_scope(p), p)
        for p in [
            "crates/sink/postgres/src/config.rs",
            "crates/sink/postgres/tests/upsert.rs",
            "crates/core/src/pipeline.rs",
            "cli/src/expand.rs",
            "cli/src/serve/ui/app.js",
            "crates/sink/postgres/build.rs",
        ]:
            self.assertFalse(ic.in_scope(p), p)


class EscapeTests(unittest.TestCase):
    def test_reason_is_read_from_its_own_line(self):
        body = "Summary\n\nno-integration-test: SIGTERM handler, not reachable in CI \n"
        self.assertEqual(ic.escape_reason(body), "SIGTERM handler, not reachable in CI")
        self.assertEqual(ic.escape_reason("No-Integration-Test: x"), "x")

    def test_absent_or_empty_reason_is_none(self):
        for body in [None, "", "no-integration-test:", "mentions no-integration-test: inline? no"]:
            self.assertIsNone(ic.escape_reason(body), body)


class TestModuleLinesTests(unittest.TestCase):
    def test_cfg_test_modules_are_found_with_their_braces(self):
        src = "\n".join([
            "fn real() {",               # 1
            '    let s = "}";',           # 2
            "}",                         # 3
            "",                          # 4
            "#[cfg(test)]",              # 5
            "mod tests {",               # 6
            "    fn t() { let c = '{'; }",  # 7
            "    // } not a close",      # 8
            "}",                         # 9
            "fn after() {}",             # 10
            "#[cfg(test)]",              # 11
            "",                          # 12
            "pub(crate) mod more { fn x() {} }",  # 13
        ])
        self.assertEqual(ic.test_module_lines(src), {5, 6, 7, 8, 9, 11, 12, 13})

    def test_cfg_test_on_a_function_is_not_a_module(self):
        self.assertEqual(ic.test_module_lines("#[cfg(test)]\nfn helper() {}\n"), set())

    def test_unreadable_file_has_no_test_lines(self):
        self.assertEqual(ic._source_test_lines("does/not/exist.rs"), set())

    def _tree(self, files):
        root = Path(tempfile.mkdtemp())
        for rel, body in files.items():
            f = root / rel
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(body)
        return root

    def test_a_cfg_test_file_module_is_test_only(self):
        root = self._tree({
            "c/src/lib.rs": "mod sink;\n#[cfg(test)]\nmod test_support;\n",
            "c/src/sink.rs": "pub fn a() {}\n",
            "c/src/test_support.rs": "pub fn helper() {}\nfn two() {}\n",
        })
        self.assertTrue(ic.is_test_only_file(str(root / "c/src/test_support.rs")))
        self.assertFalse(ic.is_test_only_file(str(root / "c/src/sink.rs")))
        self.assertEqual(ic._source_test_lines(str(root / "c/src/test_support.rs")), {1, 2})
        self.assertEqual(ic._source_test_lines(str(root / "c/src/sink.rs")), set())

    def test_attributes_between_cfg_test_and_the_declaration_are_skipped(self):
        root = self._tree({
            "c/src/lib.rs": "#[cfg(test)]\n#[allow(dead_code)]\npub(crate) mod fixtures;\n",
            "c/src/fixtures.rs": "fn f() {}\n",
        })
        self.assertTrue(ic.is_test_only_file(str(root / "c/src/fixtures.rs")))

    def test_nested_modules_under_a_test_only_parent_are_test_only(self):
        root = self._tree({
            "c/src/lib.rs": "#[cfg(test)]\nmod mocks;\nmod real;\n",
            "c/src/mocks/mod.rs": "mod http;\n",
            "c/src/mocks/http.rs": "fn h() {}\n",
            "c/src/real.rs": "mod inner;\n",
            "c/src/real/inner.rs": "fn i() {}\n",
        })
        self.assertTrue(ic.is_test_only_file(str(root / "c/src/mocks/mod.rs")))
        self.assertTrue(ic.is_test_only_file(str(root / "c/src/mocks/http.rs")))
        self.assertFalse(ic.is_test_only_file(str(root / "c/src/real/inner.rs")))
        self.assertFalse(ic.is_test_only_file(str(root / "c/src/lib.rs")))

    def test_cfg_test_on_another_module_does_not_leak(self):
        root = self._tree({
            "c/src/lib.rs": "#[cfg(test)]\nmod tests;\nmod sink;\n",
            "c/src/sink.rs": "fn s() {}\n",
        })
        self.assertFalse(ic.is_test_only_file(str(root / "c/src/sink.rs")))


class EvaluateTests(unittest.TestCase):
    def test_test_module_lines_are_not_counted(self):
        changed = {"crates/sink/x/src/sink.rs": {1, 2, 50}}
        integ = {"crates/sink/x/src/sink.rs": {1: 1, 2: 0, 50: 0}}
        rows = ic.evaluate(changed, integ, None, test_lines=lambda _: {50})
        self.assertEqual((rows[0]["changed"], rows[0]["integration"]), (2, 1))

    def test_rows_count_integration_and_unit_hits(self):
        changed = {
            "crates/sink/x/src/sink.rs": {10, 11, 12, 99},
            "crates/sink/x/src/config.rs": {1},
            "crates/core/src/lib.rs": {5},
        }
        integ = {"/repo/crates/sink/x/src/sink.rs": {10: 3, 11: 0, 12: 0}}
        unit = {"/repo/crates/sink/x/src/sink.rs": {10: 1, 11: 1, 12: 0}}
        rows = ic.evaluate(changed, integ, unit)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual((r["changed"], r["integration"], r["unit"]), (3, 1, 2))
        self.assertEqual(r["missed"], [11, 12])

    def test_file_only_in_the_unit_report_counts_as_zero_integration(self):
        changed = {"crates/source/y/src/stream.rs": {3, 4}}
        rows = ic.evaluate(changed, {}, {"crates/source/y/src/stream.rs": {3: 1, 4: 1}})
        self.assertEqual((rows[0]["changed"], rows[0]["integration"]), (2, 0))

    def test_uninstrumented_changes_produce_no_row(self):
        self.assertEqual(ic.evaluate({"crates/sink/x/src/sink.rs": {1}}, {}, None), [])


class VerdictTests(unittest.TestCase):
    def row(self, f, changed, hit):
        return {"file": f, "changed": changed, "integration": hit, "unit": None, "missed": []}

    def test_every_file_needs_one_hit_and_the_total_needs_the_floor(self):
        ok, pct, problems = ic.verdict([self.row("a", 10, 7), self.row("b", 10, 6)], 60)
        self.assertTrue(ok)
        self.assertAlmostEqual(pct, 65.0)

        ok, _, problems = ic.verdict([self.row("a", 10, 10), self.row("b", 2, 0)], 60)
        self.assertFalse(ok)
        self.assertIn("b: none of its 2 changed lines", problems[0])

        ok, pct, problems = ic.verdict([self.row("a", 10, 5)], 60)
        self.assertFalse(ok)
        self.assertIn("50.0%", problems[-1])

    def test_nothing_in_scope_passes(self):
        self.assertEqual(ic.verdict([], 60), (True, 100.0, []))


class SummaryTests(unittest.TestCase):
    def test_table_problems_and_waiver(self):
        rows = [{"file": "crates/sink/x/src/sink.rs", "changed": 4, "integration": 0, "unit": 3, "missed": [1]}]
        md = ic.summary_markdown(rows, 0.0, ["x: none"], "no backend")
        self.assertIn("| `crates/sink/x/src/sink.rs` | 4 | 0 | 3 |", md)
        self.assertIn("- x: none", md)
        self.assertIn("Waived by the PR: `no-integration-test: no backend`", md)
        self.assertIn("nothing to check", ic.summary_markdown([], 100.0, [], None))


class ParseLcovTests(unittest.TestCase):
    def test_duplicate_records_keep_the_highest_hit_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write(tmp, "a.lcov", lcov([("f.rs", {1: 0, 2: 5}), ("f.rs", {1: 2, 2: 0}), ("g.rs", {1: 0})]))
            self.assertEqual(pc.parse_lcov(path)["f.rs"], {1: 2, 2: 5})


class MainTests(unittest.TestCase):
    """Drive main() end to end against a throwaway git repository."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = self.tmp.name
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "t@example.com")
        self.git("config", "user.name", "t")
        src = Path(self.repo, "crates/sink/x/src")
        src.mkdir(parents=True)
        (src / "sink.rs").write_text("fn a() {}\n")
        self.git("add", ".")
        self.git("commit", "-qm", "base")
        self.git("branch", "base")
        (src / "sink.rs").write_text("fn a() {}\nfn b() {}\nfn c() {}\n")
        self.git("commit", "-qam", "change")
        self.cwd = os.getcwd()
        os.chdir(self.repo)

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def git(self, *args):
        subprocess.run(["git", *args], cwd=self.repo, check=True, capture_output=True)

    def run_main(self, hits, body=None, extra=()):
        path = write(self.repo, "i.lcov", lcov([(f"{self.repo}/crates/sink/x/src/sink.rs", hits)]))
        summary = Path(self.repo, "summary.md")
        env = {"GITHUB_STEP_SUMMARY": str(summary), "PR_BODY": body or ""}
        with mock.patch.dict(os.environ, env):
            code = ic.main(["--lcov", path, "--base", "base", *extra])
        return code, summary.read_text()

    def test_passes_when_changed_lines_ran(self):
        code, md = self.run_main({2: 1, 3: 1})
        self.assertEqual(code, 0)
        self.assertIn("100.0%", md)

    def test_fails_when_no_changed_line_ran(self):
        code, _ = self.run_main({2: 0, 3: 0})
        self.assertEqual(code, 1)

    def test_waiver_turns_a_failure_into_a_warning(self):
        code, md = self.run_main({2: 0, 3: 0}, body="no-integration-test: needs a paid SaaS account")
        self.assertEqual(code, 0)
        self.assertIn("Waived by the PR", md)

    def test_unit_report_is_shown_alongside(self):
        unit = write(self.repo, "u.lcov", lcov([(f"{self.repo}/crates/sink/x/src/sink.rs", {2: 1, 3: 1})]))
        code, md = self.run_main({2: 1, 3: 0}, extra=("--unit-lcov", unit, "--min", "40"))
        self.assertEqual(code, 0)
        self.assertIn("| 2 | 1 | 2 |", md)

    def test_no_in_scope_change_passes(self):
        path = write(self.repo, "i.lcov", "")
        with mock.patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": "", "PR_BODY": ""}):
            self.assertEqual(ic.main(["--lcov", path, "--base", "HEAD"]), 0)


if __name__ == "__main__":
    unittest.main()
