"""Unit tests for fidelity-coverage.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import contextlib
import importlib.util
import io
import os
import tempfile
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("fidelity_coverage", HERE / "fidelity-coverage.py")
fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fc)


class Repo:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def write(self, rel, text):
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(textwrap.dedent(text))

    def crate(self, role, kind, src="", test=None):
        base = f"crates/{role}/{kind}"
        self.write(f"{base}/Cargo.toml", f'[package]\nname = "faucet-{role}-{kind}"\n')
        self.write(f"{base}/src/lib.rs", src)
        if test is not None:
            self.write(f"{base}/tests/fidelity.rs", test)

    def check(self):
        return fc.check(str(self.root))


RUNS = "fn t() { fidelity::assert_round_trip(&a, &b, Tolerance::exact()); }\n"


class FidelityCoverageTest(unittest.TestCase):
    def test_a_crate_test_counts(self):
        r = Repo()
        r.crate("sink", "x", test=RUNS)
        self.assertEqual(r.check(), [])

    def test_a_converter_test_in_src_counts(self):
        r = Repo()
        r.crate("source", "x", src="#[cfg(test)]\nmod tests {\n" + RUNS + "}\n")
        self.assertEqual(r.check(), [])

    def test_an_interop_test_counts_for_every_crate_it_names(self):
        r = Repo()
        r.crate("source", "a")
        r.crate("sink", "b")
        r.write(
            "crates/interop-tests/tests/pair.rs",
            "use faucet_source_a::A; use faucet_sink_b::B;\n" + RUNS,
        )
        self.assertEqual(r.check(), [])

    def test_a_helper_assertion_counts_and_comments_do_not(self):
        r = Repo()
        r.crate("source", "a", test="fn t() { assert_corpus_read(&s, &c, tol).await; }\n")
        r.crate("sink", "b", test="// fidelity::assert_round_trip(&a, &b, t);\n")
        errors = r.check()
        self.assertEqual(errors, ["faucet-sink-b: does not run the fidelity corpus"])

    def test_exemptions_must_name_an_uncovered_crate(self):
        r = Repo()
        r.crate("sink", "covered", test=RUNS)
        r.crate("sink", "cloud")
        r.write(
            "crates/conformance/fidelity-exempt.txt",
            "# comment\nfaucet-sink-cloud: no emulator\nfaucet-sink-covered: stale\n"
            "faucet-sink-ghost: gone\nbad line\n",
        )
        errors = r.check()
        self.assertIn("faucet-sink-covered: exempt, but it runs the fidelity corpus — remove the exemption", errors)
        self.assertTrue(any("`faucet-sink-ghost` is not a connector crate" in e for e in errors))
        self.assertTrue(any("expected `<crate>: <reason>`" in e for e in errors))
        self.assertFalse(any("cloud" in e for e in errors))

    def test_main_reports_and_returns_status(self):
        r = Repo()
        r.crate("sink", "x")
        cwd = os.getcwd()
        try:
            os.chdir(r.root)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(fc.main(), 1)
            self.assertIn("faucet-sink-x", out.getvalue())
            r.write("crates/sink/x/tests/fidelity.rs", RUNS)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(fc.main(), 0)
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
