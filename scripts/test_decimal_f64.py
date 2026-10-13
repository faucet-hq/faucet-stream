"""Unit tests for decimal-f64.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import tempfile
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("decimal_f64", HERE / "decimal-f64.py")
df = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(df)


class Repo:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def write(self, rel, text):
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(textwrap.dedent(text))

    def check(self):
        return df.check(str(self.root))


class DecimalF64Test(unittest.TestCase):
    def test_a_decimal_through_a_float_fails(self):
        r = Repo()
        r.write(
            "crates/source/x/src/lib.rs",
            """
            fn cell(d: Decimal) -> f64 {
                d.to_f64().unwrap()
            }
            """,
        )
        errors = r.check()
        self.assertEqual(len(errors), 1)
        self.assertIn("crates/source/x/src/lib.rs:3", errors[0])

    def test_a_float_far_from_any_decimal_passes(self):
        r = Repo()
        r.write(
            "crates/source/x/src/lib.rs",
            """
            // Decimal columns render as strings.
            fn a() {}
            fn b() {}
            fn c() {}
            fn d() {}
            fn ratio(n: u64) -> f64 { n as f64 }
            """,
        )
        self.assertEqual(r.check(), [])

    def test_tests_comments_and_owner_modules_are_ignored(self):
        r = Repo()
        r.write(
            "crates/source/x/src/lib.rs",
            """
            fn ok() {}
            // let x = decimal.to_f64();
            #[cfg(test)]
            mod tests {
                fn t(d: Decimal) { d.to_f64(); }
            }
            """,
        )
        r.write("crates/source/x/tests/it.rs", "fn t(d: Decimal) { d.to_f64(); }\n")
        r.write("crates/core/src/fidelity.rs", "fn t(d: Decimal) { d.to_f64(); }\n")
        self.assertEqual(r.check(), [])

    def test_an_allow_entry_excuses_a_line_and_must_match(self):
        r = Repo()
        r.write("crates/source/x/src/lib.rs", "fn c(d: Decimal, n: u64) -> f64 { n as f64 }\n")
        r.write(
            "scripts/decimal-f64-allow.txt",
            "# comment\ncrates/source/x/src/lib.rs: n as f64 -- a count\n",
        )
        self.assertEqual(r.check(), [])
        r.write(
            "scripts/decimal-f64-allow.txt",
            "crates/source/x/src/lib.rs: n as f64 -- a count\n"
            "crates/source/y/src/lib.rs: gone -- stale\n",
        )
        errors = r.check()
        self.assertEqual(len(errors), 1)
        self.assertIn("matches no flagged line", errors[0])

    def test_a_malformed_allow_entry_is_reported(self):
        r = Repo()
        r.write("scripts/decimal-f64-allow.txt", "crates/x.rs no reason\n")
        errors = r.check()
        self.assertEqual(len(errors), 1)
        self.assertIn("expected", errors[0])

    def test_main_reports_and_returns_status(self):
        import contextlib
        import io
        import os

        r = Repo()
        r.write("crates/source/x/src/lib.rs", "fn c(d: Decimal) -> f64 { d.to_f64() }\n")
        cwd = os.getcwd()
        try:
            os.chdir(r.root)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(df.main(), 1)
            self.assertIn("decimal-f64:", out.getvalue())
            r.write("crates/source/x/src/lib.rs", "fn c() {}\n")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(df.main(), 0)
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
