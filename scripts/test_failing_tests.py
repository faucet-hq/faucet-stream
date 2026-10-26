"""Unit tests for failing_tests.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("failing_tests", HERE / "failing_tests.py")
tf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tf)

LOG = """\
     Running tests/integration.rs (target/llvm-cov-target/debug/deps/integration-0123456789abcdef)

running 2 tests
test ok_one ... ok
test widening_scale ... FAILED

failures:

---- widening_scale stdout ----

thread 'widening_scale' (62907) panicked at crates/sink/mssql/tests/integration.rs:445:5:
assertion `left == right` failed
  left: Some("1.0000")

failures:
    widening_scale

test result: FAILED. 1 passed; 1 failed
     Running tests/fidelity.rs (target/llvm-cov-target/debug/deps/fidelity-fedcba9876543210)

running 1 test
test \x1b[31mcorpus_round_trip\x1b[0m ... ok
test nested_text ... FAILED

failures:

---- nested_text stdout ----
thread 'nested_text' panicked at crates/interop-tests/tests/fidelity.rs:126:28:
flush: HttpStatus { status: 400 }

failures:
    nested_text

test result: FAILED. 0 passed; 1 failed
error: 2 targets failed:
    `-p faucet-sink-mssql --test integration`
    `-p faucet-interop-tests --test fidelity`
"""


class Parse(unittest.TestCase):
    def test_every_failing_test_is_listed_with_its_binary_and_panic(self):
        f = tf.parse(LOG)
        self.assertEqual([t["test"] for t in f["tests"]], ["widening_scale", "nested_text"])
        self.assertEqual(f["tests"][0]["binary"], "integration (tests/integration.rs)")
        self.assertEqual(f["tests"][1]["binary"], "fidelity (tests/fidelity.rs)")
        self.assertEqual(
            f["tests"][0]["reason"],
            "crates/sink/mssql/tests/integration.rs:445:5: assertion `left == right` failed",
        )
        self.assertIn("HttpStatus", f["tests"][1]["reason"])

    def test_the_no_fail_fast_target_list_is_kept(self):
        f = tf.parse(LOG)
        self.assertEqual(
            f["targets"],
            ["-p faucet-sink-mssql --test integration", "-p faucet-interop-tests --test fidelity"],
        )

    def test_a_fail_fast_rerun_hint_is_a_target(self):
        f = tf.parse("error: test failed, to rerun pass `-p faucet-core --lib`\n")
        self.assertEqual(f["targets"], ["-p faucet-core --lib"])
        self.assertEqual(f["build"], [])

    def test_build_errors_and_crashes_are_reported(self):
        log = (
            "error[E0308]: mismatched types\n"
            "  --> crates/core/src/lib.rs:10:5\n"
            "error: could not compile `faucet-core` (lib) due to 1 previous error\n"
            "error: test failed, to rerun pass `-p x --lib`\n"
            "Caused by:\n"
            "  process didn't exit successfully: `/t/deps/x-0123456789abcdef` (signal: 11, SIGSEGV)\n"
        )
        f = tf.parse(log)
        self.assertEqual(f["build"][0], "crates/core/src/lib.rs:10:5: mismatched types")
        self.assertIn("could not compile `faucet-core` (lib) due to 1 previous error", f["build"])
        self.assertEqual(f["crashes"], ["/t/deps/x-0123456789abcdef (signal: 11, SIGSEGV)"])

    def test_a_downloaded_job_log_with_timestamps_parses(self):
        stamped = "\n".join(f"2026-10-26T06:25:46.1234567Z {l}" for l in LOG.splitlines())
        self.assertEqual(tf.parse(stamped), tf.parse(LOG))

    def test_a_green_log_reports_nothing(self):
        f = tf.parse("     Running tests/a.rs (t/a-0123456789abcdef)\ntest x ... ok\ntest result: ok.\n")
        self.assertFalse(any(f.values()))
        self.assertEqual(tf.render(f), "")


class Main(unittest.TestCase):
    def test_merges_logs_writes_the_summary_and_annotates(self):
        with tempfile.TemporaryDirectory() as d:
            a, b, s = Path(d, "a.log"), Path(d, "b.log"), Path(d, "summary.md")
            a.write_text(LOG)
            b.write_text(LOG + "error: test failed, to rerun pass `-p faucet-cli --lib`\n")
            self.assertEqual(tf.main(["--summary", str(s), str(a), str(b), str(Path(d, "missing.log"))]), 0)
            text = s.read_text()
        self.assertIn("| `widening_scale` | integration (tests/integration.rs) |", text)
        self.assertEqual(text.count("`nested_text`"), 1)
        self.assertIn("- `-p faucet-cli --lib`", text)
        notes = tf.annotations(tf.merge([tf.parse(LOG)]))
        self.assertTrue(notes[0].startswith("::error title=test failed::widening_scale in integration"))

    def test_a_pipe_in_a_panic_message_does_not_break_the_table(self):
        self.assertEqual(tf.cell("a|b\nc"), "a\\|b c")


if __name__ == "__main__":
    unittest.main()
