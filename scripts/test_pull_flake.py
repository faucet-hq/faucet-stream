"""Unit tests for pull_flake.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("pull_flake", HERE / "pull_flake.py")
pf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pf)


def run(*tests):
    """A libtest log: (name, captured output or None for a pass)."""
    lines = [f"test {n} ... {'ok' if out is None else 'FAILED'}" for n, out in tests]
    lines += ["", "failures:", ""]
    for n, out in tests:
        if out is not None:
            lines += [f"---- {n} stdout ----", out, ""]
    lines += ["failures:"] + [f"    {n}" for n, out in tests if out is not None]
    lines += ["", "test result: FAILED. 1 passed; 1 failed", "error: test failed, to rerun pass `--test x`"]
    return "\n".join(lines)


class Retryable(unittest.TestCase):
    def test_every_failure_from_a_pull_is_retried(self):
        log = run(("a", None), ("kafka_up", "thread panicked: failed to pull image: toomanyrequests"))
        self.assertTrue(pf.retryable(log))

    def test_an_unrelated_failure_beside_a_pull_is_not_retried(self):
        log = run(("race", "assertion `left == right` failed"), ("kafka_up", "toomanyrequests"))
        self.assertFalse(pf.retryable(log))

    def test_a_pull_message_in_a_passing_test_does_not_excuse_a_failure(self):
        log = "PullImage retrying\n" + run(("race", "assertion failed"))
        self.assertFalse(pf.retryable(log))

    def test_without_test_failures_the_log_decides(self):
        self.assertTrue(pf.retryable("docker: error pulling image: manifest unknown"))
        self.assertFalse(pf.retryable("error[E0425]: cannot find value"))
        self.assertFalse(pf.retryable("toomanyrequests\nerror: test failed, to rerun pass `--lib`"))


if __name__ == "__main__":
    unittest.main()
