import os
import re
import sys
import unittest
from pathlib import Path
from unittest import mock

import ci_install

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "ci.yml"
BOUNDED = ("ci.yml", "integration-heavy.yml", "fresh-resolve.yml")


class PlanTest(unittest.TestCase):
    def test_apt_updates_then_installs_inside_sudo_timeout(self):
        commands = ci_install.plan(["apt", "cmake", "sqlite3"], limit=120, have_timeout=True)
        self.assertEqual(len(commands), 2)
        for command in commands:
            self.assertEqual(command[:4], ["sudo", "timeout", "--kill-after=10", "120"])
            self.assertIn("Acquire::Retries=3", command)
            self.assertIn("Acquire::http::Timeout=30", command)
            self.assertIn("DPkg::Lock::Timeout=60", command)
        self.assertEqual(commands[0][-1], "update")
        self.assertEqual(commands[1][-4:], ["install", "-y", "cmake", "sqlite3"])
        self.assertIn("DEBIAN_FRONTEND=noninteractive", commands[1])

    def test_apt_no_update_only_installs(self):
        commands = ci_install.plan(["apt", "--no-update", "libaio1"], have_timeout=True)
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0][-3:], ["install", "-y", "libaio1"])

    def test_apt_without_timeout_binary_still_uses_sudo(self):
        commands = ci_install.plan(["apt", "cmake"], have_timeout=False)
        self.assertEqual(commands[0][:2], ["sudo", "apt-get"])
        self.assertNotIn("timeout", commands[0])

    def test_brew_and_exec(self):
        self.assertEqual(ci_install.plan(["brew", "cmake", "curl"]), [["brew", "install", "cmake", "curl"]])
        self.assertEqual(
            ci_install.plan(["exec", "--", "choco", "install", "nasm"]),
            [["choco", "install", "nasm"]],
        )
        self.assertEqual(ci_install.plan(["exec", "true"]), [["true"]])

    def test_bad_input_is_refused(self):
        for argv in ([], ["apt"], ["apt", "--no-update"], ["brew"], ["exec"], ["exec", "--"], ["yum", "x"]):
            with self.assertRaises(ValueError, msg=argv):
                ci_install.plan(argv)


class InstallTest(unittest.TestCase):
    def run_install(self, results, attempts=2):
        calls, lines, sleeps = [], [], []
        outcomes = iter(results)

        def run(command, limit):
            calls.append(command)
            return next(outcomes)

        code = ci_install.install(
            [["a"], ["b"]], limit=5, attempts=attempts, run=run, sleep=sleeps.append, log=lines.append
        )
        return code, calls, lines, sleeps

    def test_success_runs_each_command_once(self):
        code, calls, _, sleeps = self.run_install([0, 0])
        self.assertEqual(code, 0)
        self.assertEqual(calls, [["a"], ["b"]])
        self.assertEqual(sleeps, [])

    def test_a_stall_is_retried_once_and_then_succeeds(self):
        code, calls, lines, sleeps = self.run_install([None, 0, 0])
        self.assertEqual(code, 0)
        self.assertEqual(calls, [["a"], ["a"], ["b"]])
        self.assertEqual(sleeps, [ci_install.PAUSE_SECS])
        self.assertTrue(any("did not finish within 5s; retrying" in line for line in lines))

    def test_timeout_exit_status_counts_as_a_stall(self):
        code, _, lines, _ = self.run_install([0, 124, 0, 0])
        self.assertEqual(code, 0)
        self.assertTrue(any("was stopped after 5s" in line for line in lines))

    def test_two_failures_fail_the_step(self):
        code, calls, lines, sleeps = self.run_install([100, 100])
        self.assertEqual(code, 1)
        self.assertEqual(calls, [["a"], ["a"]])
        self.assertEqual(len(sleeps), 1)
        self.assertTrue(any("exited with status 100; giving up" in line for line in lines))

    def test_an_interrupted_apt_run_is_repaired_before_the_retry(self):
        self.assertEqual(
            ci_install.repair(["apt", "cmake"], limit=120, have_timeout=True),
            [["sudo", "timeout", "--kill-after=10", "120", "dpkg", "--configure", "-a"]],
        )
        self.assertEqual(ci_install.repair(["brew", "cmake"]), [])
        calls = []
        outcomes = iter([None, 0, 0, 0])

        def run(command, limit):
            calls.append(command)
            return next(outcomes)

        code = ci_install.install(
            [["a"], ["b"]], limit=5, run=run, sleep=lambda _: None, log=lambda _: None,
            repairs=[["fix"]],
        )
        self.assertEqual(code, 0)
        self.assertEqual(calls, [["a"], ["fix"], ["a"], ["b"]])

    def test_a_failed_later_command_restarts_the_attempt(self):
        code, calls, _, _ = self.run_install([0, 1, 0, 0])
        self.assertEqual(code, 0)
        self.assertEqual(calls, [["a"], ["b"], ["a"], ["b"]])


class RunBoundedTest(unittest.TestCase):
    def test_returns_the_exit_code(self):
        self.assertEqual(ci_install.run_bounded([sys.executable, "-c", "raise SystemExit(3)"], 30), 3)

    @unittest.skipUnless(os.name == "posix", "process groups")
    def test_a_command_past_the_limit_is_killed(self):
        with mock.patch.object(ci_install.subprocess.Popen, "wait", autospec=True) as wait:
            wait.side_effect = [ci_install.subprocess.TimeoutExpired("x", 1), 0]
            code = ci_install.run_bounded([sys.executable, "-c", "import time; time.sleep(60)"], 1)
        self.assertIsNone(code)

    def test_a_child_it_may_not_signal_still_counts_as_out_of_time(self):
        popen = mock.Mock()
        popen.pid = 1
        popen.wait.side_effect = ci_install.subprocess.TimeoutExpired("x", 1)
        popen.kill.side_effect = PermissionError()
        with mock.patch.object(ci_install.subprocess, "Popen", return_value=popen), \
                mock.patch.object(ci_install.os, "killpg", side_effect=PermissionError(), create=True):
            self.assertIsNone(ci_install.run_bounded(["sudo", "apt-get"], 1))


class MainTest(unittest.TestCase):
    def test_usage_error_exits_2(self):
        with mock.patch("sys.stderr"):
            self.assertEqual(ci_install.main(["yum", "x"]), 2)

    def test_bad_limit_exits_2(self):
        with mock.patch.dict(os.environ, {"CI_INSTALL_LIMIT_SECS": "0"}), mock.patch("sys.stderr"):
            self.assertEqual(ci_install.main(["exec", "true"]), 2)

    def test_runs_the_command(self):
        with mock.patch.dict(os.environ, {"CI_INSTALL_ATTEMPTS": "1"}):
            self.assertEqual(ci_install.main(["exec", "--", sys.executable, "-c", "pass"]), 0)
            self.assertEqual(ci_install.main(["exec", "--", sys.executable, "-c", "raise SystemExit(1)"]), 1)


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.lines = WORKFLOW.read_text().splitlines()

    def jobs(self, name="ci.yml"):
        found, current, in_jobs = {}, None, False
        for line in (WORKFLOWS / name).read_text().splitlines():
            if line.startswith("jobs:"):
                in_jobs = True
                continue
            if not in_jobs:
                continue
            match = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
            if match:
                current = match.group(1)
                found[current] = []
            elif current:
                found[current].append(line)
        return found

    def test_every_job_has_a_time_limit(self):
        missing = [
            f"{workflow}: {name}"
            for workflow in BOUNDED
            for name, body in self.jobs(workflow).items()
            if not any(re.match(r"^    timeout-minutes:\s*\d+\s*$", line) for line in body)
        ]
        self.assertEqual(missing, [], "jobs without timeout-minutes")

    def test_no_unbounded_package_install(self):
        unbounded = [
            f"{number}: {line.strip()}"
            for number, line in enumerate(self.lines, 1)
            if re.search(r"\bapt-get\s+(-\S+\s+)*(update|install)\b|\bbrew\s+install\b|\bchoco\s+install\b", line)
            and "ci_install.py" not in line
            and not line.lstrip().startswith("#")
        ]
        self.assertEqual(unbounded, [], "package installs that do not go through scripts/ci_install.py")


if __name__ == "__main__":
    unittest.main()
