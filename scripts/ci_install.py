#!/usr/bin/env python3
"""Install CI packages with a time limit and one retry (#1087).

Usage:
    ci_install.py apt [--no-update] PACKAGE...
    ci_install.py brew PACKAGE...
    ci_install.py exec -- COMMAND...

A package install that stalls fails within LIMIT_SECS and is tried once more;
a second failure fails the step. Limits are read from CI_INSTALL_LIMIT_SECS
and CI_INSTALL_ATTEMPTS.
"""

import os
import shutil
import signal
import subprocess
import sys
import time

LIMIT_SECS = 300
ATTEMPTS = 2
PAUSE_SECS = 10
APT_OPTIONS = [
    "-o", "Acquire::Retries=3",
    "-o", "Acquire::http::Timeout=30",
    "-o", "Acquire::https::Timeout=30",
    "-o", "DPkg::Lock::Timeout=60",
]


def root_command(command, limit, have_timeout=None):
    """Wrap a command that must run as root so the limit can stop it."""
    if have_timeout is None:
        have_timeout = shutil.which("timeout") is not None
    if not have_timeout:
        return ["sudo"] + command
    # The limit runs inside sudo: an unprivileged process cannot signal a root one.
    return ["sudo", "timeout", "--kill-after=10", str(limit)] + command


def plan(argv, limit=LIMIT_SECS, have_timeout=None):
    """The commands one attempt runs, in order."""
    if not argv:
        raise ValueError("expected apt, brew or exec")
    kind, rest = argv[0], argv[1:]
    if kind == "apt":
        update = True
        if rest and rest[0] == "--no-update":
            update, rest = False, rest[1:]
        if not rest:
            raise ValueError("apt: no packages named")
        apt = ["apt-get"] + APT_OPTIONS
        commands = []
        if update:
            commands.append(root_command(apt + ["update"], limit, have_timeout))
        commands.append(
            root_command(
                ["env", "DEBIAN_FRONTEND=noninteractive"] + apt + ["install", "-y"] + rest,
                limit,
                have_timeout,
            )
        )
        return commands
    if kind == "brew":
        if not rest:
            raise ValueError("brew: no packages named")
        return [["brew", "install"] + rest]
    if kind == "exec":
        if rest and rest[0] == "--":
            rest = rest[1:]
        if not rest:
            raise ValueError("exec: no command given")
        return [rest]
    raise ValueError(f"unknown mode '{kind}': expected apt, brew or exec")


def run_bounded(command, limit):
    """Run one command; return its exit code, or None when it ran out of time."""
    own_group = os.name == "posix"
    process = subprocess.Popen(command, start_new_session=own_group)
    try:
        # The outer wait is a backstop for a command the inner limit did not stop.
        return process.wait(timeout=limit + 30)
    except subprocess.TimeoutExpired:
        if own_group:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        process.kill()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        return None


def attempt(commands, limit, run=run_bounded, log=print):
    """Run every command of one attempt; return None on success, else the reason."""
    for command in commands:
        shown = " ".join(command)
        log(f"ci_install: {shown}")
        code = run(command, limit)
        if code is None:
            return f"'{shown}' did not finish within {limit}s"
        if code == 124:
            return f"'{shown}' was stopped after {limit}s"
        if code != 0:
            return f"'{shown}' exited with status {code}"
    return None


def install(commands, limit=LIMIT_SECS, attempts=ATTEMPTS, run=run_bounded,
            sleep=time.sleep, log=print):
    """Try the install up to `attempts` times; return the process exit code."""
    for number in range(1, attempts + 1):
        reason = attempt(commands, limit, run, log)
        if reason is None:
            return 0
        if number < attempts:
            log(f"ci_install: attempt {number} of {attempts} failed: {reason}; retrying")
            sleep(PAUSE_SECS)
        else:
            log(f"ci_install: attempt {number} of {attempts} failed: {reason}; giving up")
    return 1


def positive_int(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    value = int(raw)
    if value < 1:
        raise ValueError(f"{name} must be at least 1, got {raw}")
    return value


def main(argv):
    try:
        limit = positive_int("CI_INSTALL_LIMIT_SECS", LIMIT_SECS)
        attempts = positive_int("CI_INSTALL_ATTEMPTS", ATTEMPTS)
        commands = plan(argv, limit)
    except ValueError as error:
        print(f"ci_install: {error}", file=sys.stderr)
        return 2
    return install(commands, limit, attempts)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
