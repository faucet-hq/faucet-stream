#!/usr/bin/env python3
"""Fail when a connector declares a capability its conformance test does not prove (#878).

The required-assertion table lives in `faucet_conformance::required` (RFC 0013).
This script reads it, with every built-in connector's kind-level capabilities,
from the committed snapshot `crates/conformance/declared-capabilities.json`
(kept fresh by `cli/tests/declared_capabilities.rs` in the Test job, so this
job builds no Rust). It then reads each connector crate's `src/` for the
declarations a descriptor does not carry (trait-method overrides, a buffering
`flush`, a declared `BatchAtomicity`) and its conformance tests for assertion
calls: the crate's `tests/conformance.rs`, plus
`crates/interop-tests/tests/<kind>_conformance.rs` /
`<kind>_<role>_conformance.rs` for checks that need a second connector (a sink
counted back through its source).

A requirement applies to a connector when any of its triggers holds:
  always                 every connector of the role
  capability <field>     the kind-level `Capabilities` flag is on
  write_mode <mode>      the kind-level write modes include <mode>
  overrides <method>     the crate's non-test `src/` defines `fn <method>`
  buffered_flush         the crate defines an `async fn flush` whose body is
                         more than `Ok(())`, or uses `faucet_common_file::delegate_sink!`
  declares <variant>     the crate's non-test `src/` names `BatchAtomicity::<variant>`
and is met when one of those test files calls any of its assertions.

Exemptions: `crates/conformance/conformance-coverage-exempt.txt`, one
`<crate> <requirement-id>: <reason>` per line; the `call_deadline` requirement
also reads `crates/conformance/call-deadline-exempt.txt` (`<crate>: <reason>`).
An exemption for a requirement that does not apply, or is already met, fails
the run, so the list cannot go stale.

Usage: scripts/conformance-coverage.py   (from the repo root)
"""

import json
import os
import re
import sys

SNAPSHOT = "crates/conformance/declared-capabilities.json"
EXEMPT = "crates/conformance/conformance-coverage-exempt.txt"
CALL_EXEMPT = "crates/conformance/call-deadline-exempt.txt"
INTEROP = "crates/interop-tests/tests"
ROLE_DIRS = {"sources": ("source", "crates/source"), "sinks": ("sink", "crates/sink")}


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"//[^\n]*", "", text)


def non_test_source(text):
    """The file without its trailing `#[cfg(test)] mod …` block(s)."""
    m = re.search(r"#\[cfg\(test\)\]\s*(pub(\([^)]*\))?\s+)?mod\s+\w+\s*\{", text)
    return text[: m.start()] if m else text


def crate_sources(crate_dir):
    out = []
    src = os.path.join(crate_dir, "src")
    for root, dirs, files in os.walk(src):
        dirs[:] = sorted(d for d in dirs if d != "tests")
        for f in sorted(files):
            if not f.endswith(".rs") or f == "tests.rs" or f.endswith("_tests.rs"):
                continue
            with open(os.path.join(root, f), encoding="utf-8") as fh:
                out.append(strip_comments(non_test_source(fh.read())))
    return "\n".join(out)


def fn_bodies(text, name):
    for m in re.finditer(r"\bfn\s+" + re.escape(name) + r"\s*\([^)]*\)[^{;]*\{", text):
        depth, i = 1, m.end()
        while i < len(text) and depth:
            depth += {"{": 1, "}": -1}.get(text[i], 0)
            i += 1
        yield text[m.end() : i - 1]


def buffers(src):
    if re.search(r"faucet_common_file::delegate_sink!", src):
        return True
    for body in fn_bodies(src, "flush"):
        if re.sub(r"\s+", "", body) not in ("Ok(())", ""):
            return True
    return False


def applies(trigger, caps, src):
    kind, arg = trigger["kind"], trigger.get("arg")
    if kind == "always":
        return True
    if kind == "capability":
        return caps.get(arg) is True
    if kind == "write_mode":
        return arg in caps.get("write_modes", [])
    if kind == "overrides":
        return re.search(r"\bfn\s+" + re.escape(arg) + r"\s*[<(]", src) is not None
    if kind == "buffered_flush":
        return buffers(src)
    if kind == "declares":
        return re.search(r"\bBatchAtomicity::" + re.escape(arg) + r"\b", src) is not None
    raise ValueError(f"unknown trigger kind {kind!r}")


def calls(test_src, name):
    return re.search(r"\b" + re.escape(name) + r"\s*(::<[^>]*>)?\s*\(", test_src) is not None


def crate_name(crate_dir):
    with open(os.path.join(crate_dir, "Cargo.toml"), encoding="utf-8") as fh:
        m = re.search(r'^name\s*=\s*"([^"]+)"', fh.read(), re.M)
    return m.group(1) if m else os.path.basename(crate_dir)


def parse_exemptions(path, with_requirement):
    """{(crate, requirement): reason} from an exemption file."""
    out, errors = {}, []
    if not os.path.exists(path):
        return out, errors
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            head, sep, reason = line.partition(":")
            parts = head.split()
            if not sep or not reason.strip() or len(parts) != (2 if with_requirement else 1):
                errors.append(f"{path}:{n}: expected `<crate>{' <requirement>' if with_requirement else ''}: <reason>`")
                continue
            key = (parts[0], parts[1] if with_requirement else "call_deadline")
            out[key] = reason.strip()
    return out, errors


def check(root="."):
    """Return the list of failures (empty when every requirement is met)."""
    with open(os.path.join(root, SNAPSHOT), encoding="utf-8") as fh:
        snapshot = json.load(fh)
    exempt, errors = parse_exemptions(os.path.join(root, EXEMPT), True)
    call_exempt, call_errors = parse_exemptions(os.path.join(root, CALL_EXEMPT), False)
    errors += call_errors
    exemptions = {**exempt, **call_exempt}
    used, applicable = set(), set()
    known = set()

    for role_key, (role, base) in ROLE_DIRS.items():
        reqs = [r for r in snapshot.get("requirements", []) if r["role"] == role]
        for kind, caps in sorted(snapshot.get(role_key, {}).items()):
            crate_dir = os.path.join(root, base, kind)
            if not os.path.isdir(crate_dir):
                errors.append(f"{role} `{kind}`: no crate at {base}/{kind}")
                continue
            crate = crate_name(crate_dir)
            known.add(crate)
            src = crate_sources(crate_dir)
            stem = kind.replace("-", "_")
            test_src = ""
            for test_path in (
                os.path.join(crate_dir, "tests", "conformance.rs"),
                os.path.join(root, INTEROP, f"{stem}_conformance.rs"),
                os.path.join(root, INTEROP, f"{stem}_{role}_conformance.rs"),
            ):
                if os.path.exists(test_path):
                    with open(test_path, encoding="utf-8") as fh:
                        test_src += strip_comments(fh.read())
            for req in reqs:
                if not any(applies(t, caps, src) for t in req["triggers"]):
                    continue
                key = (crate, req["id"])
                applicable.add(key)
                met = any(calls(test_src, a) for a in req["assertions"])
                if key in exemptions:
                    used.add(key)
                    if met:
                        errors.append(
                            f"{crate} {req['id']}: exempt, but its conformance tests already call "
                            f"{' / '.join(req['assertions'])} — remove the exemption"
                        )
                    continue
                if not met:
                    errors.append(
                        f"{crate} declares `{req['id']}` ({req['summary']}) but "
                        f"its conformance tests call none of: {', '.join(req['assertions'])}"
                    )

    for key, _reason in sorted(exemptions.items()):
        if key in used:
            continue
        crate, req = key
        where = CALL_EXEMPT if req == "call_deadline" and key in call_exempt else EXEMPT
        if crate not in known:
            errors.append(f"{where}: `{crate}` is not a built-in connector crate")
        elif key not in applicable:
            errors.append(f"{where}: `{crate} {req}` does not apply to that crate — remove it")
    return errors


def main():
    errors = check()
    if errors:
        print("conformance coverage: declared capabilities without their conformance test:")
        for e in errors:
            print(f"  - {e}")
        print(
            "\nCall the named assertion from the crate's tests/conformance.rs, or list it in "
            f"{EXEMPT} with the reason it cannot run in CI. The table is "
            "faucet_conformance::required (docs/book/src/reference/conformance.md)."
        )
        return 1
    print("conformance coverage: every declared capability has its conformance test")
    return 0


if __name__ == "__main__":
    sys.exit(main())
