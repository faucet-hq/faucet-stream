#!/usr/bin/env python3
"""Every connector config variant a user can pick needs an integration test (#967).

A connector usually tests one "happy" variant — plaintext, access-key
credentials, a SQL catalog — and the others break unnoticed (#962: the default
Iceberg REST catalog could not open a file). This gate lists each variant of the
connector config enums that select a code path (auth, credentials, catalog,
mode, TLS, compression, format, strategy) and fails unless an integration test
names it, or `scripts/variant-coverage-exempt.txt` says why it cannot.

A variant counts as covered when a file under the owning crate's `tests/`
(for a `common/*` crate, also the tests of every connector that depends on
it), `crates/interop-tests/tests/` or `cli/tests/` contains `Enum::Variant`,
or one of those crates' `tests/` contains the variant's serde wire name as a
quoted string or a YAML/JSON value (`type: rest`). A `#[default]` variant is
covered by any integration test of its crate (each one that leaves the field
out runs it). Unit tests in `src/` do not count.

Exemption lines: `<crate dir>::<Enum>::<Variant> -- <reason>`. A stale
exemption (the variant is gone or now covered) fails too.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXEMPT = ROOT / "scripts" / "variant-coverage-exempt.txt"
FAMILIES = ("source", "sink", "common", "state")
SELECTING = re.compile(r"(Auth|Credential|Catalog|Mode|Tls|Ssl|Compression|Format|Strategy)")
ENUM_RE = re.compile(r"((?:#\[[^\]]*\]\s*|///[^\n]*\n\s*)*)pub enum (\w+)[^{;]*\{")


@dataclass(frozen=True)
class Variant:
    crate: str
    enum: str
    name: str
    wire: str
    default: bool = False

    @property
    def key(self) -> str:
        return f"{self.crate}::{self.enum}::{self.name}"


def snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def kebab(name: str) -> str:
    return snake(name).replace("_", "-")


def wire_name(variant: str, attrs: str, rename_all: str | None) -> str:
    m = re.search(r'rename\s*=\s*"([^"]+)"', attrs)
    if m:
        return m.group(1)
    if rename_all in ("snake_case",):
        return snake(variant)
    if rename_all in ("kebab-case",):
        return kebab(variant)
    if rename_all in ("lowercase",):
        return variant.lower()
    if rename_all in ("UPPERCASE",):
        return variant.upper()
    if rename_all in ("SCREAMING_SNAKE_CASE",):
        return snake(variant).upper()
    return variant


def enum_body(src: str, start: int) -> str:
    depth = 0
    for i in range(start, len(src)):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[start + 1 : i]
    return ""


def variants_in(body: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    depth = 0
    attrs: list[str] = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch in "{(":
            depth += 1
        elif ch in "})":
            depth -= 1
        elif depth == 0:
            if body.startswith("#[", i):
                end = body.find("]\n", i)
                end = len(body) if end < 0 else end + 1
                attrs.append(body[i:end])
                i = end
                continue
            if body.startswith("//", i):
                i = body.find("\n", i)
                i = len(body) if i < 0 else i
                continue
            m = re.match(r"([A-Z]\w*)", body[i:])
            if m and (i == 0 or not (body[i - 1].isalnum() or body[i - 1] == "_")):
                joined = " ".join(attrs)
                if "serde(skip" not in joined:
                    out.append((m.group(1), joined))
                attrs = []
                i += len(m.group(1))
                continue
        i += 1
    return out


def discover(root: Path) -> list[Variant]:
    found: list[Variant] = []
    for family in FAMILIES:
        for src in sorted((root / "crates" / family).glob("*/src/**/*.rs")):
            crate = f"{family}/{src.relative_to(root / 'crates' / family).parts[0]}"
            text = src.read_text(encoding="utf-8")
            for m in ENUM_RE.finditer(text):
                attrs, name = m.group(1), m.group(2)
                if "Deserialize" not in attrs or not SELECTING.search(name):
                    continue
                if "#[cfg(test)]" in text[: m.start()].rsplit("\n\n", 1)[-1]:
                    continue
                ra = re.search(r'rename_all\s*=\s*"([^"]+)"', attrs)
                body = enum_body(text, m.end() - 1)
                for variant, vattrs in variants_in(body):
                    found.append(
                        Variant(
                            crate,
                            name,
                            variant,
                            wire_name(variant, vattrs, ra.group(1) if ra else None),
                            "#[default]" in vattrs,
                        )
                    )
    return sorted(set(found), key=lambda v: v.key)


def test_texts(root: Path) -> tuple[dict[str, str], str]:
    per_crate: dict[str, str] = {}
    for family in FAMILIES:
        for d in sorted((root / "crates" / family).glob("*/tests")):
            crate = f"{family}/{d.parent.name}"
            per_crate[crate] = "\n".join(p.read_text(encoding="utf-8", errors="replace") for p in sorted(d.rglob("*.rs")))
    shared = []
    for d in (root / "crates" / "interop-tests" / "tests", root / "cli" / "tests"):
        if d.is_dir():
            shared.extend(p.read_text(encoding="utf-8", errors="replace") for p in sorted(d.rglob("*.rs")))
    return per_crate, "\n".join(shared)


def dependents(root: Path) -> dict[str, list[str]]:
    """`common/<x>` -> the source/sink/state crates that depend on it."""
    out: dict[str, list[str]] = {}
    for family in ("source", "sink", "state"):
        for manifest in sorted((root / "crates" / family).glob("*/Cargo.toml")):
            text = manifest.read_text(encoding="utf-8")
            for m in re.finditer(r"^faucet-common-([\w-]+)\s*(?:=|\.)", text, re.M):
                out.setdefault(f"common/{m.group(1)}", []).append(f"{family}/{manifest.parent.name}")
    return out


def covered(
    v: Variant, per_crate: dict[str, str], shared: str, deps: dict[str, list[str]] | None = None
) -> bool:
    path = re.compile(rf"\b{re.escape(v.enum)}::{re.escape(v.name)}\b")
    owners = [v.crate, *(deps or {}).get(v.crate, [])]
    texts = [per_crate.get(c, "") for c in owners]
    if v.default and any(texts):
        return True
    if any(path.search(t) for t in texts) or path.search(shared):
        return True
    wire = re.compile(rf'"{re.escape(v.wire)}"|:\s*{re.escape(v.wire)}\b')
    return any(wire.search(t) for t in texts)


def load_exemptions(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, reason = line.partition(" -- ")
        if not sep or not reason.strip():
            raise SystemExit(f"{path.name}:{n}: expected `<crate>::<Enum>::<Variant> -- <reason>`")
        out[key.strip()] = reason.strip()
    return out


def check(root: Path, exempt_path: Path) -> list[str]:
    variants = discover(root)
    per_crate, shared = test_texts(root)
    deps = dependents(root)
    exempt = load_exemptions(exempt_path)
    errors: list[str] = []
    keys = {v.key for v in variants}
    for v in variants:
        is_covered = covered(v, per_crate, shared, deps)
        if not is_covered and v.key not in exempt:
            errors.append(f"untested: {v.key} (wire `{v.wire}`) — add an integration test or an exemption with a reason")
        if is_covered and v.key in exempt:
            errors.append(f"stale exemption: {v.key} is covered now — remove it from {exempt_path.name}")
    for key in sorted(set(exempt) - keys):
        errors.append(f"stale exemption: {key} no longer exists — remove it from {exempt_path.name}")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--exempt", type=Path, default=EXEMPT)
    parser.add_argument("--list", action="store_true", help="print every variant and whether it is covered")
    args = parser.parse_args(argv)
    if args.list:
        per_crate, shared = test_texts(args.root)
        deps = dependents(args.root)
        for v in discover(args.root):
            print(f"{'ok  ' if covered(v, per_crate, shared, deps) else 'MISS'} {v.key} ({v.wire})")
        return 0
    errors = check(args.root, args.exempt)
    for e in errors:
        print(f"variant-coverage: {e}")
    if errors:
        print(f"variant-coverage: {len(errors)} problem(s)")
        return 1
    print(f"variant-coverage: every selecting config variant has an integration test or an exemption")
    return 0


if __name__ == "__main__":
    sys.exit(main())
