#!/usr/bin/env python3
"""Check the agent skills under skills/ (#853).

Always (no binary needed):
  - every skill has a SKILL.md with `name` = its directory, a description
    starting "Use when", and the shared version step;
  - relative links resolve; docs-site links name a page under docs/book/src/;
    repository links and `cli/examples/...` paths name files that exist;
  - no skill pins a faucet release in prose (facts come from the binary);
  - every fenced `yaml` block is a complete faucet document (`version:` or
    `kind:`), never a fragment that nothing validates.

With `--faucet PATH`: every quoted `faucet ...` command and flag exists in that
binary, and every inline pipeline config passes `faucet validate --no-secrets`.

With `--emit-yaml DIR`: write every inline faucet document and every skill
example config to DIR, for a separate `faucet validate` / `faucet hub check`.

Usage (from the repo root):
  scripts/check_skills.py [--faucet PATH] [--emit-yaml DIR]
Tests: python3 scripts/test_check_skills.py
"""

import argparse
import functools
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKILLS = ROOT / "skills"
DOCS_SRC = ROOT / "docs" / "book" / "src"
DOCS_URL = "https://faucet-hq.github.io/faucet-stream/"
REPO_URL = re.compile(r"https://github\.com/faucet-hq/faucet-stream/(?:blob|tree)/[^/\s)]+/([^\s)#`>]+)")
EXAMPLE_PATH = re.compile(r"(?<![\w/.-])(cli/examples/[\w./-]+\.(?:yaml|yml|json))")
FENCE = re.compile(r"^(\s*)```(\w*)\s*$")
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
FLAG = re.compile(r"^--[a-z0-9][a-z0-9-]*")
PINNED_RELEASE = re.compile(r"\bfaucet(?:-cli|-core)?\s+v?\d+\.\d+", re.I)
VERSION_STEP = "## Step 0: use the project's faucet version"
YAML_LANGS = ("yaml", "yml")


def frontmatter(text):
    if not text.startswith("---\n"):
        return None
    end = text.find("\n---", 4)
    if end < 0:
        return None
    meta, key = {}, None
    for line in text[4:end].splitlines():
        m = re.match(r"^([a-z_-]+):\s*(.*)$", line)
        if m:
            key, val = m.group(1), m.group(2).strip()
            meta[key] = "" if val in (">-", ">", "|", "|-") else val
        elif key and line.startswith("  "):
            meta[key] = (meta[key] + " " + line.strip()).strip()
    return meta


def code_blocks(text):
    """(lang, body) for every fenced block."""
    out, lang, indent, buf = [], None, 0, []
    for line in text.splitlines():
        m = FENCE.match(line)
        if m and lang is None:
            lang, indent, buf = m.group(2) or "text", len(m.group(1)), []
        elif line.strip() == "```" and lang is not None:
            out.append((lang, "\n".join(buf)))
            lang = None
        elif lang is not None:
            buf.append(line[indent:] if line[:indent].strip() == "" else line.lstrip())
    return out


def prose(text):
    """The text outside fenced blocks and inline code."""
    out, inside = [], False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            inside = not inside
            continue
        if not inside:
            out.append(re.sub(r"`[^`]*`", "", line))
    return "\n".join(out)


def document_kind(body):
    """`pipeline`, `template` or None (a fragment) for a yaml block."""
    for line in body.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if re.match(r"^version:\s", line):
            return "pipeline"
        if re.match(r"^kind:\s", line):
            return "pipeline" if re.match(r"^kind:\s*pipeline\b", line) else "template"
        return None
    return None


def command_lines(body):
    """Logical `faucet ...` lines, with backslash continuations joined."""
    joined, cur = [], ""
    for raw in body.splitlines():
        line = raw.rstrip()
        cur = f"{cur} {line.rstrip(chr(92)).strip()}" if cur else line.rstrip(chr(92)).strip()
        if not line.endswith("\\"):
            joined.append(cur.strip())
            cur = ""
    if cur:
        joined.append(cur.strip())
    out = []
    for line in joined:
        line = re.sub(r"^\$\s+", "", line)
        line = line.split(" #", 1)[0].strip()
        for part in re.split(r"\s*(?:&&|\|\||;|\|)\s*", line):
            part = re.sub(r"^(?:[A-Z_][A-Z0-9_]*=\S+\s+)+", "", part.strip())
            part = re.sub(r"^mise exec --\s+", "", part)
            if part.startswith("faucet "):
                out.append(part)
    return out


def docs_page(url):
    """The docs/book/src file a docs-site URL names, or None for the site root."""
    path = url[len(DOCS_URL) :].split("#", 1)[0]
    if not path or path == "index.html":
        return None
    if not path.endswith(".html"):
        return DOCS_SRC / "__not_a_page__" / path
    return DOCS_SRC / (path[: -len(".html")] + ".md")


def link_errors(doc, text):
    errors = []
    for target in LINK.findall(text):
        if target.startswith(DOCS_URL):
            page = docs_page(target)
            if page is not None and not page.exists():
                errors.append(f"docs link {target} names no page under docs/book/src/")
            continue
        if re.match(r"^[a-z]+:", target) or target.startswith("#"):
            continue
        local = target.split("#", 1)[0]
        if not (doc.parent / local).resolve().exists():
            errors.append(f"broken link {target}")
    for path in REPO_URL.findall(text):
        if not (ROOT / path).exists():
            errors.append(f"repository link names missing path {path}")
    for path in EXAMPLE_PATH.findall(text):
        if not (ROOT / path).exists():
            errors.append(f"names missing example {path}")
    return errors


class Cli:
    def __init__(self, path):
        self.path = path

    @functools.lru_cache(maxsize=None)
    def help(self, words):
        r = subprocess.run([self.path, *words, "--help"], capture_output=True, text=True)
        return r.stdout if r.returncode == 0 else None

    @functools.lru_cache(maxsize=None)
    def subcommands(self, words):
        text = self.help(words) or ""
        if "Commands:" not in text:
            return set()
        block = text.split("Commands:", 1)[1].split("\n\n", 1)[0]
        return {m.group(1) for m in re.finditer(r"^\s{2}([a-z][a-z0-9-]*)\s", block, re.M)}

    def check(self, line):
        try:
            argv = shlex.split(line)[1:]
        except ValueError as e:
            return [f"unparseable command `{line}`: {e}"]
        words = []
        for tok in argv:
            if tok.startswith(("-", "<", "$")):
                break
            if tok in self.subcommands(tuple(words)):
                words.append(tok)
            else:
                break
        if not words and argv and not argv[0].startswith("-"):
            return [f"unknown subcommand `faucet {argv[0]}` in `{line}`"]
        text = self.help(tuple(words))
        if text is None:
            return [f"`faucet {' '.join(words)} --help` failed for `{line}`"]
        top = self.help(()) or ""
        errs = []
        for tok in argv:
            m = FLAG.match(tok)
            if m and m.group(0) not in text and m.group(0) not in top:
                errs.append(f"unknown flag {m.group(0)} for `faucet {' '.join(words)}` in `{line}`")
        return errs


def validate(cli, path):
    env = dict(os.environ)
    for var in re.findall(r"\$\{(?:env|secret):([A-Za-z_][A-Za-z0-9_]*)\}", Path(path).read_text()):
        env.setdefault(var, "placeholder")
    r = subprocess.run(
        [cli.path, "validate", "--no-secrets", "--no-env-file", str(path)], capture_output=True, text=True, env=env
    )
    return None if r.returncode == 0 else (r.stderr or r.stdout).strip().splitlines()[-1:]


def skill_errors(skill):
    md = skill / "SKILL.md"
    if not md.exists():
        return [f"{skill.name}: missing SKILL.md"]
    text = md.read_text()
    meta = frontmatter(text)
    if not meta:
        return [f"{skill.name}: SKILL.md has no frontmatter"]
    errors = []
    if meta.get("name") != skill.name:
        errors.append(f"{skill.name}: frontmatter name is {meta.get('name')!r}")
    if not meta.get("description", "").startswith("Use when"):
        errors.append(f"{skill.name}: description must start with 'Use when'")
    if VERSION_STEP not in text:
        errors.append(f"{skill.name}: SKILL.md lacks the shared `{VERSION_STEP}` section")
    return errors


def emit_name(rel, n):
    return re.sub(r"[^A-Za-z0-9_.-]+", "__", str(rel.with_suffix(""))) + f"__{n}.yaml"


def skill_docs():
    for doc in sorted(SKILLS.rglob("*.md")):
        if "target" not in doc.relative_to(SKILLS).parts:
            yield doc


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--faucet", help="faucet binary to check commands and inline configs against")
    ap.add_argument("--emit-yaml", metavar="DIR", help="write every inline faucet document to DIR")
    args = ap.parse_args(argv)
    cli = Cli(args.faucet) if args.faucet else None
    if cli and cli.help(()) is None:
        print(f"cannot run `{args.faucet} --help`", file=sys.stderr)
        return 2
    emit = Path(args.emit_yaml) if args.emit_yaml else None
    if emit:
        emit.mkdir(parents=True, exist_ok=True)

    errors, checked_cmds, checked_cfgs, emitted = [], 0, 0, 0
    skill_dirs = sorted(p for p in SKILLS.iterdir() if p.is_dir() and not p.name.startswith("."))
    if not skill_dirs:
        errors.append("no skills found under skills/")
    for skill in skill_dirs:
        errors += skill_errors(skill)

    for doc in skill_docs():
        rel = doc.relative_to(ROOT)
        text = doc.read_text()
        errors += [f"{rel}: {e}" for e in link_errors(doc, text)]
        for m in PINNED_RELEASE.finditer(prose(text)):
            errors.append(f"{rel}: names a faucet release ({m.group(0)!r}); get version facts from the binary")
        for n, (lang, body) in enumerate(code_blocks(text)):
            if lang in ("bash", "sh", "shell", "console", "text") and cli:
                for line in command_lines(body):
                    checked_cmds += 1
                    errors += [f"{rel}: {e}" for e in cli.check(line)]
            if lang not in YAML_LANGS:
                continue
            kind = document_kind(body)
            if kind is None:
                errors.append(f"{rel}: yaml block #{n} is a fragment; use a complete document or name a cli/examples/ file")
                continue
            if emit:
                (emit / emit_name(rel, n)).write_text(body + "\n")
                emitted += 1
            if cli and kind == "pipeline":
                with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
                    f.write(body)
                checked_cfgs += 1
                err = validate(cli, f.name)
                if err:
                    errors.append(f"{rel}: yaml config block #{n} fails validate: {err}")

    for cfg in sorted(SKILLS.glob("*/examples/**/*.yaml")):
        rel = cfg.relative_to(ROOT)
        if emit:
            (emit / emit_name(rel, 0)).write_text(cfg.read_text())
            emitted += 1
        if cli and "pipeline:" in cfg.read_text():
            checked_cfgs += 1
            err = validate(cli, cfg)
            if err:
                errors.append(f"{rel}: fails validate: {err}")

    for e in errors:
        print(f"FAIL {e}")
    summary = f"{len(skill_dirs)} skills, {checked_cmds} commands, {checked_cfgs} configs checked"
    if emit:
        summary += f", {emitted} documents written to {emit}"
    print(f"{summary}; {len(errors)} problem(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
