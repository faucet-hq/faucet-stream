"""Unit tests for check_skills.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("check_skills", HERE / "check_skills.py")
cs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cs)


class Blocks(unittest.TestCase):
    def test_document_kind(self):
        self.assertEqual(cs.document_kind("# c\nversion: 1\npipeline: {}"), "pipeline")
        self.assertEqual(cs.document_kind("kind: pipeline\nversion: 1"), "pipeline")
        self.assertEqual(cs.document_kind("kind: source-template\nname: x"), "template")
        self.assertIsNone(cs.document_kind("sink:\n  type: file"))
        self.assertIsNone(cs.document_kind(""))

    def test_indented_fences_are_found_and_dedented(self):
        text = "1. Run:\n   ```bash\n   faucet list\n   ```\n2. Then:\n\n   ```yaml\n   version: 1\n   ```\n"
        self.assertEqual(cs.code_blocks(text), [("bash", "faucet list"), ("yaml", "version: 1")])

    def test_command_lines_join_continuations_and_strip_prefixes(self):
        body = "$ faucet validate \\\n  --no-secrets p.yaml  # check\nFOO=1 mise exec -- faucet list && echo ok"
        self.assertEqual(cs.command_lines(body), ["faucet validate --no-secrets p.yaml", "faucet list"])

    def test_prose_drops_fences_and_inline_code(self):
        text = "faucet-cli 1.14 adds\n```bash\nfaucet 1.2\n```\nuse `faucet 1.3` here"
        self.assertEqual(len(cs.PINNED_RELEASE.findall(cs.prose(text))), 1)


class Links(unittest.TestCase):
    def test_docs_page_mapping(self):
        self.assertEqual(cs.docs_page(cs.DOCS_URL + "cookbook/dlq.html#replay"), cs.DOCS_SRC / "cookbook/dlq.md")
        self.assertIsNone(cs.docs_page(cs.DOCS_URL))
        self.assertFalse(cs.docs_page(cs.DOCS_URL + "cookbook/").exists())

    def test_link_errors(self):
        d = Path(tempfile.mkdtemp())
        (d / "ok.md").write_text("x")
        doc = d / "SKILL.md"
        text = (
            "[a](ok.md) [b](missing.md) [c](https://example.com) "
            f"[d]({cs.DOCS_URL}cookbook/dlq.html) [e]({cs.DOCS_URL}nope/missing.html) "
            "`cli/examples/does_not_exist.yaml` `cli/examples/csv_to_jsonl.yaml` "
            "https://github.com/faucet-hq/faucet-stream/blob/main/no/such/file.rs"
        )
        errors = cs.link_errors(doc, text)
        self.assertEqual(len(errors), 4, errors)
        joined = "\n".join(errors)
        for needle in ("missing.md", "nope/missing.html", "does_not_exist.yaml", "no/such/file.rs"):
            self.assertIn(needle, joined)


class Skills(unittest.TestCase):
    def make(self, text):
        d = Path(tempfile.mkdtemp()) / "faucet-x"
        d.mkdir()
        (d / "SKILL.md").write_text(text)
        return d

    def test_complete_skill_passes(self):
        text = f"---\nname: faucet-x\ndescription: >-\n  Use when testing.\n---\n\n{cs.VERSION_STEP}\n"
        self.assertEqual(cs.skill_errors(self.make(text)), [])

    def test_problems_are_reported(self):
        errors = cs.skill_errors(self.make("---\nname: other\ndescription: Does things\n---\n"))
        self.assertEqual(len(errors), 3, errors)

    def test_missing_skill_md(self):
        d = Path(tempfile.mkdtemp()) / "faucet-y"
        d.mkdir()
        self.assertEqual(cs.skill_errors(d), ["faucet-y: missing SKILL.md"])


if __name__ == "__main__":
    unittest.main()


class Flags(unittest.TestCase):
    HELP = {
        (): "Usage: faucet <COMMAND>\n\nOptions:\n      --log-level <LEVEL>  verbosity\n  -h, --help  Print help\n",
        ("backfill",): (
            "Usage: faucet backfill [OPTIONS]\n\nOptions:\n"
            "      --to-bookmark <V>  upper bound; see --to-date\n"
            "  -o, --output <FMT>  format\n"
        ),
    }

    def cli(self):
        c = cs.Cli("faucet")
        c.help = lambda words: self.HELP.get(words)
        c.subcommands = lambda words: {"backfill"} if words == () else set()
        return c

    def test_help_flags_reads_option_lines_only(self):
        self.assertEqual(cs.help_flags(self.HELP[("backfill",)]), {"--to-bookmark", "--output"})

    def test_a_flag_that_prefixes_a_real_one_is_reported(self):
        errs = self.cli().check("faucet backfill --to 2026-01-01")
        self.assertEqual(len(errs), 1)
        self.assertIn("--to", errs[0])

    def test_declared_and_global_flags_pass(self):
        self.assertEqual(self.cli().check("faucet backfill --to-bookmark 5 -o json --log-level info"), [])


class ExampleHubs(unittest.TestCase):
    def test_assemble_hub_merges_sink_templates_and_lists_ids_and_suites(self):
        with tempfile.TemporaryDirectory() as tmp:
            examples = Path(tmp) / "examples"
            (examples / "source-templates" / "acme").mkdir(parents=True)
            (examples / "source-templates" / "acme" / "api.yaml").write_text("kind: source-template\n")
            (examples / "tests" / "acme" / "api").mkdir(parents=True)
            (examples / "tests" / "acme" / "api" / "suite.yaml").write_text("version: 1\n")
            (examples / "tests" / "acme" / "api" / "replay.yaml").write_text("runs: 2\n")
            hub = Path(tmp) / "hub"
            hub.mkdir()
            ids, suites = cs.assemble_hub(examples, hub)
            self.assertEqual(ids, ["acme/api"])
            self.assertEqual(suites, ["tests/acme/api/suite.yaml"])
            self.assertTrue((hub / "sink-templates" / cs.HUB_SINK).with_suffix(".yaml").is_file())

    def test_hub_commands_lint_check_every_source_and_run_every_suite(self):
        cmds = cs.hub_commands(Path("/h"), ["acme/a", "acme/b"], ["tests/x/suite.yaml"])
        self.assertEqual(cmds[0], (["hub", "lint", "--hub", "/h"], None))
        self.assertEqual(
            [c[0][5] for c in cmds if c[0][1] == "check"], ["acme/a", "acme/b"]
        )
        self.assertEqual(cmds[-1], (["template", "test", "tests/x/suite.yaml"], "/h"))

    def test_skill_examples_form_a_hub(self):
        self.assertTrue(cs.example_hubs())
