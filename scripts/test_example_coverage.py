"""Unit tests for example-coverage.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import tempfile
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("example_coverage", HERE / "example-coverage.py")
ec = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ec)

REGISTRY = """
fn builtin_source_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v = Vec::new();
    v.push(("rest", "REST"));
    v.push((
        "postgres-cdc",
        "CDC",
    ));
    v
}
fn builtin_sink_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut v = Vec::new();
    v.push(("file", "File"));
    v
}
"""

CONFIG = """
pub struct PipelineConfig {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(
        default,
        rename = "mirror",
        alias = "replication"
    )]
    pub replication: Option<ReplicationSpec>,
    #[cfg(feature = "policy")]
    pub policy: Option<PolicySpec>,
}
"""

ENUMS = """
pub enum WriteMode {
    /// doc
    #[default]
    Append,
    Upsert,
}
"""


def write(dir, name, text):
    path = Path(dir) / name
    path.write_text(textwrap.dedent(text))
    return str(path)


class Parse(unittest.TestCase):
    def test_registered_kinds_reads_single_and_multi_line_entries(self):
        self.assertEqual(ec.registered_kinds(REGISTRY, "builtin_source_descriptions"), {"rest", "postgres-cdc"})
        self.assertEqual(ec.registered_kinds(REGISTRY, "builtin_sink_descriptions"), {"file"})

    def test_top_level_keys_honour_serde_rename(self):
        self.assertEqual(ec.top_level_keys(CONFIG), {"name", "mirror", "policy"})

    def test_enum_variants_snake_case_or_verbatim(self):
        self.assertEqual(ec.enum_variants(ENUMS, "WriteMode"), {"append", "upsert"})
        self.assertEqual(ec.enum_variants(ENUMS, "WriteMode", case=str), {"Append", "Upsert"})


class Observe(unittest.TestCase):
    def test_collects_connectors_modes_and_blocks(self):
        d = tempfile.mkdtemp()
        a = write(
            d,
            "a.yaml",
            """
            version: 1
            mirror: { mode: snapshot_then_cdc, snapshot: { source: { type: postgres, config: {} } } }
            pipeline:
              sources:
                api: { type: rest, config: { replication_method: { type: Incremental } } }
              sink: { type: file, config: { path: x, write_mode: upsert, key: [id] } }
              transforms:
                - type: sql
                  config: { relations: [{ name: r, source: { type: csv, path: x } }] }
              dlq: { sink: { type: stdout, config: {} } }
            matrix:
              - id: r
                sink: { type: kafka, config: {} }
            """,
        )
        b = write(d, "b.yaml", "kind: source-template\nname: x\n")
        seen = ec.observed([a, b])
        self.assertEqual(seen["source"], {"rest", "postgres"})
        self.assertEqual(seen["sink"], {"file", "stdout", "kafka"})
        self.assertEqual(seen["write_mode"], {"upsert"})
        self.assertEqual(seen["mirror_mode"], {"snapshot_then_cdc"})
        self.assertEqual(seen["replication_method"], {"Incremental"})
        self.assertEqual(seen["config_key"], {"version", "mirror", "pipeline", "matrix"})

    def test_unparseable_files_are_skipped(self):
        d = tempfile.mkdtemp()
        self.assertEqual(ec.observed([write(d, "bad.yaml", "pipeline: [\n")])["source"], set())


class Gaps(unittest.TestCase):
    def test_missing_allowlisted_and_stale(self):
        want = {"source": {"a", "b", "c"}}
        seen = {"source": {"a", "c"}}
        missing, stale = ec.gaps(want, seen, {"source:c"})
        self.assertEqual(missing, ["source:b"])
        self.assertEqual(stale, ["source:c"])

    def test_allowlist_strips_comments(self):
        d = tempfile.mkdtemp()
        path = write(d, "allow.txt", "# why\nsink:csv  # deprecated\n\n")
        self.assertEqual(ec.allowlist(path), {"sink:csv"})
        self.assertEqual(ec.allowlist(str(Path(d) / "missing.txt")), set())


if __name__ == "__main__":
    unittest.main()
