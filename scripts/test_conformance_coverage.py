"""Unit tests for conformance-coverage.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import json
import tempfile
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("conformance_coverage", HERE / "conformance-coverage.py")
cc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cc)

REQUIREMENTS = [
    {"id": "descriptor", "role": "sink", "triggers": [{"kind": "always"}],
     "assertions": ["assert_descriptor_matches_sink"], "summary": "descriptor"},
    {"id": "idempotent_writes", "role": "sink",
     "triggers": [{"kind": "capability", "arg": "idempotent_writes"}],
     "assertions": ["assert_idempotent_replay", "assert_capabilities_truthful"], "summary": "eo"},
    {"id": "overwrite", "role": "sink", "triggers": [{"kind": "write_mode", "arg": "overwrite"}],
     "assertions": ["assert_overwrite_lifecycle"], "summary": "ow"},
    {"id": "buffered_flush", "role": "sink", "triggers": [{"kind": "buffered_flush"}],
     "assertions": ["assert_cancellation_flushes"], "summary": "flush"},
    {"id": "partial_outcomes", "role": "sink",
     "triggers": [{"kind": "overrides", "arg": "write_batch_partial"}],
     "assertions": ["assert_partial_outcomes_truthful"], "summary": "partial"},
    {"id": "atomic_batches", "role": "sink", "triggers": [{"kind": "declares", "arg": "Atomic"}],
     "assertions": ["assert_batch_atomicity_truthful"], "summary": "atomic"},
    {"id": "call_deadline", "role": "sink", "triggers": [{"kind": "always"}],
     "assertions": ["assert_sink_call_deadline"], "summary": "deadline"},
    {"id": "discover", "role": "source", "triggers": [{"kind": "capability", "arg": "discover"}],
     "assertions": ["assert_discover_roundtrips"], "summary": "discover"},
]

ALL_SINK_CALLS = """
conf::assert_descriptor_matches_sink(&d, "x", &cfg, &sink);
assert_sink_call_deadline(|| make(), &rows).await;
"""


class Repo:
    def __init__(self, sinks=None, sources=None):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        snap = {"requirements": REQUIREMENTS, "sources": sources or {}, "sinks": sinks or {}}
        self.write("crates/conformance/declared-capabilities.json", json.dumps(snap))

    def write(self, rel, text):
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(textwrap.dedent(text))

    def crate(self, role, kind, src="", test=None):
        base = f"crates/{role}/{kind}"
        self.write(f"{base}/Cargo.toml", f'[package]\nname = "faucet-{role}-{kind}"\n')
        self.write(f"{base}/src/lib.rs", src)
        if test is not None:
            self.write(f"{base}/tests/conformance.rs", test)

    def check(self):
        return cc.check(str(self.root))

    def close(self):
        self.tmp.cleanup()


def sink_caps(**flags):
    caps = {"write_modes": flags.pop("write_modes", ["append"]), "idempotent_writes": False}
    caps.update(flags)
    return caps


class CoverageTest(unittest.TestCase):
    def setUp(self):
        self.repos = []

    def tearDown(self):
        for r in self.repos:
            r.close()

    def repo(self, **kw):
        r = Repo(**kw)
        self.repos.append(r)
        return r

    def test_plain_sink_with_the_always_checks_passes(self):
        r = self.repo(sinks={"x": sink_caps()})
        r.crate("sink", "x", "impl Sink for X { async fn write_batch() {} }", ALL_SINK_CALLS)
        self.assertEqual(r.check(), [])

    def test_missing_conformance_file_fails_the_always_checks(self):
        r = self.repo(sinks={"x": sink_caps()})
        r.crate("sink", "x")
        errors = r.check()
        self.assertEqual(len(errors), 2)
        self.assertIn("faucet-sink-x declares `descriptor`", errors[0])

    def test_declared_capability_requires_one_of_its_assertions(self):
        r = self.repo(sinks={"x": sink_caps(idempotent_writes=True)})
        r.crate("sink", "x", "", ALL_SINK_CALLS)
        self.assertIn("idempotent_writes", r.check()[0])
        r.crate("sink", "x", "", ALL_SINK_CALLS + "conf::assert_capabilities_truthful(&s, c).await;")
        self.assertEqual(r.check(), [])

    def test_commented_out_call_does_not_count(self):
        r = self.repo(sinks={"x": sink_caps(idempotent_writes=True)})
        r.crate("sink", "x", "", ALL_SINK_CALLS + "// assert_idempotent_replay(&s, c).await;")
        self.assertEqual(len(r.check()), 1)

    def test_interop_conformance_file_counts(self):
        r = self.repo(sinks={"x-y": sink_caps(idempotent_writes=True)})
        r.crate("sink", "x-y", "", ALL_SINK_CALLS)
        self.assertEqual(len(r.check()), 1)
        r.write("crates/interop-tests/tests/x_y_sink_conformance.rs", "assert_idempotent_replay(&s, c).await;")
        self.assertEqual(r.check(), [])

    def test_turbofish_call_counts(self):
        r = self.repo(sinks={"x": sink_caps(idempotent_writes=True)})
        r.crate("sink", "x", "", ALL_SINK_CALLS + "assert_idempotent_replay::<X, _, _>(&s, c).await;")
        self.assertEqual(r.check(), [])

    def test_write_mode_trigger(self):
        r = self.repo(sinks={"x": sink_caps(write_modes=["append", "overwrite"])})
        r.crate("sink", "x", "", ALL_SINK_CALLS)
        self.assertIn("overwrite", r.check()[0])

    def test_override_trigger_ignores_test_modules(self):
        r = self.repo(sinks={"x": sink_caps()})
        src = """
        impl Sink for X {}
        #[cfg(test)]
        mod tests { fn write_batch_partial() {} }
        """
        r.crate("sink", "x", src, ALL_SINK_CALLS)
        self.assertEqual(r.check(), [])
        r.crate("sink", "x", "impl Sink for X { async fn write_batch_partial(&self, r: &[Value]) {} }", ALL_SINK_CALLS)
        self.assertIn("partial_outcomes", r.check()[0])

    def test_trivial_flush_is_not_buffering(self):
        r = self.repo(sinks={"x": sink_caps()})
        src = """
        impl Sink for X {
            async fn flush(&self) -> Result<(), FaucetError> {
                // nothing buffered
                Ok(())
            }
        }
        """
        r.crate("sink", "x", src, ALL_SINK_CALLS)
        self.assertEqual(r.check(), [])

    def test_working_flush_and_delegate_sink_are_buffering(self):
        r = self.repo(sinks={"x": sink_caps(), "y": sink_caps()})
        r.crate("sink", "x", "impl Sink for X { async fn flush(&self) -> R { self.w.flush().await } }", ALL_SINK_CALLS)
        r.crate("sink", "y", "faucet_common_file::delegate_sink!(Y, inner);", ALL_SINK_CALLS)
        errors = r.check()
        self.assertEqual(len(errors), 2)
        self.assertTrue(all("buffered_flush" in e for e in errors))

    def test_declares_trigger(self):
        r = self.repo(sinks={"x": sink_caps()})
        r.crate("sink", "x", "fn batch_atomicity(&self) -> B { BatchAtomicity::Atomic }", ALL_SINK_CALLS)
        self.assertIn("atomic_batches", r.check()[0])

    def test_exemption_with_reason_suppresses_and_stale_exemptions_fail(self):
        r = self.repo(sinks={"x": sink_caps(idempotent_writes=True)})
        r.crate("sink", "x", "", ALL_SINK_CALLS)
        r.write(cc.EXEMPT, "# comment\nfaucet-sink-x idempotent_writes: backend not in CI\n")
        self.assertEqual(r.check(), [])
        r.crate("sink", "x", "", ALL_SINK_CALLS + "assert_idempotent_replay(&s, c).await;")
        self.assertIn("remove the exemption", r.check()[0])

    def test_exemption_for_a_requirement_that_does_not_apply_fails(self):
        r = self.repo(sinks={"x": sink_caps()})
        r.crate("sink", "x", "", ALL_SINK_CALLS)
        r.write(cc.EXEMPT, "faucet-sink-x overwrite: nope\nfaucet-sink-zz descriptor: gone\n")
        errors = r.check()
        self.assertEqual(len(errors), 2)
        self.assertIn("does not apply", errors[0])
        self.assertIn("not a built-in connector crate", errors[1])

    def test_malformed_exemption_fails(self):
        r = self.repo(sinks={"x": sink_caps()})
        r.crate("sink", "x", "", ALL_SINK_CALLS)
        r.write(cc.EXEMPT, "faucet-sink-x descriptor\n")
        self.assertIn("expected", r.check()[0])

    def test_call_deadline_file_exempts_the_call_deadline(self):
        r = self.repo(sinks={"x": sink_caps()})
        r.crate("sink", "x", "", "conf::assert_descriptor_matches_sink(&d, k, &c, &s);")
        r.write(cc.CALL_EXEMPT, "faucet-sink-x: local file writes only\n")
        self.assertEqual(r.check(), [])

    def test_source_capability_and_missing_crate(self):
        r = self.repo(sources={"s": {"discover": True}, "ghost": {}})
        r.crate("source", "s", "", "")
        errors = r.check()
        self.assertIn("no crate at crates/source/ghost", errors[0])
        self.assertIn("discover", errors[1])

    def test_driver_trigger(self):
        caps = sink_caps()
        req = {"kind": "driver"}
        self.assertTrue(cc.applies(req, caps, "inner: DriverSink<SqlSinkDriver<D>>,"))
        self.assertTrue(cc.applies(req, caps, "impl SqlDriver for SqliteDriver {"))
        self.assertTrue(cc.applies(req, caps, "impl<B: X> SinkDriver for FileDriver<B> {"))
        self.assertTrue(cc.applies(req, caps, "impl MessageDriver for NatsDriver {}"))
        self.assertTrue(cc.applies(req, caps, "impl HttpDriver for Endpoint {}"))
        self.assertTrue(cc.applies(req, caps, "inner: FileSink<S3Backend>,"))
        self.assertTrue(cc.applies(req, caps, "inner: DriverSource<RestDriver>,"))
        self.assertFalse(cc.applies(req, caps, "struct NotAFileSinkWrapper;"))
        self.assertFalse(cc.applies(req, caps, "impl Sink for Plain { }"))
        self.assertFalse(cc.applies(req, caps, "use faucet_core::driver::SinkDriver;"))

    def test_unknown_trigger_kind_raises(self):
        with self.assertRaises(ValueError):
            cc.applies({"kind": "nope"}, {}, "")

    def test_main_exit_codes(self):
        r = self.repo(sinks={"x": sink_caps()})
        r.crate("sink", "x", "", ALL_SINK_CALLS)
        import contextlib
        import io
        import os

        cwd = os.getcwd()
        try:
            os.chdir(r.root)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cc.main(), 0)
                r.crate("sink", "x", "", "")
                self.assertEqual(cc.main(), 1)
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
