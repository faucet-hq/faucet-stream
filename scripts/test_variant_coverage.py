"""Unit tests for variant-coverage.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "variant_coverage", Path(__file__).with_name("variant-coverage.py")
)
vc = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = vc
spec.loader.exec_module(vc)

CONFIG = textwrap.dedent(
    """
    #[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
    #[serde(tag = "type", content = "config", rename_all = "snake_case")]
    pub enum WidgetAuth {
        #[default]
        None,
        /// A bearer token.
        Bearer { token: String },
        #[serde(rename = "api-key")]
        ApiKey(String),
        #[serde(skip)]
        Internal,
    }

    #[derive(Debug, Deserialize)]
    pub enum Unrelated { A, B }
    """
)


class VariantCoverageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "crates/common/widget/src").mkdir(parents=True)
        (self.root / "crates/common/widget/src/config.rs").write_text(CONFIG)
        (self.root / "crates/sink/widget/tests").mkdir(parents=True)
        (self.root / "crates/sink/widget/Cargo.toml").write_text(
            "[dependencies]\nfaucet-common-widget.workspace = true\n"
        )
        self.test_file = self.root / "crates/sink/widget/tests/integration.rs"
        self.exempt = self.root / "exempt.txt"

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, test_body, exempt=""):
        self.test_file.write_text(test_body)
        self.exempt.write_text(exempt)
        return vc.check(self.root, self.exempt)

    def test_discovers_selecting_enums_with_wire_names(self):
        found = {v.name: v for v in vc.discover(self.root)}
        self.assertEqual(set(found), {"None", "Bearer", "ApiKey"})
        self.assertEqual(found["Bearer"].wire, "bearer")
        self.assertEqual(found["ApiKey"].wire, "api-key")
        self.assertTrue(found["None"].default)

    def test_rust_path_and_wire_value_in_a_dependents_tests_count(self):
        errors = self.check('let a = WidgetAuth::Bearer { token };\nlet c = json!({"type": "api-key"});')
        self.assertEqual(errors, [])

    def test_yaml_wire_value_counts(self):
        self.assertEqual(self.check("auth:\n  type: bearer\nWidgetAuth::ApiKey(x)"), [])

    def test_an_untested_variant_fails(self):
        errors = self.check("WidgetAuth::Bearer { token }")
        self.assertEqual(len(errors), 1)
        self.assertIn("common/widget::WidgetAuth::ApiKey", errors[0])

    def test_an_exemption_with_a_reason_passes(self):
        errors = self.check(
            "WidgetAuth::Bearer { token }",
            "common/widget::WidgetAuth::ApiKey -- paid service, no emulator\n",
        )
        self.assertEqual(errors, [])

    def test_stale_exemptions_fail(self):
        errors = self.check(
            "WidgetAuth::Bearer { token }\nWidgetAuth::ApiKey(k)",
            "common/widget::WidgetAuth::ApiKey -- old\ncommon/widget::WidgetAuth::Gone -- removed\n",
        )
        self.assertEqual(len(errors), 2)
        self.assertTrue(all("stale exemption" in e for e in errors))

    def test_an_exemption_needs_a_reason(self):
        self.test_file.write_text("")
        self.exempt.write_text("common/widget::WidgetAuth::ApiKey\n")
        with self.assertRaises(SystemExit):
            vc.check(self.root, self.exempt)

    def test_the_default_variant_needs_some_integration_test(self):
        self.exempt.write_text("")
        keys = [e for e in vc.check(self.root, self.exempt) if "::None " in e]
        self.assertEqual(len(keys), 1)

    def test_main_reports_and_lists(self):
        self.test_file.write_text("WidgetAuth::Bearer { t }\nWidgetAuth::ApiKey(k)")
        self.exempt.write_text("")
        self.assertEqual(vc.main(["--root", str(self.root), "--exempt", str(self.exempt)]), 0)
        self.assertEqual(vc.main(["--root", str(self.root), "--list"]), 0)


if __name__ == "__main__":
    unittest.main()
