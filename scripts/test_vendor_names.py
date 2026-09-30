"""Unit tests for vendor-names.py.

Run: python3 scripts/test_vendor_names.py
"""

import importlib.util
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("vendor_names", HERE / "vendor-names.py")
vn = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vn)


def hit(line):
    return bool(vn.scan("some/file.rs", line))


class Matches(unittest.TestCase):
    def test_every_identifier_spelling(self):
        for line in [
            "HubSpot", "hubspot", "hub_spot", "hub-spot", "HUB_SPOT", "hub spot",
            "quick_books", "QuickBooksClient", "let quick_books_token = 1;",
            "fn jira_server()", "JIRA_TOKEN", "newHubSpot", "ShopifyBulk",
            "fn groups_zip_ga4_rows()", "NET_SUITE", "net-suite-to-bigquery",
        ]:
            self.assertTrue(hit(line), line)

    def test_lower_case_adp_as_a_word(self):
        for line in ["/adp/workers", "./adp-identity.p12", "ADP_P12_PASSWORD", "adp"]:
            self.assertTrue(hit(line), line)
        for line in ["loadPath", "BadParam", "readpipe", "adapter", "8adp9"]:
            self.assertFalse(hit(line), line)

    def test_ordinary_words_do_not_match(self):
        for line in [
            "ORC reads stripe by stripe", "the stripe statistics", "ramp up the rate",
            "a sage choice", "metadata", "shopping", "networks", "sapling",
            "the sales force of a company", "stripes: 3", "zohan",
        ]:
            self.assertFalse(hit(line), line)

    def test_capitalised_ambiguous_names_match(self):
        for line in ["Stripe charges", "STRIPE_TOKEN", "SAP B1", "Ramp cards", "stripe_charges",
                     "state_key: stripe:charges", "https://api.stripe.com"]:
            self.assertTrue(hit(line), line)

    def test_file_names_and_the_allowlist(self):
        self.assertTrue(vn.scan("crates/hubspot/src/lib.rs", "clean"))
        self.assertFalse(vn.scan("scripts/vendor-names.py", "HubSpot"))
        self.assertFalse(vn.scan("some/file.rs", "a generic OAuth1 provider"))

    def test_changelogs_are_skipped(self):
        self.assertTrue(vn.SKIPPED_FILES.search("crates/core/CHANGELOG.md"))
        self.assertFalse(vn.SKIPPED_FILES.search("docs/changelog-notes.md"))


if __name__ == "__main__":
    unittest.main()
