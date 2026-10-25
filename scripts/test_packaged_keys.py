"""Unit tests for packaged-keys.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("packaged_keys", HERE / "packaged-keys.py")
pk = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pk)

BODY = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC96JIXrKdbPnoV" * 2


class IsKey(unittest.TestCase):
    def write(self, name, text):
        p = Path(self.tmp.name) / name
        p.write_text(text)
        return str(p)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_key_inlined_in_rust_source_is_found(self):
        escaped = f'let k = "-----BEGIN PRIVATE KEY-----\\n{BODY}\\n-----END PRIVATE KEY-----\\n";'
        self.assertTrue(pk.is_key(self.write("lib.rs", escaped)))
        raw = f'const K: &str = "-----BEGIN RSA PRIVATE KEY-----\n{BODY}\n-----END RSA PRIVATE KEY-----";'
        self.assertTrue(pk.is_key(self.write("t.rs", raw)))

    def test_a_placeholder_key_is_not_flagged(self):
        fake = '"-----BEGIN PRIVATE KEY-----\\nnope\\n-----END PRIVATE KEY-----\\n"'
        self.assertFalse(pk.is_key(self.write("x.rs", fake)))

    def test_key_containers_by_name_and_suffix(self):
        for name in ("a.der", "a.jks", "a.p8", "id_rsa", "a.key"):
            self.assertTrue(pk.is_key(self.write(name, "x")), name)
        self.assertFalse(pk.is_key(self.write("cert.pem", "-----BEGIN CERTIFICATE-----\n" + BODY)))

    def test_an_excluded_fixture_is_not_packaged(self):
        pkg = {"exclude": ["tests/fixtures/**"]}
        self.assertFalse(pk.packaged("tests/fixtures/k.pem", pkg))
        self.assertTrue(pk.packaged("src/lib.rs", pkg))


if __name__ == "__main__":
    unittest.main()
