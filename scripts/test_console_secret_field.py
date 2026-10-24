"""Checks the web console's secret-field classification (cli/src/serve/ui/secret-field.js)."""

import json
import shutil
import subprocess
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parent.parent / "cli/src/serve/ui/secret-field.js"

SECRETS = [
    "password", "key_password", "pkcs12_password", "token", "auth_token", "sas_token",
    "session_token", "client_secret", "hmac_secret", "secret", "secret_key",
    "secret_access_key", "access_key", "account_key", "service_account_key", "credential",
    "api_key",
]
NOT_SECRETS = [
    "next_token_path", "token_path", "token_url", "commit_token_topic", "secret_arn",
    "token_endpoint", "api_key_header", "secret_name", "url", "username", "table",
]


def classify(names):
    script = (
        f"import({json.dumps(MODULE.as_uri())}).then((m) => {{"
        f"const names = {json.dumps(names)};"
        "process.stdout.write(JSON.stringify(names.map((n) => m.isSecretField(n, undefined))));"
        "});"
    )
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        check=True, capture_output=True, text=True,
    ).stdout
    return dict(zip(names, json.loads(out)))


@unittest.skipIf(shutil.which("node") is None, "node is not installed")
class SecretFieldTest(unittest.TestCase):
    def test_secret_names_are_masked(self):
        result = classify(SECRETS)
        self.assertEqual([n for n, v in result.items() if not v], [])

    def test_locator_names_are_not_masked(self):
        result = classify(NOT_SECRETS)
        self.assertEqual([n for n, v in result.items() if v], [])

    def test_title_is_consulted(self):
        script = (
            f"import({json.dumps(MODULE.as_uri())}).then((m) => {{"
            "process.stdout.write(JSON.stringify([m.isSecretField('value', 'API token'),"
            " m.isSecretField('value', 'Region')]));});"
        )
        out = subprocess.run(
            ["node", "--input-type=module", "-e", script],
            check=True, capture_output=True, text=True,
        ).stdout
        self.assertEqual(json.loads(out), [True, False])


if __name__ == "__main__":
    unittest.main()
