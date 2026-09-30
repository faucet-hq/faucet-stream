"""Unit tests for connector-deps.py.

Run: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import importlib.util
import os
import tomllib
import unittest
from pathlib import Path

HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("connector_deps", HERE / "connector-deps.py")
cd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cd)


def manifest(text):
    return tomllib.loads(text)


class Violations(unittest.TestCase):
    def test_a_versioned_dev_dependency_on_another_connector_fails(self):
        m = manifest(
            '[package]\nname = "faucet-sink-oracle"\n'
            '[dev-dependencies]\nfaucet-source-oracle.workspace = true\n'
        )
        self.assertEqual(
            cd.violations(m), ["faucet-sink-oracle: [dev-dependencies] faucet-source-oracle"]
        )

    def test_every_section_and_renames_are_checked(self):
        m = manifest(
            '[package]\nname = "faucet-source-file"\n'
            '[dependencies]\nfaucet-core = "1"\nfaucet-common-file = "1"\n'
            'alias = { package = "faucet-sink-jsonl", version = "1" }\n'
            '[build-dependencies]\nfaucet-sink-csv = { path = "../../sink/csv" }\n'
            '[target.\'cfg(unix)\'.dev-dependencies]\nfaucet-source-csv = "1"\n'
        )
        self.assertEqual(
            cd.violations(m),
            [
                "faucet-source-file: [dependencies] faucet-sink-jsonl",
                "faucet-source-file: [build-dependencies] faucet-sink-csv",
                "faucet-source-file: [target.cfg(unix).dev-dependencies] faucet-source-csv",
            ],
        )

    def test_common_crates_are_checked(self):
        m = manifest(
            '[package]\nname = "faucet-common-file"\n'
            '[dev-dependencies]\nfaucet-source-file = "1"\n'
        )
        self.assertEqual(len(cd.violations(m)), 1)

    def test_core_common_and_unpublished_crates_pass(self):
        ok = manifest(
            '[package]\nname = "faucet-sink-s3"\n'
            '[dependencies]\nfaucet-core = "1"\nfaucet-common-file = "1"\n'
            '[dev-dependencies]\nfaucet-conformance = { path = "../../conformance" }\n'
        )
        unpublished = manifest(
            '[package]\nname = "faucet-sink-x"\npublish = false\n'
            '[dev-dependencies]\nfaucet-source-x = "1"\n'
        )
        interop = manifest(
            '[package]\nname = "faucet-interop-tests"\n'
            '[dev-dependencies]\nfaucet-source-x = "1"\n'
        )
        self.assertEqual(cd.violations(ok), [])
        self.assertEqual(cd.violations(unpublished), [])
        self.assertEqual(cd.violations(interop), [])


class Workspace(unittest.TestCase):
    def test_the_workspace_passes(self):
        cwd = os.getcwd()
        os.chdir(HERE.parent)
        try:
            self.assertEqual(cd.main(), 0)
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
