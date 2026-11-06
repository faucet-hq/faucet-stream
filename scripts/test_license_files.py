import importlib.util
import os
import shutil
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("license_files", os.path.join(HERE, "license-files.py"))
license_files = importlib.util.module_from_spec(spec)
spec.loader.exec_module(license_files)


class LicenseFilesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        shutil.copy(os.path.join(HERE, "..", "LICENSE-APACHE"), self.root)
        open(os.path.join(self.root, "LICENSE-MIT"), "w").write("MIT License\n")
        self.write("Cargo.toml", '[workspace]\nmembers = ["crates/*"]\n')
        self.crate("a")

    def write(self, rel, text):
        path = os.path.join(self.root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "w").write(text)

    def crate(self, name, publish=True, link=True):
        self.write(f"crates/{name}/Cargo.toml", f'[package]\nname = "{name}"\n' + ("" if publish else "publish = false\n"))
        if link:
            for f in license_files.LICENSE_FILES:
                os.symlink(os.path.join("..", "..", f), os.path.join(self.root, "crates", name, f))

    def test_linked_crates_pass(self):
        self.assertEqual(license_files.problems(self.root), [])

    def test_a_crate_without_the_files_fails(self):
        self.crate("b", link=False)
        found = license_files.problems(self.root)
        self.assertEqual(len(found), 2)
        self.assertIn("crates/b/LICENSE-APACHE is missing", found[0])

    def test_an_unpublished_crate_is_skipped(self):
        self.crate("c", publish=False, link=False)
        self.assertEqual(license_files.problems(self.root), [])

    def test_a_copy_that_is_not_the_root_file_fails(self):
        self.crate("d", link=False)
        for f in license_files.LICENSE_FILES:
            open(os.path.join(self.root, "crates", "d", f), "w").write("other\n")
        self.assertEqual(len(license_files.problems(self.root)), 2)

    def test_a_paraphrased_apache_text_fails(self):
        open(os.path.join(self.root, "LICENSE-APACHE"), "a").write("extra\n")
        self.assertIn("is not the Apache License 2.0 text", license_files.problems(self.root)[0])


if __name__ == "__main__":
    unittest.main()
