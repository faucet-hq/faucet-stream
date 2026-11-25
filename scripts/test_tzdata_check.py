import importlib.util
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("tzdata_check", os.path.join(HERE, "tzdata-check.py"))
tzdata_check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tzdata_check)

PATCHED = '[[package]]\nname = "chrono-tz"\nversion = "0.10.4"\ndependencies = [\n "chrono",\n]\n'
REGISTRY = (
    '[[package]]\nname = "chrono-tz"\nversion = "0.10.4"\n'
    'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
)


class TzdataCheckTest(unittest.TestCase):
    def test_a_path_resolved_chrono_tz_passes(self):
        self.assertEqual(tzdata_check.lock_problems(PATCHED), [])

    def test_a_registry_chrono_tz_fails(self):
        problems = tzdata_check.lock_problems(PATCHED + REGISTRY)
        self.assertEqual(len(problems), 1)
        self.assertIn("not in effect", problems[0])

    def test_a_lock_without_chrono_tz_fails(self):
        self.assertTrue(tzdata_check.lock_problems('[[package]]\nname = "chrono"\n'))

    def test_release_names_are_read(self):
        self.assertEqual(tzdata_check.news_release("News\n\nRelease 2026f - 2026-10-27 23:16:49 -0700\n"), "2026f")
        self.assertIsNone(tzdata_check.news_release("nothing"))
        self.assertEqual(tzdata_check.generated_release('pub const IANA_TZDB_VERSION: &str = "2026f";'), "2026f")

    def test_the_repository_passes(self):
        self.assertEqual(tzdata_check.check(os.path.join(HERE, "..")), [])


if __name__ == "__main__":
    unittest.main()
