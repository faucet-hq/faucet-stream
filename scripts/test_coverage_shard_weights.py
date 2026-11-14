import importlib.util
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
spec = importlib.util.spec_from_file_location(
    "coverage_shard_weights", pathlib.Path(__file__).with_name("coverage_shard_weights.py")
)
cw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cw)

LOG = """\
Coverage shard (libs-1)\tCollect\t2026-11-13T16:08:00.0000000Z \x1b[1m\x1b[92m     Running\x1b[0m tests/a.rs (target/debug/deps/a-1111)
Coverage shard (libs-1)\tCollect\t2026-11-13T16:08:05.5000000Z test result: ok. 1 passed
Coverage shard (libs-1)\tCollect\t2026-11-13T16:08:10.0000000Z \x1b[1m\x1b[92m     Running\x1b[0m tests/b_c.rs (target/debug/deps/b_c-2222)
Coverage shard (libs-1)\tCollect\t2026-11-13T16:08:40.0000000Z \x1b[1m\x1b[92m     Running\x1b[0m tests/z.rs (target/debug/deps/z-3333)
Coverage shard (libs-1)\tCollect\t2026-11-13T16:09:00.0000000Z test result: ok. 3 passed
"""
TARGETS = {"pkg-one": ["b-c", "a"], "pkg-two": ["z"]}


class BinaryTimes(unittest.TestCase):
    def test_each_binary_runs_until_the_next_or_the_last_result(self):
        self.assertEqual(cw.binary_times(LOG), [("a", 10.0), ("b_c", 30.0), ("z", 20.0)])

    def test_a_binary_without_an_end_is_refused(self):
        with self.assertRaises(ValueError):
            cw.binary_times(LOG.splitlines()[0])


class Attribute(unittest.TestCase):
    def test_binaries_follow_packages_then_targets_in_name_order(self):
        got = cw.attribute(cw.binary_times(LOG), ["pkg-two", "pkg-one"], TARGETS)
        self.assertEqual(got, {"pkg-one": 40.0, "pkg-two": 20.0})

    def test_a_binary_out_of_place_is_named(self):
        times = [("a", 1.0), ("z", 1.0), ("b_c", 1.0)]
        with self.assertRaisesRegex(ValueError, "binary 1 is 'z'"):
            cw.attribute(times, ["pkg-one", "pkg-two"], TARGETS)

    def test_a_missing_binary_is_refused(self):
        with self.assertRaisesRegex(ValueError, "expects 3"):
            cw.attribute([("a", 1.0), ("b_c", 1.0)], ["pkg-one", "pkg-two"], TARGETS)


class Combine(unittest.TestCase):
    def test_averages_over_the_runs_that_measured_a_package(self):
        self.assertEqual(
            cw.combine([{"p": 10.0, "q": 0.2}, {"p": 20.0}]), {"p": 15, "q": 1}
        )


if __name__ == "__main__":
    unittest.main()
