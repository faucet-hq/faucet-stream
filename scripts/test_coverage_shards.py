import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location(
    "coverage_shards", pathlib.Path(__file__).with_name("coverage_shards.py")
)
cs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cs)

TARGETS = {
    "faucet-cli": ["a", "b", "c", "d", "e"],
    "big": ["t1", "t2", "t3", "t4"],
    "mid": ["t1", "t2"],
    "small": ["t1"],
    "tiny": ["t1"],
}


class PlanLibs(unittest.TestCase):
    def test_every_package_once_and_never_the_cli(self):
        shards = cs.plan_libs(TARGETS, 2)
        flat = [p for s in shards for p in s]
        self.assertEqual(sorted(flat), ["big", "mid", "small", "tiny"])
        self.assertNotIn("faucet-cli", flat)

    def test_balances_by_target_count(self):
        self.assertEqual(cs.plan_libs(TARGETS, 2), [["big"], ["mid", "small", "tiny"]])

    def test_more_shards_than_packages_leaves_some_empty(self):
        self.assertEqual(sum(1 for s in cs.plan_libs(TARGETS, 6) if not s), 2)


class PlanCli(unittest.TestCase):
    def test_contiguous_parts_cover_every_target(self):
        parts = cs.plan_cli(TARGETS, 3)
        self.assertEqual(parts, [["a", "b"], ["c", "d"], ["e"]])

    def test_no_cli_targets(self):
        self.assertEqual(cs.plan_cli({}, 2), [[], []])


class CargoArgs(unittest.TestCase):
    def test_libs_args(self):
        self.assertEqual(cs.cargo_args("libs", 1, 2, TARGETS), ["-p", "big"])

    def test_cli_args(self):
        self.assertEqual(
            cs.cargo_args("cli", 3, 3, TARGETS), ["-p", "faucet-cli", "--test", "e"]
        )

    def test_rejects_bad_input(self):
        with self.assertRaises(ValueError):
            cs.cargo_args("libs", 0, 2, TARGETS)
        with self.assertRaises(ValueError):
            cs.cargo_args("docs", 1, 1, TARGETS)


if __name__ == "__main__":
    unittest.main()
