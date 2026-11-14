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

    def test_balances_by_measured_time(self):
        weights = {"big": 10, "mid": 100, "small": 50, "tiny": 45}
        self.assertEqual(cs.plan_libs(TARGETS, 2, weights), [["mid"], ["big", "small", "tiny"]])

    def test_an_unmeasured_package_weighs_its_targets_at_the_median_rate(self):
        weights = {"mid": 40, "small": 30, "tiny": 10}  # 20, 30, 10 s per target
        self.assertEqual(cs.package_weights(TARGETS, weights)["big"], 4 * 20)

    def test_a_stale_file_still_runs_every_package_once(self):
        weights = {"gone": 999, "mid": 5}
        flat = [p for s in cs.plan_libs(TARGETS, 3, weights) for p in s]
        self.assertEqual(sorted(flat), ["big", "mid", "small", "tiny"])

    def test_the_committed_weights_file_loads(self):
        weights = cs.load_weights()
        self.assertIsNotNone(weights)
        self.assertTrue(all(v > 0 for v in weights.values()))

    def test_no_weights_file_means_target_counts(self):
        self.assertIsNone(cs.load_weights(pathlib.Path(__file__).with_name("no-such-file.json")))


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
