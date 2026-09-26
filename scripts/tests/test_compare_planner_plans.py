import copy
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "plans", Path(__file__).parents[1] / "compare-planner-plans.py")
plans = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plans)


class PlannerCaptureComparison(unittest.TestCase):
    def setUp(self):
        self.capture = {"schema": 1, "cases": {
            "planned": {"input": {"query": "MATCH (n) RETURN n"}, "outcome": {
                "plan": {"metrics": {"rule_fires": 7, "selected_cost": {
                    "object_reads": 9, "multi_get_calls": 1}}}}},
            "rejected": {"input": {"query": "CALL missing()"},
                         "outcome": {"error": "unsupported procedure"}},
        }}

    def test_exact_replay_and_denominator(self):
        result = plans.compare(self.capture, copy.deepcopy(self.capture))
        self.assertEqual(result["identical"], 2)
        self.assertFalse(result["review_required"])
        candidate = copy.deepcopy(self.capture)
        del candidate["cases"]["rejected"]
        candidate["cases"]["new"] = copy.deepcopy(candidate["cases"]["planned"])
        result = plans.compare(self.capture, candidate)
        self.assertEqual(result["missing"], ["rejected"])
        self.assertEqual(result["added"], ["new"])
        self.assertTrue(result["review_required"])

    def test_improvements_and_rejections_require_review(self):
        candidate = copy.deepcopy(self.capture)
        candidate["cases"]["planned"]["outcome"]["plan"]["metrics"]["selected_cost"]["object_reads"] = 1
        candidate["cases"]["rejected"]["outcome"]["error"] = "different rejection"
        result = plans.compare(self.capture, candidate)
        self.assertTrue(result["review_required"])
        self.assertEqual(len(result["changes"]), 2)
        self.assertEqual(result["changes"][0]["cost"]["object_reads"], {"before": 9, "after": 1})
        self.assertTrue(result["changes"][1]["error_changed"])

    def test_input_and_nonmetric_changes_stay_visible(self):
        candidate = copy.deepcopy(self.capture)
        candidate["cases"]["planned"]["input"]["query"] = "RETURN 1"
        candidate["cases"]["rejected"]["outcome"]["new_field"] = 42
        changes = plans.compare(self.capture, candidate)["changes"]
        self.assertTrue(changes[0]["input_changed"])
        self.assertEqual(len(changes), 2)

    def test_harness_errors_fail_instead_of_passing(self):
        for invalid in [[], {}, {"schema": 2}, {"schema": 1, "cases": {}},
                        {"schema": 1, "cases": {"null_plan": {"input": {}, "outcome": {"plan": None}}}},
                        {"schema": 1, "cases": {"empty_plan": {"input": {}, "outcome": {"plan": {}}}}},
                        {"schema": 1, "cases": {"empty_error": {"input": {}, "outcome": {"error": ""}}}},
                        {"schema": 1, "cases": {"incomplete": {}}},
                        {"schema": 1, "cases": {"ambiguous": {
                            "input": {}, "outcome": {"plan": {}, "error": "bad"}}}}]:
            with self.assertRaises(ValueError):
                plans.compare(self.capture, invalid)


if __name__ == "__main__":
    unittest.main()
