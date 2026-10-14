"""Check the independent benchmark oracle on adversarial edge order."""
import unittest
import early_termination_benchmark as benchmark


class OracleTests(unittest.TestCase):
    def test_pair_order_parallel_edges_self_loops_and_cross_root_duplicates(self):
        fixture = {
            "roots": [10, 20], "degree": 2,
            "records": [
                {"$id": 9, "$from": 10, "$to": 5, "kind": "k0", "ordinal": 0},
                {"$id": 2, "$from": 10, "$to": 99, "kind": "k1", "ordinal": 1},
                {"$id": 4, "$from": 10, "$to": 10, "kind": "k0", "ordinal": 0},
                {"$id": 3, "$from": 10, "$to": 99, "kind": "k1", "ordinal": 1},
                {"$id": 6, "$from": 10, "$to": 20, "kind": "k0", "ordinal": 0},
            ],
        }
        for take, selected in [(None, [2, 3, 4, 6, 9, 6]), (1, [2]), (3, [2, 3, 4])]:
            _, expected = benchmark.case(fixture, take, "dense", 2)
            self.assertEqual(expected, {"q0": selected})
        _, expected = benchmark.case(fixture, None, "sparse", 2)
        self.assertEqual(expected, {"q0": [4, 6, 9, 6]})
        _, expected = benchmark.case(fixture, None, "late", 2)
        self.assertEqual(expected, {"q0": [2, 3]})
        payload, expected = benchmark.case(fixture, 3, "dense", 2, sort=True)
        self.assertEqual(expected, {"q0": [9, 6, 6]})
        self.assertIn("order_by", payload["query"]["read"]["entries"][0]["query"]["root"]["id"]["input"]["limit"]["input"])
        for take, empty in [(0, None), (1, None), (10, []), (None, [])]:
            _, expected = benchmark.case(fixture, take, "none", 2)
            self.assertEqual(expected, {"q0": empty})


if __name__ == "__main__":
    unittest.main()
