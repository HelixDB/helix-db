"""Exercise the actual paired calibration with stable timestamp ties."""
import contextlib
import io
import unittest
from unittest import mock

import range_benchmark as benchmark


class RangeCalibrationTests(unittest.TestCase):
    def run_calibration(self, descending, reverse_ascending_ties=False):
        ascending = [
            {"id": "a", "last_seen": 1},
            {"id": "b", "last_seen": 1},
            {"id": "c", "last_seen": 2},
            {"id": "d", "last_seen": 2},
        ]

        if reverse_ascending_ties:
            ascending = [ascending[1], ascending[0], ascending[3], ascending[2]]

        def request(_port, payload):
            order = payload["query"]["read"]["entries"][0]["query"]["root"]["value_map"]["input"]["limit"]["input"]["order_by"]["order"]
            rows = ascending if order == "asc" else descending
            return {"q": rows}, 0.01

        with mock.patch.object(benchmark, "request", side_effect=request), contextlib.redirect_stdout(io.StringIO()):
            return benchmark.calibrate(1, 4, 2)

    def test_valid_ties_preserve_entity_order(self):
        result = self.run_calibration([
            {"id": "c", "last_seen": 2}, {"id": "d", "last_seen": 2},
            {"id": "a", "last_seen": 1}, {"id": "b", "last_seen": 1},
        ])
        self.assertEqual(result["extra_us_per_visited_entry"], 0)

    def test_reversed_tied_ids_are_rejected(self):
        with self.assertRaises(AssertionError):
            self.run_calibration([
                {"id": "d", "last_seen": 2}, {"id": "c", "last_seen": 2},
                {"id": "b", "last_seen": 1}, {"id": "a", "last_seen": 1},
            ])

    def test_missing_or_changed_rows_are_rejected(self):
        for last in [{"id": "wrong", "last_seen": 1}, {"id": "b", "last_seen": 0}]:
            with self.subTest(last=last), self.assertRaises(AssertionError):
                self.run_calibration([
                    {"id": "c", "last_seen": 2}, {"id": "d", "last_seen": 2},
                    {"id": "a", "last_seen": 1}, last,
                ])

    def test_shared_tie_order_bug_is_rejected(self):
        with self.assertRaises(AssertionError):
            self.run_calibration([
                {"id": "d", "last_seen": 2}, {"id": "c", "last_seen": 2},
                {"id": "b", "last_seen": 1}, {"id": "a", "last_seen": 1},
            ], reverse_ascending_ties=True)
