"""Sample parsing, windowing/diffing and lag-histogram math."""

import copy
import json
import unittest

import samples

QUEUE_FIELDS = [
    "retained_bytes",
    "pending_members",
    "pending_operations",
    "uncertain_operations",
    "published_operations",
    "published_entities",
    "committed_batches",
    "commit_conflicts",
    "uncertain_commits",
    "output_retries",
    "blocked_attempts",
    "discarded_operations",
    "queue_reads",
    "queue_read_bytes",
    "queue_read_micros",
    "committed_operations",
    "discovered_operations",
    "acknowledged_operations",
    "censored_acknowledgements",
    "oldest_pending_micros",
    "publication_attempts",
    "publication_attempt_micros",
    "publication_retries",
    "publication_error_retries",
    "deferred_attempts",
]


def cost(**fields):
    return (
        dict.fromkeys(
            (
                "merges",
                "operands",
                "max_operands",
                "input_bytes",
                "output_bytes",
                "nanos",
            ),
            0,
        )
        | fields
    )


def io_row(service="s3", method="PUT", **fields):
    return {
        "service": service,
        "method": method,
        "connector_attempts": 0,
        "request_body_bytes_offered": 0,
        "response_body_bytes_delivered": 0,
        "in_flight": 0,
        "peak_in_flight": 0,
        "headers": {},
        "transport_errors": {},
        "cancelled_before_headers": 0,
        "bodies_complete": 0,
        "bodies_failed": 0,
        "bodies_dropped": 0,
    } | fields


def sample(
    number, *, unix_ms=None, queue=None, lag=None, merge=None, io=(), storage=()
):
    """A well-formed writer sample; lag defaults to agree with the queue."""
    q = dict.fromkeys(QUEUE_FIELDS, 0) | (queue or {})
    return {
        samples.MARKER: 1,
        "role": "writer",
        "sample": number,
        "elapsed_ns": number * 1_000_000_000,
        "unix_ms": 1_000_000 + number * 1000 if unix_ms is None else unix_ms,
        "queue": q,
        "lag": lag
        or {
            "buckets": {},
            "count": q["acknowledged_operations"] - q["censored_acknowledgements"],
            "sum_micros": 0,
            "max_micros": 0,
        },
        "merge": merge or {k: cost() for k in ("partial", "resolved")},
        "io": list(io),
        "storage": list(storage),
    }


def lag(values):
    buckets = {}
    for micros in values:
        floor = samples.bucket_floor(micros)
        buckets[str(floor)] = buckets.get(str(floor), 0) + 1
    return {
        "buckets": buckets,
        "count": len(values),
        "sum_micros": sum(values),
        "max_micros": max(values, default=0),
    }


class BucketMath(unittest.TestCase):
    def test_floors_match_the_server_layout(self):
        self.assertEqual([samples.bucket_floor(m) for m in range(16)], list(range(16)))
        # Reference values from the server's own doc test and unit tests.
        self.assertEqual(samples.bucket_floor(100), 96)
        self.assertEqual(samples.bucket_floor(101), 96)
        self.assertEqual(samples.bucket_floor(5_000), 4_608)
        self.assertEqual(samples.bucket_floor(2**64 - 1), 0xF000_0000_0000_0000)
        for micros in (16, 17, 31, 32, 1_000, 999_999, 2**63):
            floor = samples.bucket_floor(micros)
            self.assertLessEqual(micros - floor, floor // 8)
            self.assertEqual(samples.bucket_floor(floor), floor)

    def test_nearest_rank_quantiles(self):
        histogram = {int(k): v for k, v in lag(range(1, 101))["buckets"].items()}
        self.assertEqual(samples.bucket_quantile(histogram, 0.0), 1)
        self.assertEqual(samples.bucket_quantile(histogram, 0.5), 48)
        self.assertEqual(samples.bucket_quantile(histogram, 0.99), 96)
        self.assertEqual(samples.bucket_quantile(histogram, 1.0), 96)
        self.assertIsNone(samples.bucket_quantile({}, 0.5))
        self.assertEqual(samples.bucket_quantile({3: 1, 96: 2, 4_608: 1}, 0.5), 96)
        self.assertEqual(samples.nearest_rank(0.95, 1), 1)
        self.assertEqual(samples.nearest_rank(0.29, 100), 29)


class Parsing(unittest.TestCase):
    def test_parse_keeps_only_marked_lines(self):
        lines = [
            "\x1b[2m2026-09-25 INFO starting\n",
            json.dumps(sample(0)) + "\n",
            '{"not": "a sample"}\n',
            "{broken json\n",
            json.dumps(sample(1)) + "\n",
        ]
        self.assertEqual([s["sample"] for s in samples.parse(lines)], [0, 1])

    def test_rejects_empty_gapped_mixed_or_restarted_streams(self):
        with self.assertRaises(ValueError):
            samples.parse(["no samples here"])
        with self.assertRaises(ValueError):
            samples.check([sample(0), sample(2)])
        reader = sample(1) | {"role": "reader"}
        with self.assertRaises(ValueError):
            samples.check([sample(0), reader])
        restarted = sample(1) | {"elapsed_ns": 0}
        with self.assertRaises(ValueError):
            samples.check([sample(0) | {"elapsed_ns": 5}, restarted])

    def test_reports_regressions_and_lag_invariant_mismatches(self):
        first = sample(0, queue={"acknowledged_operations": 5})
        second = sample(1, queue={"acknowledged_operations": 4})
        second["lag"]["count"] = 3
        anomalies = samples.check([first, second])
        self.assertIn(
            {"sample": 1, "regressed": "queue.acknowledged_operations"}, anomalies
        )
        self.assertIn({"sample": 1, "lag_count": 3, "expected": 4}, anomalies)
        # Gauges may fall freely.
        gauge = [sample(0, queue={"pending_members": 9}), sample(1)]
        self.assertEqual(samples.check(gauge), [])


class Windows(unittest.TestCase):
    def test_bracket_selects_covering_samples_and_reports_gaps(self):
        stream = [sample(n) for n in range(5)]
        before, after, gaps = samples.bracket(stream, 1_001_500, 1_003_000)
        self.assertEqual((before["sample"], after["sample"]), (1, 3))
        self.assertEqual(gaps, {"start_gap_ms": 500, "end_gap_ms": 0})
        for start, end in (
            (999_000, 1_002_000),
            (1_001_000, 1_009_000),
            (1_002_000, 1_002_000),
        ):
            with self.assertRaises(ValueError):
                samples.bracket(stream, start, end)

    def test_window_diffs_counters_and_keeps_gauges_and_maxima(self):
        before = sample(
            0,
            queue={
                "acknowledged_operations": 10,
                "published_entities": 8,
                "pending_members": 4,
                "committed_operations": 12,
                "queue_read_bytes": 100,
            },
            lag=lag([5, 100]) | {"count": 2},
            merge={
                "partial": cost(merges=3, operands=9, max_operands=5),
                "resolved": cost(),
            },
            io=[io_row(connector_attempts=4, headers={"200": 4}, peak_in_flight=2)],
            storage=[
                {
                    "name": "slatedb.compactor.bytes_compacted",
                    "labels": [],
                    "value": 10,
                },
                {"name": "slatedb.db.l0_sst_count", "labels": [], "value": 7},
                {
                    "name": "slatedb.object_store.request_duration_seconds",
                    "labels": [["api", "put"]],
                    "value": {"count": 2, "sum": 0.5},
                },
                {"name": "slatedb.unchanged", "labels": [], "value": 1},
            ],
        )
        before["queue"]["censored_acknowledgements"] = 8
        middle = copy.deepcopy(before) | {
            "sample": 1,
            "elapsed_ns": 1_000_000_000,
            "unix_ms": 1_001_000,
        }
        middle["queue"] = middle["queue"] | {"pending_members": 30}
        after = sample(
            2,
            queue={
                "acknowledged_operations": 16,
                "published_entities": 13,
                "pending_members": 2,
                "pending_operations": 3,
                "oldest_pending_micros": 7_000,
                "committed_operations": 15,
                "queue_read_bytes": 400,
                "censored_acknowledgements": 9,
            },
            lag=lag([5, 100, 101, 5_000, 20, 30, 40]),
            merge={
                "partial": cost(merges=7, operands=20, max_operands=11),
                "resolved": cost(merges=1),
            },
            io=[
                io_row(
                    connector_attempts=9,
                    request_body_bytes_offered=900,
                    headers={"200": 8, "503": 1},
                    transport_errors={"timeout": 1},
                    in_flight=1,
                    peak_in_flight=6,
                ),
                io_row(
                    "s3", "GET", connector_attempts=2, response_body_bytes_delivered=64
                ),
                io_row("non_s3_or_unsigned", "GET"),
            ],
            storage=[
                {
                    "name": "slatedb.compactor.bytes_compacted",
                    "labels": [],
                    "value": 50,
                },
                {"name": "slatedb.db.l0_sst_count", "labels": [], "value": 3},
                {
                    "name": "slatedb.object_store.request_duration_seconds",
                    "labels": [["api", "put"]],
                    "value": {"count": 5, "sum": 2.0},
                },
                {"name": "slatedb.unchanged", "labels": [], "value": 1},
            ],
        )
        w = samples.window(before, after, [before, middle, after])
        self.assertEqual(w["seconds"], 2.0)
        self.assertEqual(w["samples"], [0, 2])
        self.assertEqual(w["queue"]["acknowledged_operations"], 6)
        self.assertEqual(w["per_second"]["acknowledged_operations"], 3.0)
        self.assertEqual(w["per_second"]["published_entities"], 2.5)
        self.assertEqual(w["queue"]["queue_read_bytes"], 300)
        self.assertNotIn("pending_members", w["queue"])
        self.assertEqual(
            w["gauges"]["pending_members"], {"start": 4, "end": 2, "min": 2, "max": 30}
        )
        self.assertEqual(w["lag"]["count"], 5)
        self.assertEqual(
            w["lag"]["buckets"], {"20": 1, "30": 1, "40": 1, "96": 1, "4608": 1}
        )
        self.assertEqual(w["lag"]["p50_micros"], 40)
        self.assertEqual(w["lag"]["p99_micros"], 4_608)
        self.assertEqual(w["lag"]["mean_micros"], (5_296 - 105) / 5)
        self.assertEqual(w["lag"]["max_micros_lifetime"], 5_000)
        self.assertEqual(w["lag"]["censored"], 1)
        self.assertEqual(w["lag"]["unfinished_operations"], 3)
        self.assertEqual(w["lag"]["unfinished_oldest_micros_lower_bound"], 7_000)
        self.assertEqual(w["merge"]["partial"]["merges"], 4)
        self.assertEqual(w["merge"]["partial"]["operands"], 11)
        self.assertEqual(w["merge"]["partial"]["max_operands_lifetime"], 11)
        self.assertEqual(w["merge"]["resolved"]["merges"], 1)
        put = w["io"]["s3 PUT"]
        self.assertEqual(put["connector_attempts"], 5)
        self.assertEqual(put["request_body_bytes_offered"], 900)
        self.assertEqual(put["headers.200"], 4)
        self.assertEqual(put["headers.503"], 1)
        self.assertEqual(put["transport_errors.timeout"], 1)
        self.assertEqual((put["in_flight_end"], put["peak_in_flight_lifetime"]), (1, 6))
        self.assertEqual(w["io"]["s3 GET"]["response_body_bytes_delivered"], 64)
        self.assertNotIn("non_s3_or_unsigned GET", w["io"])
        self.assertEqual(
            w["storage"],
            {
                "slatedb.compactor.bytes_compacted": {
                    "start": 10,
                    "end": 50,
                    "delta": 40,
                },
                "slatedb.db.l0_sst_count": {"start": 7, "end": 3, "delta": -4},
                "slatedb.object_store.request_duration_seconds{api=put}": {
                    "count": 3,
                    "sum": 1.5,
                },
            },
        )

    def test_lag_window_rejects_inconsistent_histograms(self):
        with self.assertRaises(ValueError):
            samples.lag_window(lag([100, 100]), lag([100]))
        with self.assertRaises(ValueError):
            samples.lag_window(lag([]), lag([100]) | {"count": 2})
        with self.assertRaises(ValueError):
            samples.lag_window(
                lag([]),
                {"buckets": {"17": 1}, "count": 1, "sum_micros": 17, "max_micros": 17},
            )
        empty = samples.lag_window(lag([7]), lag([7]))
        self.assertEqual(
            (empty["count"], empty["p50_micros"], empty["mean_micros"]), (0, None, None)
        )

    def test_timeseries_rows(self):
        rows = samples.timeseries(
            [sample(0, queue={"pending_members": 3, "acknowledged_operations": 2})]
        )
        self.assertEqual(rows[0]["pending_members"], 3)
        self.assertEqual(rows[0]["acknowledged_operations"], 2)
        self.assertEqual(rows[0]["elapsed_s"], 0.0)


if __name__ == "__main__":
    unittest.main()
