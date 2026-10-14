"""Run summaries: ledger validation, client/server windows, drain and aggregation."""

import json
import tempfile
import unittest
from pathlib import Path

import summarize
from test_samples import lag, sample

T0_NS = 1_700_000_000_000_000_000
T0_MS = T0_NS // 1_000_000
S = 1_000_000_000
MS = 1_000_000
PHASES = [["warmup", 0, 2 * S], ["measure", 2 * S, 6 * S]]


def result(identity, kind, at_ns, outcome, latency_ms=None):
    record = {
        "event": "result",
        "id": identity,
        "kind": kind,
        "request_id": "r",
        "at_ns": at_ns,
        "outcome": outcome,
    }
    if outcome == "client_overload":
        return record | {"finished_ns": at_ns}
    latency = round(latency_ms * MS)
    return record | {
        "latency_ns": latency,
        "finished_ns": at_ns + MS + latency,
        "scheduled_latency_ns": MS + latency,
        "dispatch_lag_ns": MS,
    }


RESULTS = [
    ("write", 0, "acknowledged", 5),
    ("write", S, "acknowledged", 5),
    ("strong", S, "search_ok", 3),
    ("write", 2 * S, "acknowledged", 10),
    ("strong", 2 * S + S // 5, "search_ok", 7),
    ("eventual", 2 * S + 3 * S // 10, "search_ok", 9),
    ("write", 2 * S + S // 2, "acknowledged", 20),
    ("write", 3 * S, "conflict", 2),
    ("strong", 3 * S + S // 5, "http_error", 1),
    ("write", 3 * S + S // 2, "backpressure", 2),
    ("write", 4 * S, "timeout", 1000),
    ("eventual", 4 * S + 2 * S // 5, "request_error", 1),
    ("write", 4 * S + S // 2, "unknown_write", 3),
    ("eventual", 5 * S + 2 * S // 5, "timeout", 1000),
    ("write", 5 * S, "rejected", 1),
    ("write", 5 * S + S // 2, "client_overload", None),
    ("write", 5 * S + 9 * S // 10, "acknowledged", 200),
]


def write_run(root, *, results=None, footer=None, run=None):
    """A complete synthetic run directory; returns its path."""
    root = Path(root)
    (root / "replay").mkdir(parents=True)
    (root / "writer").mkdir()
    (root / "trace-manifest.json").write_text(
        json.dumps({"trace_sha256": "t" * 64, "phases": PHASES})
    )
    rows = results or [
        result(i, *row) for i, row in enumerate(sorted(RESULTS, key=lambda r: r[1]))
    ]
    counts = {}
    for row in rows:
        counts[f"{row['kind']}:{row['outcome']}"] = (
            counts.get(f"{row['kind']}:{row['outcome']}", 0) + 1
        )
    start = {
        "event": "start",
        "trace_sha256": "t" * 64,
        "planned": len(rows),
        "clock_unix_ns": T0_NS,
        "clock_monotonic_ns": 7_000,
        "start_monotonic_ns": 7_000,
    }
    end = footer or {"event": "end", "offered": len(rows), "counts": counts}
    (root / "replay" / "requests.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in [start, *rows, end])
    )
    pending = [5, 5, 4, 3, 2, 1, 2, 1, 0]
    stream = [
        sample(
            n,
            unix_ms=T0_MS + n * 1000,
            queue={
                "acknowledged_operations": 3 * n,
                "pending_operations": pending[n],
                "pending_members": pending[n],
                "committed_operations": 3 * n + pending[n],
                "commit_conflicts": n // 4,
            },
            lag=lag([100] * 3 * n),
        )
        for n in range(9)
    ]
    (root / "writer" / "stdout.log").write_text(
        "log line\n" + "".join(json.dumps(s) + "\n" for s in stream)
    )
    (root / "docker-stats.jsonl").write_text(
        "".join(
            json.dumps(
                {"unix_ms": 0, "role": "writer", "cpu_percent": c, "memory_bytes": m}
            )
            + "\n"
            for c, m in ((50.0, 100), (150.0, 300))
        )
    )
    (root / "resources-reader").mkdir()
    (root / "resources-reader" / "resources.json").write_text(
        json.dumps({"average_cpu_percent": 12.5, "peak_bytes": 4096})
    )
    if run is not None:
        (root / "run.json").write_text(json.dumps(run))
    return root


class Summary(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_client_metrics_by_kind_outcome_and_phase(self):
        summary = summarize.summarize(write_run(self.root / "run"))
        client = summary["client"]
        self.assertEqual(summary["measurement_window_ns"], (2 * S, 6 * S))
        acked = client["latency"]["write"]["acknowledged"]
        self.assertEqual(
            (acked["count"], acked["p50_ms"], acked["p95_ms"], acked["max_ms"]),
            (3, 20.0, 200.0, 200.0),
        )
        self.assertNotIn("client_overload", client["latency"]["write"])
        self.assertEqual(client["latency"]["strong"]["search_ok"]["p99_ms"], 7.0)
        # Only acknowledgements finishing inside [2 s, 6 s): 2.011 s and 2.521 s.
        self.assertEqual(client["acknowledged_writes_per_second"], 0.5)
        self.assertEqual(
            client["offered_per_second"],
            {"write": 2.25, "strong": 0.5, "eventual": 0.75},
        )
        self.assertEqual(
            client["categories"],
            {
                "backpressure_429": 1,
                "conflict_409": 1,
                "client_overload": 1,
                "timeouts": 2,
                "uncertain_writes": 2,
                "rejected_writes": 1,
                "failed_searches": 3,
            },
        )
        self.assertEqual(
            client["outcomes_by_phase"]["warmup"],
            {"strong:search_ok": 1, "write:acknowledged": 2},
        )
        self.assertEqual(client["dispatch_lag"]["write"]["count"], 8)

    def test_server_windows_drain_and_resources(self):
        summary = summarize.summarize(write_run(self.root / "run"))
        writer = summary["server"]["writer"]
        self.assertEqual(
            set(writer["windows"]), {"measurement", "phase:warmup", "phase:measure"}
        )
        measured = writer["windows"]["measurement"]
        self.assertEqual(measured["samples"], [2, 6])
        self.assertEqual(measured["boundary"], {"start_gap_ms": 0, "end_gap_ms": 0})
        self.assertEqual(measured["per_second"]["acknowledged_operations"], 3.0)
        self.assertEqual(measured["queue"]["commit_conflicts"], 1)
        self.assertEqual(
            (measured["lag"]["count"], measured["lag"]["p99_micros"]), (12, 96)
        )
        self.assertEqual(
            measured["gauges"]["pending_members"],
            {"start": 4, "end": 2, "min": 1, "max": 4},
        )
        self.assertEqual(writer["windows"]["phase:warmup"]["samples"], [0, 2])
        self.assertEqual(
            writer["drain"],
            {
                "pending_operations_at_trace_end": 2,
                "pending_members_at_trace_end": 2,
                "seconds_to_empty": 2.0,
                "final_pending_operations": 0,
            },
        )
        self.assertEqual(writer["anomalies"], [])
        self.assertEqual(summary["resources"]["writer"]["average_cpu_percent"], 100.0)
        self.assertEqual(summary["resources"]["writer"]["peak_memory_bytes"], 300)
        self.assertEqual(summary["resources"]["reader"]["peak_memory_bytes"], 4096)
        headline = summary["headline"]
        self.assertEqual(headline["published_ops_per_s"], 3.0)
        self.assertEqual(headline["lag_p99_ms"], 0.096)
        self.assertEqual(headline["unfinished_ops"], 2)
        self.assertEqual(headline["drain_seconds"], 2.0)
        self.assertEqual(headline["writer_cpu_percent"], 100.0)
        self.assertEqual(headline["merge_partial_merges"], 0)
        self.assertEqual(headline["s3_attempts"], 0)

    def test_explicit_window_overrides_phases(self):
        summary = summarize.summarize(
            write_run(self.root / "run"), from_ns=3 * S, until_ns=5 * S
        )
        self.assertEqual(
            summary["server"]["writer"]["windows"]["measurement"]["samples"], [3, 5]
        )
        with self.assertRaises(ValueError):
            summarize.summarize(self.root / "run", from_ns=5 * S, until_ns=5 * S)

    def test_rejects_incomplete_or_inconsistent_ledgers(self):
        rows = [
            result(0, "write", 0, "acknowledged", 1),
            result(1, "write", S, "acknowledged", 1),
        ]
        broken = {
            "duplicate": [rows[0], rows[0] | {"id": 0}],
            "unknown-outcome": [rows[0], rows[1] | {"outcome": "search_ok"}],
        }
        for name, records in broken.items():
            with self.subTest(name), self.assertRaises(ValueError):
                summarize.summarize(write_run(self.root / name, results=records))
        footer = {"event": "end", "offered": 2, "counts": {"write:acknowledged": 1}}
        with self.assertRaises(ValueError):
            summarize.summarize(
                write_run(self.root / "footer", results=rows, footer=footer)
            )
        truncated = write_run(self.root / "truncated")
        ledger = truncated / "replay" / "requests.jsonl"
        ledger.write_text("".join(ledger.read_text().splitlines(keepends=True)[:-1]))
        with self.assertRaises(ValueError):
            summarize.summarize(truncated)
        other = write_run(self.root / "other-trace")
        (other / "trace-manifest.json").write_text(
            json.dumps({"trace_sha256": "u" * 64, "phases": PHASES})
        )
        with self.assertRaises(ValueError):
            summarize.summarize(other)


class Aggregation(unittest.TestCase):
    def test_groups_repetitions_by_screen_layout_and_cache(self):
        runs = [
            {
                "run": {"screen": "combined", "layout": layout, "cache": "warm"},
                "headline": {"write_p99_ms": value, "lag_p99_ms": None},
            }
            for layout, value in (
                ("map", 30.0),
                ("map", 10.0),
                ("map", 20.0),
                ("rows", 5.0),
            )
        ]
        groups = summarize.aggregate(runs)
        self.assertEqual(
            [(g["layout"], g["runs"]) for g in groups], [("map", 3), ("rows", 1)]
        )
        self.assertEqual(
            groups[0]["metrics"]["write_p99_ms"],
            {"median": 20.0, "min": 10.0, "max": 30.0},
        )
        self.assertNotIn("lag_p99_ms", groups[0]["metrics"])
        table = summarize.markdown(groups)
        self.assertIn("| combined/map/warm (n=3) | combined/rows/warm (n=1) |", table)
        self.assertIn("| write_p99_ms | 20 (10-30) | 5 |", table)

    def test_aggregates_saved_summaries(self):
        with tempfile.TemporaryDirectory() as root:
            first = write_run(
                Path(root) / "a", run={"screen": "s", "layout": "map", "cache": "cold"}
            )
            second = write_run(
                Path(root) / "b", run={"screen": "s", "layout": "rows", "cache": "cold"}
            )
            for run in (first, second):
                summary = summarize.summarize(run)
                (run / "summary.json").write_text(json.dumps(summary))
            groups = summarize.aggregate(
                [
                    json.loads((run / "summary.json").read_text())
                    for run in (first, second)
                ]
            )
            self.assertEqual([g["layout"] for g in groups], ["map", "rows"])
            self.assertEqual(groups[0]["metrics"]["published_ops_per_s"]["median"], 3.0)


if __name__ == "__main__":
    unittest.main()
