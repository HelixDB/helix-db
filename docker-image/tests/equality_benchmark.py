#!/usr/bin/env python3
"""Synthetic equality benchmark on disposable localhost images.

Checks every complete response against the fixture oracle. Empty and rare
matches use the same three indexed predicates and ordinary bound parameters.
Run with --no-seed --ordered to check both ordered projections on the same
fixture. Timings are measurements; planner estimates are not work
counters and are deliberately not reported as measured I/O.
"""

import argparse
import concurrent.futures
import json
import statistics
import threading
import time
from pathlib import Path

import range_benchmark as bench


def lookup(resource_type="pod"):
    predicate = {"and": {"predicates": [
        {"eq": {"left": {"property": prop}, "right": {"param": prop}}}
        for prop in ["tenant", "type", "deleted"]
    ]}}
    predicate["and"]["predicates"].insert(0, {"eq": {
        "left": {"property": "$label"}, "right": {"constant": {"string": "Resource"}}
    }})
    request = bench.batch("read", [{"values": {
        "input": {"nodes_where": {"predicate": predicate}}, "properties": ["id"],
    }}])
    request["parameters"] = {"tenant": bench.TENANT, "type": resource_type, "deleted": True}
    request["query_name"] = "equality_intersection"
    return request


def fixture_rows(size, raw_bytes):
    rows = bench.fixture_rows(size, raw_bytes, False)
    for index, row in enumerate(rows):
        row["deleted"] = index % 997 == 0
    return rows


def type_union_lookup(resource_types):
    request = bench.batch("read", [{"values": {
        "input": {"nodes_where": {"predicate": {"and": {"predicates": [
            {"eq": {"left": {"property": "$label"}, "right": {"constant": {"string": "Resource"}}}},
            {"or": {"predicates": [{"eq": {"left": {"property": "type"}, "right": {"param": f"type-{index}"}}}
                                    for index in range(len(resource_types))]}},
            {"eq": {"left": {"property": "tenant"}, "right": {"param": "tenant"}}},
        ]}}}}, "properties": ["id"],
    }}])
    request["parameters"] = {"tenant": bench.TENANT, **{f"type-{index}": value for index, value in enumerate(resource_types)}}
    request["query_name"] = "equality_type_union"
    return request


def prepare(port, rows):
    for prop in ["tenant", "type", "deleted", "last_seen"]:
        family = "node_range" if prop == "last_seen" else "node_equality"
        spec = {"label": "Resource", "property": prop}
        spec.update({"direction": "asc"} if family == "node_range" else {"unique": False})
        receipt, _ = bench.request(port, bench.batch("write", [{"create_index": {
            "spec": {family: spec}, "if_not_exists": True,
        }}]))
        operation = receipt["q0"].get("operation_id")
        if operation:
            deadline = time.monotonic() + 120
            while True:
                status, _ = bench.request(port, bench.batch("read", [{"get_index_operation": {"operation_id": operation}}]))
                state = status["q0"]["status"]
                if state == "succeeded":
                    break
                if state in ["failed", "blocked", "aborted"] or time.monotonic() >= deadline:
                    raise RuntimeError(status)
                time.sleep(.05)
    for offset in range(0, len(rows), 100):
        roots = [{"add_n": {"label": "Resource", "properties": [
            [key, {"value": {"bool" if isinstance(value, bool) else "i64" if isinstance(value, int) else "string": value}}]
            for key, value in row.items()
        ]}} for row in rows[offset:offset + 100]]
        bench.request(port, bench.batch("write", roots))


def expected_ids(rows, resource_type):
    return sorted(row["id"] for row in rows if row["tenant"] == bench.TENANT
                  and row["type"] == resource_type and row["deleted"])


def check_result(result, expected):
    actual = result["q0"]
    assert sorted(actual, key=lambda row: row["id"]) == [{"id": value} for value in expected], (actual, expected)


def run(args):
    rows = fixture_rows(args.size, args.raw_bytes)
    ports = [args.baseline, args.candidate]
    if not args.no_seed:
        for port in ports:
            prepare(port, rows)
    report = {"rows": len(rows), "raw_bytes": args.raw_bytes, "samples": args.samples,
              "mode": "warm", "workers": args.workers, "churn": args.churn, "cases": []}
    cases = [("rare" if resource_type == "pod" else "empty", lookup(resource_type), expected_ids(rows, resource_type))
             for resource_type in ["pod", "no-matching-type"]]
    cases.append(("equality_type_union", type_union_lookup(["pod", "no-matching-type"]),
                  sorted(row["id"] for row in rows if row["tenant"] == bench.TENANT and row["type"] == "pod")))
    if args.ordered:
        cases = []
        for fixture in ["ordered-range-wide-projection.json", "ordered-range-narrow-projection.json"]:
            for window in ["broad", "narrow"]:
                payload = json.loads((bench.FIXTURES / fixture).read_text())
                cutoff = bench.CUTOFF if window == "broad" else bench.CUTOFF + len(rows) - 100
                payload["parameters"]["since_last_seen_us"] = cutoff
                projection = payload["query"]["read"]["entries"][0]["query"]["root"]["value_map"]["properties"]
                selected = sorted((row for row in rows if row["tenant"] == bench.TENANT and row["type"] == "pod" and row["last_seen"] > cutoff), key=lambda row: -row["last_seen"])
                expected = [{key: row[key] for key in projection if key in row} for row in selected[:1000]]
                cases.append((fixture + ":" + window, payload, expected))
    for case_name, payload, expected in cases:
        measurements = {port: [] for port in ports}
        for sample in range(args.samples + 3):
            for port in ports if sample % 2 == 0 else ports[::-1]:
                with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
                    responses = list(pool.map(lambda _: bench.request(port, payload), range(args.workers)))
                for result, elapsed in responses:
                    if args.ordered:
                        assert bench.normalized(result) == expected, (case_name, port)
                    else:
                        check_result(result, expected)
                    if sample >= 3:
                        measurements[port].append(elapsed)
        timing = {role: {"p50_ms": statistics.median(measurements[port]) * 1000,
                         "p95_ms": bench.percentile(measurements[port], .95) * 1000,
                         "max_ms": max(measurements[port]) * 1000,
                         "n": len(measurements[port])}
                  for role, port in [("baseline", args.baseline), ("candidate", args.candidate)]}
        case = {"case": case_name, "results": len(expected), **timing}
        report["cases"].append(case)
        print(json.dumps(case), flush=True)
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=int, default=18250)
    parser.add_argument("--candidate", type=int, default=18251)
    parser.add_argument("--size", type=int, default=2000)
    parser.add_argument("--raw-bytes", type=int, default=4096)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--no-seed", action="store_true")
    parser.add_argument("--churn", action="store_true")
    parser.add_argument("--ordered", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.size < 1 or args.samples < 1 or args.workers < 1:
        parser.error("size, samples and workers must be positive")
    if args.churn:
        if not args.no_seed:
            parser.error("seed before starting concurrent churn")
        stop = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            writers = [pool.submit(bench.churn, port, stop) for port in [args.baseline, args.candidate]]
            try:
                run(args)
            finally:
                stop.set()
                for writer in writers:
                    print(json.dumps(writer.result()), flush=True)
    else:
        run(args)
