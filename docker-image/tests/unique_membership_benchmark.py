#!/usr/bin/env python3
"""Compare unique membership on two disposable localhost images.

Creates only synthetic records. Every response is checked against an independent
oracle. Run after both images are ready; never pass an existing database port.
"""
import argparse
import concurrent.futures
import json
import statistics
import time

import range_benchmark as bench


def query(values, parameterized=False):
    source = {"nodes_where": {"predicate": {"and": {"predicates": [
        {"eq": {"left": {"property": "$label"}, "right": {"constant": {"string": "UniqueFixture"}}}},
        {"is_in": {"value": {"property": "key"}, "values":
            {"param": "keys"} if parameterized else {"constant": {"i64_array": values}}}},
    ]}}}}
    request = bench.batch("read", [{"values": {"input": source, "properties": ["key"]}}])
    if parameterized:
        request["parameters"] = {"keys": values}
    return request


def prepare(port, size):
    receipt, _ = bench.request(port, bench.batch("write", [{"create_index": {
        "spec": {"node_equality": {"label": "UniqueFixture", "property": "key", "unique": True}},
        "if_not_exists": True,
    }}]))
    operation = receipt["q0"]["operation_id"]
    deadline = time.monotonic() + 120
    while True:
        status, _ = bench.request(port, bench.batch("read", [{"get_index_operation": {"operation_id": operation}}]))
        state = status["q0"]["status"]
        if state == "succeeded":
            break
        if state not in ["queued", "running"] or time.monotonic() >= deadline:
            raise RuntimeError(status)
        time.sleep(.05)
    for start in range(0, size, 100):
        bench.request(port, bench.batch("write", [{"add_n": {"label": "UniqueFixture", "properties": [
            ["key", {"value": {"i64": key}}],
        ]}} for key in range(start, min(start + 100, size))]))


def run(args):
    ports = [args.baseline, args.candidate]
    if not args.no_seed:
        for port in ports:
            prepare(port, args.size)
    for count in [0, 1, 2, 3, 4, 5, 8, 16, 64, 65]:
        for parameterized in [False, True]:
            values = list(range(count)) + ([0] if count else [])
            if 0 < count < 64:
                values.append(args.size + 1)
            expected = [{"key": key} for key in range(min(count, args.size))]
            payload = query(values, parameterized)
            timings = {port: [] for port in ports}
            for iteration in range(args.samples + 2):
                for port in ports if iteration % 2 == 0 else ports[::-1]:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
                        responses = list(pool.map(lambda _: bench.request(port, payload), range(args.workers)))
                    for result, elapsed in responses:
                        assert sorted(result["q0"], key=lambda row: row["key"]) == expected, (port, count, result)
                        if iteration >= 2:
                            timings[port].append(elapsed * 1000)
            print(json.dumps({"rows": args.size, "count": count, "parameterized": parameterized,
                "workers": args.workers, "baseline_p50_ms": statistics.median(timings[args.baseline]),
                "candidate_p50_ms": statistics.median(timings[args.candidate])}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=int, default=18270)
    parser.add_argument("--candidate", type=int, default=18271)
    parser.add_argument("--size", type=int, default=1000)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--no-seed", action="store_true")
    args = parser.parse_args()
    if min(args.size, args.samples, args.workers) < 1 or args.baseline == args.candidate:
        parser.error("positive size/samples/workers and distinct local ports required")
    run(args)
