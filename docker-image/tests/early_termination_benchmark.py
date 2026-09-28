#!/usr/bin/env python3
"""Compare local disposable images against an independent write-time oracle.

Seed each empty server once, then compare fixture files. Timing includes the full
HTTP request and JSON transfer. First-request samples are not cold-cache claims.
"""
import argparse
import concurrent.futures
import json
import math
from pathlib import Path
import statistics
import time
import urllib.request


def request(port, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}/v2/query", data=data,
                                 headers={"Content-Type": "application/json"})
    start = time.perf_counter()
    with urllib.request.urlopen(req, timeout=180) as response:
        body = response.read()
    elapsed = time.perf_counter() - start
    return json.loads(body), elapsed * 1000


def batch(kind, roots):
    names = [f"q{i}" for i in range(len(roots))]
    return {"request_type": kind, "query": {kind: {
        "entries": [{"query": {"name": name, "root": root}}
                    for name, root in zip(names, roots)], "returns": names}}}


def nodes(ids):
    return {"nodes": {"reference": {"ids": ids}}}


def properties(values):
    return [[key, {"value": {"string" if isinstance(value, str) else "i64": value}}]
            for key, value in values.items()]


def seed(args):
    roots, leaves, records = [], [], []
    for label, count, ids in [("DemandRoot", args.roots, roots),
                               ("DemandLeaf", args.degree, leaves)]:
        for start in range(0, count, 64):
            queries = [{"id": {"input": {"add_n": {"label": label, "properties":
                properties({"rank": rank, "kind": f"k{rank % 8}", "payload": "x" * args.payload})}}}}
                for rank in range(start, min(start + 64, count))]
            response, _ = request(args.port, batch("write", queries))
            for i in range(len(queries)):
                assert len(response[f"q{i}"]) == 1
                ids.append(response[f"q{i}"][0])

    pending = []

    def flush():
        response, _ = request(args.port, batch("write", [query for query, _ in pending]))
        for i, (_, expected) in enumerate(pending):
            actual = response[f"q{i}"]
            assert len(actual) == len(expected)
            # Use returned IDs only; verify every other field against the input.
            by_pair = {(item["$from"], item["$to"]): item for item in expected}
            for row in actual:
                values = {key: value for key, value in row.items() if key != "$id"}
                assert values == by_pair[(row["$from"], row["$to"])]
                records.append(row)
        pending.clear()

    def edge(from_ids, to_ids, values, label="DemandLink"):
        query = {"edge_properties": {"input": {"add_e": {
            "input": nodes(from_ids), "to": {"ids": to_ids}, "label": label,
            "properties": properties(values)}}}}
        expected = [{"$from": source, "$to": target, "$label": label, **values}
                    for source in from_ids for target in to_ids]
        pending.append((query, expected))
        if len(pending) == 32:
            flush()

    for start in range(0, len(roots), 64):
        group = roots[start:start + 64]
        for i, leaf in enumerate(leaves):
            values = {"kind": f"k{i % 8}", "ordinal": i, "payload": "x" * args.payload}
            if i % 2:
                edge([leaf], group, values)
            else:
                edge(group, [leaf], values)
            # Parallel edges must remain separate occurrences.
            if i % 2:
                edge([leaf], group, values, "AlternateLink")
            else:
                edge(group, [leaf], values, "AlternateLink")
        print(json.dumps({"seeded_roots": min(start + 64, len(roots)), "port": args.port}), flush=True)
    for root in roots:
        edge([root], [root], {})
    if len(roots) > 1:
        edge([roots[0]], [roots[1]], {"kind": "k0", "payload": "shared"})
    if pending:
        flush()
    fixture = {"port": args.port, "roots": roots, "leaves": leaves,
               "records": records, "payload": args.payload, "degree": args.degree}
    args.fixture.write_text(json.dumps(fixture))
    print(json.dumps({"fixture": str(args.fixture), "edges": len(records)}), flush=True)


def case(fixture, take, selectivity, root_count, projection="id", sort=False):
    roots = fixture["roots"][:root_count]
    accepted = {
        "dense": lambda row: row.get("kind") in {f"k{i}" for i in range(8)},
        "sparse": lambda row: row.get("kind") == "k0",
        "late": lambda row: row.get("ordinal") == fixture["degree"] - 1,
        "none": lambda row: row.get("ordinal") == fixture["degree"],
    }[selectivity]
    if selectivity == "dense":
        predicate = {"is_in": {"value": {"property": "kind"},
                               "values": {"constant": {"string_array": [f"k{i}" for i in range(8)]}}}}
    else:
        key, value = ("kind", {"string": "k0"}) if selectivity == "sparse" else (
            "ordinal", {"i64": fixture["degree"] - (selectivity == "late")})
        predicate = {"eq": {"left": {"property": key}, "right": {"constant": value}}}
    traversal = {"where": {"input": {"both_e": {"input": nodes(roots)}}, "predicate": predicate}}
    adjacent = {root: [] for root in roots}
    for row in fixture["records"]:
        if accepted(row):
            for root in {row["$from"], row["$to"]} & adjacent.keys():
                adjacent[root].append(row)
    rows = [row for root in roots for row in sorted(adjacent[root], key=lambda r: r["$id"])]
    if sort:
        traversal = {"order_by": {"input": traversal, "property": "$id", "order": "desc"}}
        rows.sort(key=lambda row: row["$id"], reverse=True)
    if take is not None:
        traversal = {"limit": {"input": traversal, "count": {"literal": take}}}
        rows = rows[:take]
    payload = batch("read", [{projection: {"input": traversal}}])
    expected = {"q0": ([row["$id"] for row in rows] if projection == "id" else rows) or (None if take in (0, 1) else [])}
    return payload, expected


def verify(args):
    fixture = json.loads(args.fixture.read_text())
    roots = fixture["roots"]
    identity = {"root": "context"}
    empty = {"root": {"limit": {"input": "context", "count": {"literal": 0}}}}
    for operation, config in [
        ("union", {"traversals": [identity, identity]}),
        ("coalesce", {"traversals": [empty, identity]}),
        ("optional", {"traversal": empty}),
        ("repeat", {"config": {"traversal": identity, "times": 3, "emit": "before", "max_depth": 3}}),
    ]:
        child = {operation: {"input": nodes(roots), **config}}
        query = {"id": {"input": {"limit": {"input": child, "count": {"literal": 1}}}}}
        actual, _ = request(fixture["port"], batch("read", [query]))
        assert actual == {"q0": [roots[0]]}, (operation, actual)
    for take in (0, 1, len(roots)):
        for before in (True, False):
            def update(input_node, value):
                return {"set_property": {"input": input_node, "name": "demand_marker", "value": {"value": {"i64": value}}}}
            request(fixture["port"], batch("write", [update(nodes(roots), 0)]))
            limited = lambda node: {"limit": {"input": node, "count": {"literal": take}}}
            changed = update(limited(nodes(roots)), 1) if before else limited(update(nodes(roots), 1))
            request(fixture["port"], batch("write", [changed]))
            read = {"id": {"input": {"where": {"input": nodes(roots), "predicate": {
                "eq": {"left": {"property": "demand_marker"}, "right": {"constant": {"i64": 1}}}}}}}}
            actual, _ = request(fixture["port"], batch("read", [read]))
            expected = roots[:take] if before else roots
            assert actual == {"q0": expected}, ("mutation", take, before, actual)
    print(json.dumps({"port": fixture["port"], "nested_and_mutation_contracts": "passed"}), flush=True)


def cold(args):
    import subprocess
    fixtures = [json.loads(path.read_text()) for path in (args.baseline, args.candidate)]
    containers = [args.baseline_container, args.candidate_container]
    for selectivity in ("dense", "late", "none"):
        for take in (0, 1, 10, 100, None):
            runs = [case(f, take, selectivity, len(f["roots"])) for f in fixtures]
            timings = [[], []]
            for iteration in range(args.samples):
                for index in ([0, 1] if iteration % 2 == 0 else [1, 0]):
                    subprocess.run(["docker", "restart", containers[index]], check=True, stdout=subprocess.DEVNULL)
                    deadline = time.monotonic() + 60
                    while True:
                        try:
                            with urllib.request.urlopen(f"http://127.0.0.1:{fixtures[index]['port']}/healthz", timeout=2) as response:
                                if response.status == 200:
                                    break
                        except (OSError, urllib.error.URLError):
                            if time.monotonic() >= deadline:
                                raise
                        time.sleep(0.05)
                    actual, elapsed = request(fixtures[index]["port"], runs[index][0])
                    assert actual == runs[index][1], (selectivity, take, index)
                    timings[index].append(elapsed)
            medians = [statistics.median(values) for values in timings]
            print(json.dumps({"selectivity": selectivity, "take": take,
                "cache_state": "server restarted; host filesystem cache retained",
                "baseline_p50_ms": medians[0], "candidate_p50_ms": medians[1],
                "samples_ms": timings, "speedup": medians[0] / medians[1],
                "ordered_results_verified": True}), flush=True)


def compare(args):
    import hashlib
    fixtures = [json.loads(path.read_text()) for path in (args.baseline, args.candidate)]
    assert fixtures[0]["port"] != fixtures[1]["port"]
    assert [(len(f["roots"]), f["degree"], f["payload"]) for f in fixtures][0] == (
        len(fixtures[1]["roots"]), fixtures[1]["degree"], fixtures[1]["payload"])
    for roots in sorted({1, len(fixtures[0]["roots"])}):
        for selectivity in ("dense", "sparse", "late", "none"):
            for take in (0, 1, 10, 100, None):
                runs = [case(f, take, selectivity, roots, args.projection, args.sort) for f in fixtures]
                timings = [[], []]
                first = [[], []]
                for iteration in range(args.samples + 2):
                    for index in ([0, 1] if iteration % 2 == 0 else [1, 0]):
                        payload, expected = runs[index]
                        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
                            responses = list(pool.map(lambda _: request(fixtures[index]["port"], payload), range(args.workers)))
                        for actual, elapsed in responses:
                            assert actual == expected, (selectivity, take, roots, index, "ordered result mismatch", actual, expected)
                            if iteration >= 2:
                                timings[index].append(elapsed)
                            elif iteration == 0:
                                first[index].append(elapsed)
                medians = [statistics.median(values) for values in timings]
                print(json.dumps({"selectivity": selectivity, "take": take, "roots": roots,
                    "degree": fixtures[0]["degree"], "payload_bytes": fixtures[0]["payload"],
                    "projection": args.projection, "workers": args.workers, "full_input_sort": args.sort,
                    "baseline_p50_ms": medians[0], "candidate_p50_ms": medians[1],
                    "speedup": medians[0] / medians[1], "samples_ms": timings,
                    "p95_ms": [sorted(values)[math.ceil(0.95 * len(values)) - 1] for values in timings],
                    "first_request_ms": first, "ordered_results_verified": True,
                    "result_sha256": [hashlib.sha256(json.dumps(r[1], sort_keys=True).encode()).hexdigest() for r in runs]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    seed_parser = commands.add_parser("seed")
    seed_parser.add_argument("--port", type=int, required=True)
    seed_parser.add_argument("--fixture", type=Path, required=True)
    seed_parser.add_argument("--roots", type=int, default=128)
    seed_parser.add_argument("--degree", type=int, default=128)
    seed_parser.add_argument("--payload", type=int, default=64)
    verification = commands.add_parser("verify")
    verification.add_argument("--fixture", type=Path, required=True)
    cold_parser = commands.add_parser("cold")
    cold_parser.add_argument("--baseline", type=Path, required=True)
    cold_parser.add_argument("--candidate", type=Path, required=True)
    cold_parser.add_argument("--baseline-container", required=True)
    cold_parser.add_argument("--candidate-container", required=True)
    cold_parser.add_argument("--samples", type=int, default=7)
    comparison = commands.add_parser("compare")
    comparison.add_argument("--baseline", type=Path, required=True)
    comparison.add_argument("--candidate", type=Path, required=True)
    comparison.add_argument("--sort", action="store_true", help="sort the complete filtered input before limiting")
    comparison.add_argument("--samples", type=int, default=7)
    comparison.add_argument("--workers", type=int, default=1)
    comparison.add_argument("--projection", choices=("id", "edge_properties"), default="id")
    args = parser.parse_args()
    if args.command == "seed":
        if args.roots < 1 or args.degree < 1 or args.payload < 0 or args.fixture.exists():
            parser.error("positive roots/degree, nonnegative payload and a new fixture path required")
        seed(args)
    elif args.command == "verify":
        verify(args)
    elif args.command == "cold":
        if args.samples < 7:
            parser.error("at least seven cold samples required")
        cold(args)
    else:
        if args.samples < 7 or args.workers < 1:
            parser.error("at least seven samples and positive workers required")
        compare(args)
