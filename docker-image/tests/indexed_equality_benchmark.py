#!/usr/bin/env python3
"""Compare two local full images using synthetic equality fixtures.

Build both images first. Example:
  python3 docker-image/tests/indexed_equality_benchmark.py --baseline-image helixdb:baseline \
    --candidate-image helixdb:candidate --output /absolute/path/results.json

Only disposable loopback containers and volumes are used. Every timed response
is checked against a Python oracle. Timings include HTTP, planning, execution,
and serialization. Reopened reads have cold process caches, not cold host disks.

The 25 base shapes (five fixtures, one to five equalities) are always timed.
`--extended` adds three indexed equality properties, a nullable equality-indexed
property `maybe`, and a range-only indexed property `rank` to every fixture, and
times further oracle-checked source-filter shapes under report["extended_cases"]:
counts, `.as()` then count, partly indexed and unindexed ORs, IN lists and
parameterized OR lists over the union limit, null equality, equality on a
range-only property, point IDs intersected with an index, and six to eight
equalities. Without `--extended` the fixtures, indexes and requests are
unchanged, so base results stay comparable with earlier runs.
"""
import argparse
import hashlib
import http.client
import json
import math
import statistics
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

PROPERTIES = [f"p{index}" for index in range(5)]
# Extended runs only: more indexed equalities, a nullable equality-indexed
# property and a property with only a range index.
EXTENDED_PROPERTIES = [f"p{index}" for index in range(5, 8)]
NULLABLE = "maybe"
RANGE_ONLY = "rank"
# Longer than the planner's 64-branch union limit.
LONG_LIST = 200


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def request(port, payload):
    wire = json.dumps(payload).encode()
    started = time.perf_counter_ns()
    req = urllib.request.Request(f"http://127.0.0.1:{port}/v2/query", wire,
                                 {"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(error.read().decode()) from error
    return result, (time.perf_counter_ns() - started) / 1_000_000


def batch(kind, roots, returning=True):
    names = [f"q{index}" for index in range(len(roots))]
    return {"request_type": kind, "query": {kind: {
        "entries": [{"query": {"name": name, "root": root}}
                    for name, root in zip(names, roots)],
        "returns": names if returning else []}}}


def ready(port):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=1):
                return
        except (urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected, ConnectionResetError):
            time.sleep(.1)
    raise TimeoutError(f"local server {port} did not become ready")


def fixture(label, size, mode, extended):
    props = PROPERTIES + EXTENDED_PROPERTIES if extended else PROPERTIES
    rows = []
    for ordinal in range(size):
        if mode == "sparse":
            values = [ordinal % (1000 + index * 101) for index in range(len(props))]
        elif mode == "broad":
            values = [ordinal % 2] * len(props)
        else:
            selective = 0 if mode == "skew_first" else 4
            values = [ordinal % (1000 if index == selective else 2) for index in range(len(props))]
        rows.append({"ordinal": ordinal, **dict(zip(props, values)),
                     "keep": ordinal % 3, "payload": "x" * 512})
        if extended:
            rows[-1][RANGE_ONLY] = ordinal
            # Explicit null for a third of the rows, absent for another third.
            if ordinal % 3 != 1:
                rows[-1][NULLABLE] = None if ordinal % 3 == 0 else ordinal % 11
    return {"label": label, "mode": mode, "rows": rows}


def seed(port, data, extended):
    # Activate all indexes on an empty database before ingesting fixtures. This
    # keeps unrelated index backfills out of the loading and timing phases.
    for item in data:
        label = item["label"]
        specs = [{"node_equality": {"label": label, "property": prop, "unique": False}}
                 for prop in PROPERTIES]
        if extended:
            specs += [{"node_equality": {"label": label, "property": prop, "unique": False}}
                      for prop in EXTENDED_PROPERTIES + [NULLABLE]]
            specs.append({"node_range": {"label": label, "property": RANGE_ONLY, "direction": "asc"}})
        for spec in specs:
            receipt, _ = request(port, batch("write", [{"create_index": {
                "spec": spec, "if_not_exists": True}}]))
            operation = receipt["q0"].get("operation_id")
            if operation:
                deadline = time.monotonic() + 120
                while True:
                    status, _ = request(port, batch("read", [{"get_index_operation": {"operation_id": operation}}]))
                    state = status["q0"]["status"]
                    if state == "succeeded":
                        break
                    if state in ("failed", "blocked", "aborted") or time.monotonic() > deadline:
                        raise RuntimeError(status)
                    time.sleep(.05)
    for item in data:
        label = item["label"]
        for start in range(0, len(item["rows"]), 100):
            roots = [{"add_n": {"label": label, "properties": [
                [key, {"value": "null" if value is None else
                       {"i64" if isinstance(value, int) else "string": value}}]
                for key, value in row.items()]}} for row in item["rows"][start:start + 100]]
            request(port, batch("write", roots, returning=False))
        print(json.dumps({"seeded_port": port, "label": label, "rows": len(item["rows"])}), flush=True)


def query(item, count, parameterized=False, nested=False, reverse=False, residual=False, missing=False,
          terminal="values"):
    """Returns the request and oracle for `count` zero equalities on one fixture.

    `terminal` is "values" (sorted ordinals), "count", or "as_count" (the source
    saved with `.as()` and then counted); counts expect an integer.
    """
    value = -1 if missing else 0
    props = (PROPERTIES + EXTENDED_PROPERTIES)[:count]
    if reverse:
        props = list(reversed(props))
    terms = [{"eq": {"left": {"property": prop}, "right":
              {"param": prop} if parameterized else {"constant": {"i64": value}}}} for prop in props]
    if residual:
        terms.append({"eq": {"left": {"property": "keep"}, "right": {"constant": {"i64": 0}}}})
    if nested:
        terms = [{"and": {"predicates": terms[:2]}}, {"and": {"predicates": terms[2:]}}]
    terms.insert(0, {"eq": {"left": {"property": "$label"}, "right": {"constant": {"string": item["label"]}}}})
    source = {"nodes_where": {"predicate": {"and": {"predicates": terms}}}}
    root = {"values": {"input": source, "properties": ["ordinal"]}} if terminal == "values" else {
        "count": {"input": source if terminal == "count" else {"as": {"input": source, "name": "x"}}}}
    payload = batch("read", [root])
    if parameterized:
        payload["parameters"] = {prop: value for prop in props}
        payload["parameter_types"] = {prop: "i64" for prop in props}
    expected = [row["ordinal"] for row in item["rows"]
                if all(row[prop] == value for prop in props) and (not residual or row["keep"] == 0)]
    return payload, expected if terminal == "values" else len(expected)


def filtered(item, predicate, accept, parameters=None, parameter_types=None):
    """Returns a values request for `$label == label AND predicate` and its oracle."""
    label = {"eq": {"left": {"property": "$label"}, "right": {"constant": {"string": item["label"]}}}}
    payload = batch("read", [{"values": {"input": {"nodes_where": {
        "predicate": {"and": {"predicates": [label, predicate]}}}}, "properties": ["ordinal"]}}])
    if parameters is not None:
        payload["parameters"] = parameters
        payload["parameter_types"] = parameter_types
    return payload, [row["ordinal"] for row in item["rows"] if accept(row)]


def eq(prop, right):
    return {"eq": {"left": {"property": prop}, "right": right}}


def extended_cases(item, ports):
    """Yields (name, request by role, oracle) for every extended shape on one fixture.

    Every shape here must be answered from indexes by the candidate, except the
    OR with an unindexed branch, which is a correctness check only.
    """
    zero = {"constant": {"i64": 0}}
    for count in [1, 2, 3, 4, 5]:
        for terminal in ["count", "as_count"]:
            payload, expected = query(item, count, terminal=terminal)
            yield f"{terminal}_{count}", {role: payload for role in ports}, expected
    for count in [6, 7, 8]:
        payload, expected = query(item, count)
        yield f"equalities_{count}", {role: payload for role in ports}, expected
    cases = [
        ("partial_or", {"or": {"predicates": [
            {"and": {"predicates": [eq("p0", zero), eq("keep", zero)]}}, eq("p1", zero)]}},
         lambda row: (row["p0"] == 0 and row["keep"] == 0) or row["p1"] == 0, None, None),
        ("unindexed_or", {"or": {"predicates": [eq("p0", zero), eq("keep", {"constant": {"i64": 1}})]}},
         lambda row: row["p0"] == 0 or row["keep"] == 1, None, None),
        (f"in_{LONG_LIST}_literal",
         {"is_in": {"value": {"property": "p0"}, "values": {"constant": {"i64_array": list(range(LONG_LIST))}}}},
         lambda row: row["p0"] < LONG_LIST, None, None),
        (f"in_{LONG_LIST}_param", {"is_in": {"value": {"property": "p0"}, "values": {"param": "values"}}},
         lambda row: row["p0"] < LONG_LIST, {"values": list(range(LONG_LIST))}, {"values": {"array": "i64"}}),
        ("null_literal", eq(NULLABLE, {"constant": "null"}),
         lambda row: row.get(NULLABLE) is None, None, None),
        ("null_param", eq(NULLABLE, {"param": "value"}),
         lambda row: row.get(NULLABLE) is None, {"value": None}, {"value": "value"}),
        ("in_null_5_literal",
         {"is_in": {"value": {"property": NULLABLE}, "values": {"constant": {"array": ["null", {"i64": 5}]}}}},
         lambda row: row.get(NULLABLE) in (None, 5), None, None),
        ("in_null_5_param", {"is_in": {"value": {"property": NULLABLE}, "values": {"param": "values"}}},
         lambda row: row.get(NULLABLE) in (None, 5), {"values": [None, 5]}, {"values": {"array": "value"}}),
        ("range_only_literal", eq(RANGE_ONLY, {"constant": {"i64": len(item["rows"]) // 2}}),
         lambda row: row[RANGE_ONLY] == len(item["rows"]) // 2, None, None),
        ("range_only_param", eq(RANGE_ONLY, {"param": "value"}),
         lambda row: row[RANGE_ONLY] == len(item["rows"]) // 2,
         {"value": len(item["rows"]) // 2}, {"value": "i64"}),
    ]
    for branches in [8, 100]:
        cases.append((f"or_list_{branches}_param",
                      {"or": {"predicates": [eq("p0", {"param": f"v{index}"}) for index in range(branches)]}},
                      lambda row, branches=branches: row["p0"] < branches,
                      {f"v{index}": index for index in range(branches)},
                      {f"v{index}": "i64" for index in range(branches)}))
    for name, predicate, accept, parameters, parameter_types in cases:
        payload, expected = filtered(item, predicate, accept, parameters, parameter_types)
        yield name, {role: payload for role in ports}, expected
    # Point IDs intersected with an index. IDs are allocated per database, so
    # each role looks up the IDs of the first 50 ingested rows itself.
    chosen = item["rows"][:50]
    payloads = {}
    for role, port in ports.items():
        response, _ = request(port, batch("read", [{"values": {"input": {"nodes_where": {"predicate": eq(
            "$label", {"constant": {"string": item["label"]}})}}, "properties": ["$id", "ordinal"]}}]))
        ids = {row["ordinal"]: row["$id"] for row in response["q0"]}
        payloads[role] = batch("read", [{"count": {"input": {"where": {
            "input": {"nodes": {"reference": {"ids": [ids[row["ordinal"]] for row in chosen]}}},
            "predicate": eq("p0", zero)}}}}])
    yield "point_ids_count", payloads, sum(row["p0"] == 0 for row in chosen)


def checked(port, payload, expected):
    response, elapsed = request(port, payload)
    actual = response["q0"] if isinstance(expected, int) else sorted(row["ordinal"] for row in response["q0"])
    if actual != expected:
        if isinstance(expected, int):
            raise AssertionError({"port": port, "actual": actual, "expected": expected})
        raise AssertionError({"port": port, "actual_count": len(actual), "expected_count": len(expected),
                              "actual_head": actual[:10], "expected_head": expected[:10]})
    return elapsed


def sampled(ports, payloads, expected, samples):
    """Times `samples` warm, oracle-checked requests per role after three warmups.

    Roles alternate which one goes first so neither always runs on a cache the
    other just warmed.
    """
    timings = {role: [] for role in ports}
    for sample in range(samples + 3):
        roles = list(ports) if sample % 2 == 0 else list(reversed(ports))
        for role in roles:
            elapsed = checked(ports[role], payloads[role], expected)
            if sample >= 3:
                timings[role].append(elapsed)
    return timings


def summary(values):
    return {"p50_ms": statistics.median(values), "p95_ms": sorted(values)[math.ceil(len(values) * .95) - 1],
            "samples_ms": values}


def run(args):
    token = uuid.uuid4().hex[:10]
    resources = []
    report = {"complete": False, "rows": args.rows, "samples": args.samples, "row_payload_bytes": 512,
              "platform": "linux/arm64", "measurement": "end-to-end HTTP milliseconds", "images": {}, "cases": []}
    data = [fixture("SparseFixture", args.rows, "sparse", args.extended),
            fixture("SkewFirstFixture", max(1000, args.rows // 5), "skew_first", args.extended),
            fixture("SkewLastFixture", max(1000, args.rows // 5), "skew_last", args.extended),
            fixture("BroadFixture", max(1000, args.rows // 5), "broad", args.extended),
            fixture("SmallFixture", 16, "broad", args.extended)]
    if args.extended:
        report["extended_cases"] = []
    try:
        ports = {}
        for role, image in [("baseline", args.baseline_image), ("candidate", args.candidate_image)]:
            report["images"][role] = {"tag": image, "id": docker("image", "inspect", image, "--format", "{{.Id}}")}
            name = f"helix-equality-{token}-{role}"
            volume = f"{name}-data"
            docker("volume", "create", volume)
            resources.append((name, volume))
            docker("run", "-d", "--name", name, "--platform", "linux/arm64", "-p", "127.0.0.1::8080",
                   "-e", "HELIX_DATA_DIR=/var/lib/helix", "-e", "RUST_LOG=error",
                   "--mount", f"type=volume,source={volume},target=/var/lib/helix", image)
            ports[role] = int(docker("port", name, "8080/tcp").rsplit(":", 1)[1])
            ready(ports[role])
            seed(ports[role], data, args.extended)
        for item in data:
            for count in [1, 2, 3, 4, 5]:
                payload, expected = query(item, count)
                timings = sampled(ports, {role: payload for role in ports}, expected, args.samples)
                case = {"label": item["label"], "equalities": count, "mode": "warm",
                        "matches": len(expected), "result_sha256": hashlib.sha256(json.dumps(expected).encode()).hexdigest(),
                        **{role: summary(values) for role, values in timings.items()}}
                case["candidate_over_baseline_p50"] = case["candidate"]["p50_ms"] / case["baseline"]["p50_ms"]
                report["cases"].append(case)
                Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
                print(json.dumps({key: value for key, value in case.items() if key not in ports}), flush=True)
            for parameterized, nested, reverse, residual, missing in [
                (True, False, False, False, False), (True, True, True, True, False),
                (False, True, True, False, True), (False, False, True, True, False)]:
                payload, expected = query(item, 5, parameterized, nested, reverse, residual, missing)
                for port in ports.values():
                    checked(port, payload, expected)
        report["extra_correctness_checks"] = len(data) * 4 * 2
        for item in data if args.extended else []:
            for name, payloads, expected in extended_cases(item, ports):
                timings = sampled(ports, payloads, expected, args.samples)
                case = {"label": item["label"], "case": name, "mode": "warm",
                        "matches": expected if isinstance(expected, int) else len(expected),
                        "result_sha256": hashlib.sha256(json.dumps(expected).encode()).hexdigest(),
                        **{role: summary(values) for role, values in timings.items()}}
                case["candidate_over_baseline_p50"] = case["candidate"]["p50_ms"] / case["baseline"]["p50_ms"]
                report["extended_cases"].append(case)
                Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
                print(json.dumps({key: value for key, value in case.items() if key not in ports}), flush=True)
        # Both variants reopen their own durable fixture; no database is shared
        # between binaries, so this does not test a storage migration.
        for role, (name, _) in zip(ports, resources):
            docker("stop", "--time", "60", name)
            docker("start", name)
            ports[role] = int(docker("port", name, "8080/tcp").rsplit(":", 1)[1])
            ready(ports[role])
        payload, expected = query(data[0], 5, parameterized=True, nested=True, reverse=True)
        report["reopened_sparse_5_ms"] = {role: checked(port, payload, expected) for role, port in ports.items()}
        report["complete"] = True
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    finally:
        for name, volume in reversed(resources):
            subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True)
            subprocess.run(["docker", "volume", "rm", volume], check=False, capture_output=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-image", required=True)
    parser.add_argument("--candidate-image", required=True)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--output", required=True)
    parser.add_argument("--extended", action="store_true",
                        help="also time the extended source-filter shapes (see the module docstring)")
    args = parser.parse_args()
    if args.rows < 1000 or args.samples < 5:
        parser.error("use at least 1000 rows and five samples")
    run(args)
