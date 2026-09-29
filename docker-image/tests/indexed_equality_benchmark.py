#!/usr/bin/env python3
"""Compare two local full images using synthetic equality fixtures.

Build both images first. Example:
  python3 docker-image/tests/indexed_equality_benchmark.py --baseline-image helixdb:baseline \
    --candidate-image helixdb:candidate --output /absolute/path/results.json

Only disposable loopback containers and volumes are used. Every timed response
is checked against a Python oracle. Timings include HTTP, planning, execution,
and serialization. Reopened reads have cold process caches, not cold host disks.
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


def fixture(label, size, mode):
    rows = []
    for ordinal in range(size):
        if mode == "sparse":
            values = [ordinal % (1000 + index * 101) for index in range(5)]
        elif mode == "broad":
            values = [ordinal % 2] * 5
        else:
            selective = 0 if mode == "skew_first" else 4
            values = [ordinal % (1000 if index == selective else 2) for index in range(5)]
        rows.append({"ordinal": ordinal, **dict(zip(PROPERTIES, values)),
                     "keep": ordinal % 3, "payload": "x" * 512})
    return {"label": label, "mode": mode, "rows": rows}


def seed(port, data):
    # Activate all indexes on an empty database before ingesting fixtures. This
    # keeps unrelated index backfills out of the loading and timing phases.
    for item in data:
        label = item["label"]
        for prop in PROPERTIES:
            receipt, _ = request(port, batch("write", [{"create_index": {
                "spec": {"node_equality": {"label": label, "property": prop, "unique": False}},
                "if_not_exists": True}}]))
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
                [key, {"value": {"i64" if isinstance(value, int) else "string": value}}]
                for key, value in row.items()]}} for row in item["rows"][start:start + 100]]
            request(port, batch("write", roots, returning=False))
        print(json.dumps({"seeded_port": port, "label": label, "rows": len(item["rows"])}), flush=True)


def query(item, count, parameterized=False, nested=False, reverse=False, residual=False, missing=False):
    value = -1 if missing else 0
    props = PROPERTIES[:count]
    if reverse:
        props = list(reversed(props))
    terms = [{"eq": {"left": {"property": prop}, "right":
              {"param": prop} if parameterized else {"constant": {"i64": value}}}} for prop in props]
    if residual:
        terms.append({"eq": {"left": {"property": "keep"}, "right": {"constant": {"i64": 0}}}})
    if nested:
        terms = [{"and": {"predicates": terms[:2]}}, {"and": {"predicates": terms[2:]}}]
    terms.insert(0, {"eq": {"left": {"property": "$label"}, "right": {"constant": {"string": item["label"]}}}})
    payload = batch("read", [{"values": {"input": {"nodes_where": {
        "predicate": {"and": {"predicates": terms}}}}, "properties": ["ordinal"]}}])
    if parameterized:
        payload["parameters"] = {prop: value for prop in props}
        payload["parameter_types"] = {prop: "i64" for prop in props}
    expected = [row["ordinal"] for row in item["rows"]
                if all(row[prop] == value for prop in props) and (not residual or row["keep"] == 0)]
    return payload, expected


def checked(port, payload, expected):
    response, elapsed = request(port, payload)
    actual = sorted(row["ordinal"] for row in response["q0"])
    if actual != expected:
        raise AssertionError({"port": port, "actual_count": len(actual), "expected_count": len(expected),
                              "actual_head": actual[:10], "expected_head": expected[:10]})
    return elapsed


def summary(values):
    return {"p50_ms": statistics.median(values), "p95_ms": sorted(values)[math.ceil(len(values) * .95) - 1],
            "samples_ms": values}


def run(args):
    token = uuid.uuid4().hex[:10]
    resources = []
    report = {"complete": False, "rows": args.rows, "samples": args.samples, "row_payload_bytes": 512,
              "platform": "linux/arm64", "measurement": "end-to-end HTTP milliseconds", "images": {}, "cases": []}
    data = [fixture("SparseFixture", args.rows, "sparse"),
            fixture("SkewFirstFixture", max(1000, args.rows // 5), "skew_first"),
            fixture("SkewLastFixture", max(1000, args.rows // 5), "skew_last"),
            fixture("BroadFixture", max(1000, args.rows // 5), "broad"),
            fixture("SmallFixture", 16, "broad")]
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
            seed(ports[role], data)
        for item in data:
            for count in [1, 2, 3, 4, 5]:
                payload, expected = query(item, count)
                timings = {role: [] for role in ports}
                for sample in range(args.samples + 3):
                    roles = list(ports) if sample % 2 == 0 else list(reversed(ports))
                    for role in roles:
                        elapsed = checked(ports[role], payload, expected)
                        if sample >= 3:
                            timings[role].append(elapsed)
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
    args = parser.parse_args()
    if args.rows < 1000 or args.samples < 5:
        parser.error("use at least 1000 rows and five samples")
    run(args)
