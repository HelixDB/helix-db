#!/usr/bin/env python3
"""Compare unique-key claims on two disposable localhost images.

Each request claims five jobs by a unique `job_id`, guarded by status and an
expiry cutoff over indexed properties: as five entries keyed by one id each
(`single`), and as one entry keyed by `job_id IN $ids` (`list`). Creates only synthetic records, and
checks every response and the final state of both images against a local
model of the jobs. `select` reads the five guarded jobs without writing, so
`claim` minus `select` is mutation and commit time. Run after both images are
ready; never pass an existing database port.
"""
import argparse
import json
import random
import statistics
import time

import range_benchmark as bench

STATUSES = ["available", "claimed", "done", "done"]
CLAIMED_UNTIL = 1_000_000
CUTOFF = 900_000


def job(n):
    """Status, and claim time for claimed jobs, of job `n`."""
    status = STATUSES[n % len(STATUSES)]
    return {"status": status, "claimed_at": (n * 7919) % CLAIMED_UNTIL if status == "claimed" else None}


def claimable(state):
    return state["status"] == "available" or (
        state["status"] == "claimed" and state["claimed_at"] < CUTOFF)


def guarded(key):
    return {"nodes_where": {"predicate": {"and": {"predicates": [
        {"eq": {"left": {"property": "$label"}, "right": {"constant": {"string": "Job"}}}},
        key,
        {"is_in": {"value": {"property": "status"},
                   "values": {"constant": {"string_array": ["available", "claimed"]}}}},
        {"or": {"predicates": [
            {"eq": {"left": {"property": "status"}, "right": {"constant": {"string": "available"}}}},
            {"lt": {"left": {"property": "claimed_at"}, "right": {"param": "cutoff"}}},
        ]}},
    ]}}}}


def claims(ids, at, write, listed):
    keys = [{"is_in": {"value": {"property": "job_id"}, "values": {"param": "ids"}}}] if listed else [
        {"eq": {"left": {"property": "job_id"}, "right": {"param": f"id{i}"}}}
        for i in range(len(ids))]
    roots = []
    for key in keys:
        source = guarded(key)
        if write:
            for name, value in [("status", {"string": "claimed"}), ("claimed_at", {"i64": at}),
                                ("attempt", {"i64": at})]:
                source = {"set_property": {"input": source, "name": name, "value": {"value": value}}}
        roots.append({"values": {"input": source, "properties": ["job_id"]}})
    request = bench.batch("write" if write else "read", roots)
    keyed = {"ids": [f"j{n}" for n in ids]} if listed else {f"id{i}": f"j{n}" for i, n in enumerate(ids)}
    request["parameters"] = {"cutoff": CUTOFF, **keyed}
    return request


def create_index(port, spec):
    receipt, _ = bench.request(port, bench.batch("write", [{"create_index": {
        "spec": spec, "if_not_exists": True}}]))
    operation = receipt["q0"]["operation_id"]
    deadline = time.monotonic() + 600
    while True:
        status, _ = bench.request(port, bench.batch("read", [{"get_index_operation": {"operation_id": operation}}]))
        state = status["q0"]["status"]
        if state == "succeeded":
            return
        if state not in ["queued", "running"] or time.monotonic() >= deadline:
            raise RuntimeError(status)
        time.sleep(.1)


def prepare(port, size):
    for spec in [
        {"node_equality": {"label": "Job", "property": "job_id", "unique": True}},
        {"node_equality": {"label": "Job", "property": "status", "unique": False}},
        {"node_range": {"label": "Job", "property": "claimed_at", "direction": "asc"}},
    ]:
        create_index(port, spec)
    for start in range(0, size, 500):
        roots = []
        for n in range(start, min(start + 500, size)):
            state = job(n)
            properties = [["job_id", {"value": {"string": f"j{n}"}}],
                          ["status", {"value": {"string": state["status"]}}]]
            if state["claimed_at"] is not None:
                properties.append(["claimed_at", {"value": {"i64": state["claimed_at"]}}])
            roots.append({"add_n": {"label": "Job", "properties": properties}})
        bench.request(port, bench.batch("write", roots))
    # Index rows publish asynchronously: wait until both guarded indexes
    # count every seeded job.
    def counted(predicate):
        return {"count": {"input": {"nodes_where": {"predicate": {"and": {"predicates": [
            {"eq": {"left": {"property": "$label"}, "right": {"constant": {"string": "Job"}}}}, predicate,
        ]}}}}}}
    states = [job(n) for n in range(size)]
    expected = {"q0": sum(state["status"] == "available" for state in states),
                "q1": sum(state["claimed_at"] is not None and state["claimed_at"] < CUTOFF for state in states)}
    deadline = time.monotonic() + 1800
    while True:
        result, _ = bench.request(port, bench.batch("read", [
            counted({"eq": {"left": {"property": "status"}, "right": {"constant": {"string": "available"}}}}),
            counted({"lt": {"left": {"property": "claimed_at"}, "right": {"constant": {"i64": CUTOFF}}}}),
        ]))
        if result == expected:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError((port, result, expected))
        time.sleep(1)


def run(args):
    ports = [args.baseline, args.candidate]
    if not args.no_seed:
        for port in ports:
            prepare(port, args.size)
    model = {n: job(n) for n in range(args.size)}
    rng = random.Random(7)
    cases = [(shape, mode) for shape in ["single", "list"] for mode in ["select", "claim"]]
    timings = {(case, port): [] for case in cases for port in ports}
    for sample in range(args.samples + 1):
        for shape in ["single", "list"]:
            # Mix claimable jobs with unexpired, done and missing ones.
            ids = rng.sample(range(args.size), 4) + [args.size + sample]
            claimable_ids = [n for n in ids if n in model and claimable(model[n])]
            expected = [[{"job_id": f"j{n}"} for n in claimable_ids]] if shape == "list" else [
                [{"job_id": f"j{n}"}] if n in claimable_ids else [] for n in ids]
            at = CLAIMED_UNTIL + 2 * sample + (shape == "list")
            for mode in ["select", "claim"]:
                for port in ports if sample % 2 == 0 else ports[::-1]:
                    result, elapsed = bench.request(port, claims(ids, at, mode == "claim", shape == "list"))
                    rows = [sorted(result[f"q{i}"], key=lambda row: row["job_id"]) for i in range(len(expected))]
                    assert rows == [sorted(e, key=lambda row: row["job_id"]) for e in expected], (
                        port, shape, mode, ids, result)
                    if sample > 0:
                        timings[((shape, mode), port)].append(elapsed * 1000)
            for n in claimable_ids:
                model[n] = {"status": "claimed", "claimed_at": at}
    for port in ports:
        claimed, _ = bench.request(port, bench.batch("read", [{"count": {"input": {"nodes_where": {"predicate": {
            "and": {"predicates": [
                {"eq": {"left": {"property": "$label"}, "right": {"constant": {"string": "Job"}}}},
                {"gte": {"left": {"property": "claimed_at"}, "right": {"constant": {"i64": CLAIMED_UNTIL}}}},
            ]}}}}}}]))
        assert claimed["q0"] == sum(state["claimed_at"] is not None and state["claimed_at"] >= CLAIMED_UNTIL
                                    for state in model.values()), (port, claimed)
    print(json.dumps({"rows": args.size, "samples": args.samples, **{
        f"{shape}_{mode}_{name}_p50_ms": round(statistics.median(timings[((shape, mode), port)]), 1)
        for shape, mode in cases for name, port in [("baseline", args.baseline), ("candidate", args.candidate)]
    }}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=int, default=18270)
    parser.add_argument("--candidate", type=int, default=18271)
    parser.add_argument("--size", type=int, default=100_000)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--no-seed", action="store_true")
    run(parser.parse_args())
