#!/usr/bin/env python3
"""Summarize one run directory, or aggregate several into a comparison.

A run directory (written by `run.py`, or assembled from EC2 hosts) holds:

    run.json                   exact configuration (optional)
    trace-manifest.json        trace phases and hash (from workload.py)
    replay/requests.jsonl      request ledger (from replay.py)
    writer/stdout.log          writer stdout, i.e. its benchmark samples
    reader/stdout.log          optional reader stdout
    docker-stats.jsonl         optional sampled CPU/memory (run.py)
    resources-<role>/resources.json  optional cgroup captures (resources.py)

Client latency percentiles are exact nearest-rank values over offers scheduled
in the window (late completions included), per kind and outcome. Acknowledged
ingestion counts writes acknowledged inside the window. Server metrics are
differences of the writer's (and reader's) samples bracketing each window; see
`samples.py`. The measurement window defaults to all non-warm-up phases.
Everything is held in memory: a 40-minute run at a few hundred offers per
second is a few hundred thousand small records.
"""

import argparse
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path

import samples
from replay import OUTCOMES, Kind

NS_PER_MS = 1_000_000


def load_ledger(path):
    """Returns `(start, results, end)`; rejects incomplete or inconsistent ledgers."""
    with Path(path).open() as source:
        records = [json.loads(line) for line in source]
    if (
        len(records) < 2
        or records[0]["event"] != "start"
        or records[-1]["event"] != "end"
    ):
        raise ValueError("ledger lacks its start or end record")
    start, end, results = records[0], records[-1], records[1:-1]
    if any(r["event"] != "result" for r in results):
        raise ValueError("unexpected ledger record")
    if sorted(r["id"] for r in results) != list(range(start["planned"])):
        raise ValueError("ledger does not hold exactly one outcome per offer")
    counts = Counter(f"{r['kind']}:{r['outcome']}" for r in results)
    if any(r["outcome"] not in OUTCOMES[Kind(r["kind"])] for r in results):
        raise ValueError("unknown outcome in ledger")
    if dict(counts) != end["counts"] or end["offered"] != start["planned"]:
        raise ValueError("ledger footer disagrees with its records")
    return start, results, end


def distribution(values_ns):
    """Exact nearest-rank percentiles in milliseconds."""
    if not values_ns:
        return {"count": 0}
    ordered = sorted(values_ns)

    def pick(q):
        return ordered[samples.nearest_rank(q, len(ordered)) - 1] / NS_PER_MS

    return {
        "count": len(ordered),
        "p50_ms": pick(0.5),
        "p95_ms": pick(0.95),
        "p99_ms": pick(0.99),
        "max_ms": ordered[-1] / NS_PER_MS,
    }


def client(results, window, phases):
    """Latency by kind and outcome, outcome categories, and throughput."""
    low, high = window
    seconds = (high - low) / 1e9
    chosen = [r for r in results if low <= r["at_ns"] < high]
    sent = [r for r in chosen if r["outcome"] != "client_overload"]
    latency = defaultdict(lambda: defaultdict(list))
    for r in sent:
        latency[r["kind"]][r["outcome"]].append(r["latency_ns"])
    outcomes = Counter((r["kind"], r["outcome"]) for r in chosen)
    by_phase = defaultdict(Counter)
    for r in results:
        name = next(n for n, s, e in phases if s <= r["at_ns"] < e)
        by_phase[name][f"{r['kind']}:{r['outcome']}"] += 1
    kinds = [kind.value for kind in Kind]
    return {
        "window_seconds": seconds,
        "offered_per_second": {
            k: sum(r["kind"] == k for r in chosen) / seconds for k in kinds
        },
        "acknowledged_writes_per_second": sum(
            r["outcome"] == "acknowledged" and low <= r["finished_ns"] < high
            for r in results
        )
        / seconds,
        "latency": {
            kind: {
                outcome: distribution(values) for outcome, values in sorted(by.items())
            }
            for kind, by in sorted(latency.items())
        },
        "scheduled_latency": {
            k: distribution([r["scheduled_latency_ns"] for r in sent if r["kind"] == k])
            for k in kinds
        },
        "dispatch_lag": {
            k: distribution([r["dispatch_lag_ns"] for r in sent if r["kind"] == k])
            for k in kinds
        },
        "outcomes": {f"{k}:{o}": n for (k, o), n in sorted(outcomes.items())},
        "categories": {
            "backpressure_429": sum(
                n for (_, o), n in outcomes.items() if o == "backpressure"
            ),
            "conflict_409": sum(n for (_, o), n in outcomes.items() if o == "conflict"),
            "client_overload": sum(
                n for (_, o), n in outcomes.items() if o == "client_overload"
            ),
            "timeouts": sum(n for (_, o), n in outcomes.items() if o == "timeout"),
            "uncertain_writes": outcomes["write", "unknown_write"]
            + outcomes["write", "timeout"],
            "rejected_writes": outcomes["write", "rejected"],
            "failed_searches": sum(
                n
                for (k, o), n in outcomes.items()
                if k != "write" and o in ("http_error", "request_error", "timeout")
            ),
        },
        "outcomes_by_phase": {
            name: dict(sorted(c.items())) for name, c in by_phase.items()
        },
    }


def server(stream, windows, t0_ns, trace_end_ns):
    """Sample windows for one role, plus the post-load drain."""
    result = {}
    for name, (low, high) in windows.items():
        before, after, gaps = samples.bracket(
            stream, (t0_ns + low) // NS_PER_MS, -(-(t0_ns + high) // NS_PER_MS)
        )
        series = stream[before["sample"] : after["sample"] + 1]
        result[name] = samples.window(before, after, series) | {"boundary": gaps}
    end_ms = (t0_ns + trace_end_ns) // NS_PER_MS
    tail = [s for s in stream if s["unix_ms"] >= end_ms] or stream[-1:]
    empty = next((s for s in tail if s["queue"]["pending_operations"] == 0), None)
    return {
        "windows": result,
        "drain": {
            "pending_operations_at_trace_end": tail[0]["queue"]["pending_operations"],
            "pending_members_at_trace_end": tail[0]["queue"]["pending_members"],
            "seconds_to_empty": None
            if empty is None
            else (empty["elapsed_ns"] - tail[0]["elapsed_ns"]) / 1e9,
            "final_pending_operations": stream[-1]["queue"]["pending_operations"],
        },
        "anomalies": samples.check(stream)[:50],
        "final": {k: stream[-1][k] for k in ("sample", "elapsed_ns", "queue", "lag")},
    }


def resources(directory):
    """Per role: cgroup captures when present, else sampled `docker stats`."""
    found = {}
    for path in sorted(directory.glob("resources-*/resources.json")):
        report = json.loads(path.read_text())
        found[path.parent.name.removeprefix("resources-")] = {
            "source": "cgroup v2 (resources.py)",
            "average_cpu_percent": report["average_cpu_percent"],
            "peak_memory_bytes": report["peak_bytes"],
        }
    stats = directory / "docker-stats.jsonl"
    if stats.exists():
        rows = defaultdict(list)
        for line in stats.read_text().splitlines():
            row = json.loads(line)
            rows[row["role"]].append(row)
        for role, series in rows.items():
            found.setdefault(
                role,
                {
                    "source": "docker stats sampled (approximate; not a cgroup peak)",
                    "samples": len(series),
                    "average_cpu_percent": statistics.fmean(
                        r["cpu_percent"] for r in series
                    ),
                    "peak_memory_bytes": max(r["memory_bytes"] for r in series),
                },
            )
    return found


def summarize(directory, *, from_ns=None, until_ns=None):
    directory = Path(directory)
    manifest = json.loads((directory / "trace-manifest.json").read_text())
    phases = [tuple(p) for p in manifest["phases"]]
    start, results, _ = load_ledger(directory / "replay" / "requests.jsonl")
    if start["trace_sha256"] != manifest["trace_sha256"]:
        raise ValueError("ledger was not produced from this trace")
    measured = [p for p in phases if not p[0].startswith("warmup")]
    window = (
        measured[0][1] if from_ns is None else from_ns,
        measured[-1][2] if until_ns is None else until_ns,
    )
    if not 0 <= window[0] < window[1]:
        raise ValueError("measurement window must have positive duration")
    # Unix time of trace offset zero on the load generator's clock.
    t0_ns = (
        start["clock_unix_ns"]
        + start["start_monotonic_ns"]
        - start["clock_monotonic_ns"]
    )
    windows = {"measurement": window} | {f"phase:{n}": (s, e) for n, s, e in phases}
    run = directory / "run.json"
    summary = {
        "schema": 1,
        "run": json.loads(run.read_text()) if run.exists() else None,
        "trace_sha256": manifest["trace_sha256"],
        "measurement_window_ns": window,
        "client": client(results, window, phases),
        "server": {},
        "resources": resources(directory),
    }
    for role in ("writer", "reader"):
        log = directory / role / "stdout.log"
        if log.exists():
            with log.open() as lines:
                stream = samples.parse(lines)
            summary["server"][role] = server(stream, windows, t0_ns, phases[-1][2])
    summary["headline"] = headline(summary)
    return summary


def headline(summary):
    """Flat key metrics for the measurement window, for tables and aggregation."""
    latency = summary["client"]["latency"]
    flat = {
        f"{kind}_{q}_ms": latency.get(kind, {}).get(outcome, {}).get(f"{q}_ms")
        for kind, outcome in (
            ("write", "acknowledged"),
            ("strong", "search_ok"),
            ("eventual", "search_ok"),
        )
        for q in ("p50", "p95", "p99")
    }
    flat["acked_writes_per_s"] = summary["client"]["acknowledged_writes_per_second"]
    flat |= summary["client"]["categories"]
    for role, report in summary["server"].items():
        w = report["windows"]["measurement"]
        prefix = "" if role == "writer" else "reader_"
        merge = w["merge"]
        flat |= {
            f"{prefix}merge_{kind}_{field}": merge[kind][field]
            for kind in merge
            for field in ("merges", "operands", "nanos")
        }
        flat[f"{prefix}s3_attempts"] = sum(
            row.get("connector_attempts", 0)
            for key, row in w["io"].items()
            if key.startswith("s3 ")
        )
        flat[f"{prefix}s3_request_body_bytes"] = sum(
            row.get("request_body_bytes_offered", 0)
            for key, row in w["io"].items()
            if key.startswith("s3 ")
        )
        flat[f"{prefix}s3_response_body_bytes"] = sum(
            row.get("response_body_bytes_delivered", 0)
            for key, row in w["io"].items()
            if key.startswith("s3 ")
        )
        if role != "writer":
            continue
        queue, lag, gauges = w["queue"], w["lag"], w["gauges"]
        flat |= {
            "published_ops_per_s": w["per_second"]["acknowledged_operations"],
            "published_entities_per_s": w["per_second"]["published_entities"],
            "committed_ops_per_s": w["per_second"]["committed_operations"],
            "lag_count": lag["count"],
            **{
                f"lag_{q}_ms": None
                if lag[f"{q}_micros"] is None
                else lag[f"{q}_micros"] / 1000
                for q in ("p50", "p95", "p99")
            },
            "lag_censored": lag["censored"],
            "unfinished_ops": lag["unfinished_operations"],
            "unfinished_oldest_s": lag["unfinished_oldest_micros_lower_bound"] / 1e6,
            "pending_members_start": gauges["pending_members"]["start"],
            "pending_members_end": gauges["pending_members"]["end"],
            "pending_members_max": gauges["pending_members"]["max"],
            "commit_conflicts": queue["commit_conflicts"],
            "publication_retries": queue["publication_retries"],
            "queue_read_bytes": queue["queue_read_bytes"],
            "queue_read_ms": queue["queue_read_micros"] / 1000,
            "drain_seconds": report["drain"]["seconds_to_empty"],
        }
    for role, usage in summary["resources"].items():
        flat[f"{role}_cpu_percent"] = usage["average_cpu_percent"]
        flat[f"{role}_peak_memory_bytes"] = usage["peak_memory_bytes"]
    return flat


def aggregate(summaries):
    """Median/min/max of headline metrics per (screen, layout, cache) group."""
    groups = defaultdict(list)
    for summary in summaries:
        run = summary.get("run") or {}
        groups[(run.get("screen"), run.get("layout"), run.get("cache"))].append(
            summary["headline"]
        )
    result = []
    for (screen, layout, cache), rows in sorted(
        groups.items(), key=lambda item: tuple(map(str, item[0]))
    ):
        metrics = {}
        for key in dict.fromkeys(k for row in rows for k in row):
            values = [
                row[key] for row in rows if isinstance(row.get(key), (int, float))
            ]
            if values:
                metrics[key] = {
                    "median": statistics.median(values),
                    "min": min(values),
                    "max": max(values),
                }
        result.append(
            {
                "screen": screen,
                "layout": layout,
                "cache": cache,
                "runs": len(rows),
                "metrics": metrics,
            }
        )
    return result


def markdown(groups):
    """One column per group, one row per metric (medians; min-max when repeated)."""
    names = list(dict.fromkeys(k for g in groups for k in g["metrics"]))

    def cell(m):
        if m is None:
            return ""
        if m["min"] == m["max"]:
            return f"{m['median']:.4g}"
        return f"{m['median']:.4g} ({m['min']:.4g}-{m['max']:.4g})"

    header = [
        f"{g['screen']}/{g['layout']}/{g['cache']} (n={g['runs']})" for g in groups
    ]
    lines = [
        "| metric | " + " | ".join(header) + " |",
        "|---" * (len(groups) + 1) + "|",
    ]
    lines += [
        f"| {name} | " + " | ".join(cell(g["metrics"].get(name)) for g in groups) + " |"
        for name in names
    ]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("runs", type=Path, nargs="+")
    parser.add_argument("--from-ns", type=int)
    parser.add_argument("--until-ns", type=int)
    parser.add_argument(
        "--compare",
        type=Path,
        help="write an aggregate comparison here (.json and .md)",
    )
    args = parser.parse_args()
    summaries = []
    for run in args.runs:
        existing = run / "summary.json"
        if args.compare is not None and existing.exists():
            summaries.append(json.loads(existing.read_text()))
            continue
        summary = summarize(run, from_ns=args.from_ns, until_ns=args.until_ns)
        existing.write_text(json.dumps(summary, indent=2) + "\n")
        (run / "summary.md").write_text(markdown(aggregate([summary])))
        summaries.append(summary)
    if args.compare is not None:
        groups = aggregate(summaries)
        args.compare.with_suffix(".json").write_text(
            json.dumps(groups, indent=2) + "\n"
        )
        args.compare.with_suffix(".md").write_text(markdown(groups))
        print(markdown(groups))


if __name__ == "__main__":
    main()
