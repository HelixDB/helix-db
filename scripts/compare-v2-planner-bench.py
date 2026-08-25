#!/usr/bin/env python3
"""Run and compare frozen baseline/candidate V2 planner benchmark binaries."""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
from pathlib import Path
from typing import Any, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--repetitions", type=int, default=10)
    return parser.parse_args()


def run(binary: Path, variant: str) -> dict[str, Any]:
    environment = os.environ.copy()
    environment["HELIX_PLANNER_BENCH_VARIANT"] = variant
    completed = subprocess.run(
        [str(binary)],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    return json.loads(completed.stdout)


def percentile(values: list[int], percentage: int) -> int:
    ordered = sorted(values)
    index = max(0, (len(ordered) * percentage + 99) // 100 - 1)
    return ordered[index]


def case_key(case: dict[str, Any]) -> tuple[str, int]:
    return case["name"], case["population"]


def normalized_shape(case: dict[str, Any], variant: str) -> str:
    statistics = case["planner_statistics"]
    accesses = (
        statistics["node_accesses"]
        if case["element"] == "node"
        else statistics["edge_accesses"]
    )
    if variant == "baseline":
        if "ordered_range" in case["name"]:
            return "legacy_row_merge_sort"
        if "same_index_union" in case["name"]:
            return "legacy_equality_range_union"
        if "multi_index_intersection" in case["name"]:
            return "legacy_equality_range_intersection"
        if "unique_equality" in case["name"]:
            return "legacy_unique_equality_range"
        if accesses["equality_index_lookups"]:
            return "legacy_equality_range"
        if accesses["range_index_scans"]:
            return "legacy_ordered_range"
    if shape := case.get("selected_shape"):
        return shape
    return f"{variant}_other"


def aggregate(runs: list[dict[str, Any]], variant: str) -> dict[str, Any]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for report in runs:
        for case in report["cases"]:
            grouped.setdefault(case_key(case), []).append(case)
    cases = []
    for key in sorted(grouped, key=lambda item: (item[1], item[0])):
        samples = grouped[key]
        representative = samples[-1]
        planning_samples = [sample["planning_nanos_p50"] for sample in samples]
        planning_p95_samples = [sample["planning_nanos_p95"] for sample in samples]
        cases.append(
            {
                "name": key[0],
                "population": key[1],
                "element": representative["element"],
                "selected_shape": normalized_shape(representative, variant),
                "plan_digest": representative["plan_digest"],
                "selected_cost": representative["selected_cost"],
                "planning_nanos_p50": int(statistics.median(planning_samples)),
                "planning_nanos_p95": percentile(planning_p95_samples, 95),
                "planning_throughput_per_second_p50": (
                    1_000_000_000 / statistics.median(planning_samples)
                ),
                "allocations_per_plan": representative.get("allocations_per_plan"),
                "allocated_bytes_per_plan": representative.get(
                    "allocated_bytes_per_plan"
                ),
                "planner_statistics": representative["planner_statistics"],
            }
        )
    return {"variant": variant, "runs": len(runs), "cases": cases}


def percent_delta(candidate: float, baseline: float) -> Optional[float]:
    if baseline == 0:
        return None
    return (candidate - baseline) * 100 / baseline


def comparison(
    baseline: dict[str, Any], candidate: dict[str, Any], repetitions: int
) -> dict[str, Any]:
    baseline_cases = {case_key(case): case for case in baseline["cases"]}
    candidate_cases = {case_key(case): case for case in candidate["cases"]}
    cases = []
    for key in sorted(candidate_cases, key=lambda item: (item[1], item[0])):
        before = baseline_cases[key]
        after = candidate_cases[key]
        cases.append(
            {
                "name": key[0],
                "population": key[1],
                "baseline_shape": before["selected_shape"],
                "candidate_shape": after["selected_shape"],
                "baseline_plan_digest": before["plan_digest"],
                "candidate_plan_digest": after["plan_digest"],
                "planning_p50_delta_percent": percent_delta(
                    after["planning_nanos_p50"], before["planning_nanos_p50"]
                ),
                "planning_p95_delta_percent": percent_delta(
                    after["planning_nanos_p95"], before["planning_nanos_p95"]
                ),
                "baseline_cost": before["selected_cost"],
                "candidate_cost": after["selected_cost"],
                "baseline_allocations_per_plan": before["allocations_per_plan"],
                "candidate_allocations_per_plan": after["allocations_per_plan"],
                "baseline_allocated_bytes_per_plan": before["allocated_bytes_per_plan"],
                "candidate_allocated_bytes_per_plan": after[
                    "allocated_bytes_per_plan"
                ],
            }
        )
    return {
        "schema_version": 1,
        "host": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "protocol": {
            "warmup_repetitions_per_binary_run": 3,
            "measured_repetitions_per_case_per_binary_run": 10,
            "interleaved_binary_runs": repetitions,
            "performance_deltas": "report_only",
        },
        "baseline": baseline,
        "candidate": candidate,
        "cases": cases,
    }


def markdown(report: dict[str, Any]) -> str:
    lines = [
        "# V2 planner benchmark comparison",
        "",
        (
            "Report-only results from ten interleaved baseline/candidate runs on "
            f"{report['host']['system']} {report['host']['machine']}. Each binary run "
            "performs three warmups and ten measured plans per case."
        ),
        "",
        "| Population | Case | Baseline shape | Candidate shape | p50 planning delta | "
        "Baseline costed I/O | Candidate costed I/O | Allocations/plan |",
        "|---:|---|---|---|---:|---|---|---:|",
    ]
    for case in report["cases"]:
        delta = case["planning_p50_delta_percent"]
        delta_text = "n/a" if delta is None else f"{delta:+.1f}%"
        before = case["baseline_cost"]
        after = case["candidate_cost"]
        before_io = (
            f"get={before.get('object_reads', 0)}, multi={before.get('multi_get_calls', 0)}, "
            f"seek={before.get('range_seeks', 0)}, next={before.get('range_nexts', 0)}"
        )
        after_io = (
            f"get={after.get('object_reads', 0)}, multi={after.get('multi_get_calls', 0)}, "
            f"seek={after.get('range_seeks', 0)}, next={after.get('range_nexts', 0)}, "
            f"auth={after.get('authoritative_graph_reads', 0)}"
        )
        allocations = (
            f"{case['baseline_allocations_per_plan']:.0f} → "
            f"{case['candidate_allocations_per_plan']:.0f}"
        )
        lines.append(
            f"| {case['population']} | {case['name']} | {case['baseline_shape']} | "
            f"{case['candidate_shape']} | {delta_text} | {before_io} | {after_io} | "
            f"{allocations} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Non-unique equality and equality sets move from range iteration to V4 "
            "bitmap point reads, including one multi-get for same-index unions.",
            "- Equality intersections combine verified IDs before the final row materialization.",
            "- Mixed equality/range cases use the ordered range as the driver, filter verified "
            "bitmap IDs, and no longer require an explicit sort.",
            "- Unique equality and range candidates retain authoritative graph verification; "
            "non-unique bitmap IDs do not add a graph existence read.",
            "- I/O columns are the selected plan's storage-cost components; functional "
            "executor tests separately assert observed point, multi-get, and graph-read counts.",
            "- Latency deltas are observations, not pass/fail gates.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    if args.repetitions <= 0:
        raise SystemExit("--repetitions must be positive")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    baseline_runs = []
    candidate_runs = []
    for _ in range(args.repetitions):
        baseline_runs.append(run(args.baseline, "baseline"))
        candidate_runs.append(run(args.candidate, "candidate"))
    baseline = aggregate(baseline_runs, "baseline")
    candidate = aggregate(candidate_runs, "candidate")
    report = comparison(baseline, candidate, args.repetitions)
    (args.output_dir / "baseline.json").write_text(
        json.dumps(baseline, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_dir / "candidate.json").write_text(
        json.dumps(candidate, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_dir / "comparison.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_dir / "README.md").write_text(markdown(report), encoding="utf-8")


if __name__ == "__main__":
    main()
