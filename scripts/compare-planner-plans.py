#!/usr/bin/env python3
"""Diff complete planner captures. Every change requires review; never bless it."""

import argparse
import difflib
import hashlib
import json
from pathlib import Path


def validate(capture):
    if not isinstance(capture, dict) or capture.get("schema") != 1:
        raise ValueError("unsupported planner capture schema")
    cases = capture.get("cases")
    if not isinstance(cases, dict) or not cases:
        raise ValueError("missing or empty planner capture")
    for name, case in cases.items():
        if not isinstance(case, dict) or not {"input", "outcome"} <= case.keys():
            raise ValueError("incomplete planner case: " + name)
        outcome = case["outcome"]
        if not isinstance(outcome, dict) or ("plan" in outcome) == ("error" in outcome):
            raise ValueError("expected exactly one plan or rejection: " + name)
        if not isinstance(case["input"], dict):
            raise ValueError("invalid planner input: " + name)
        if "error" in outcome:
            if not isinstance(outcome["error"], str) or not outcome["error"]:
                raise ValueError("invalid planner rejection: " + name)
        else:
            plan = outcome["plan"]
            if not isinstance(plan, dict):
                raise ValueError("invalid selected plan: " + name)
            selected_metrics = plan.get("metrics", plan.get("planner"))
            if not isinstance(selected_metrics, dict) or not selected_metrics:
                raise ValueError("missing planner metrics: " + name)
    return cases


def metrics(case):
    plan = case["outcome"].get("plan", {})
    return plan.get("metrics", plan.get("planner", {}))


def deltas(before, after):
    return {key: {"before": before.get(key), "after": after.get(key)}
            for key in sorted(before.keys() | after.keys())
            if before.get(key) != after.get(key)}


def compare(before, after):
    old, new = validate(before), validate(after)
    report = {"schema": 1, "before": len(old), "after": len(new),
              "missing": sorted(old.keys() - new.keys()),
              "added": sorted(new.keys() - old.keys()), "identical": 0, "changes": []}
    for name in sorted(old.keys() & new.keys()):
        first, second = old[name], new[name]
        if first == second:
            report["identical"] += 1
            continue
        first_metrics, second_metrics = metrics(first), metrics(second)
        change = {"case": name, "input_changed": first["input"] != second["input"],
                  "error_changed": first["outcome"].get("error") != second["outcome"].get("error"),
                  "metrics": deltas(first_metrics, second_metrics),
                  "cost": deltas(first_metrics.get("selected_cost", {}),
                                 second_metrics.get("selected_cost", {}))}
        change["metrics"].pop("selected_cost", None)
        report["changes"].append(change)
    report["review_required"] = bool(report["changes"] or report["missing"] or report["added"])
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    before, after = (json.loads(path.read_text()) for path in [args.baseline, args.candidate])
    report = compare(before, after)
    # Keep separate evidence for each comparison; never overwrite an old review.
    args.output.mkdir(parents=True, exist_ok=False)
    changes = {change["case"]: change for change in report["changes"]}
    for name in sorted(before["cases"].keys() | after["cases"].keys()):
        first, second = before["cases"].get(name), after["cases"].get(name)
        if first == second:
            continue
        diff = "".join(difflib.unified_diff(
            json.dumps(first, indent=2, sort_keys=True).splitlines(True),
            json.dumps(second, indent=2, sort_keys=True).splitlines(True),
            fromfile="baseline/" + name, tofile="candidate/" + name))
        filename = hashlib.sha256(name.encode()).hexdigest() + ".diff"
        (args.output / filename).write_text(diff)
        if name in changes:
            changes[name]["diff"] = filename
    (args.output / "comparison.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "changes"}
                     | {"changed": len(report["changes"])}))
    raise SystemExit(1 if report["review_required"] else 0)


if __name__ == "__main__":
    main()
