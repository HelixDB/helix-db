#!/usr/bin/env python3
"""Plan screening and final-comparison trace specifications (plans, not results).

`screens` expands the seven screening workloads with short local defaults:
vector-only, text-only, combined, concurrent strong/eventual searches,
repeated hot-entity updates, index-property removal plus whole-entity
deletion, and sustained load with backlog growth (1x/2x/4x the write rate).

`final` expands reviewed calibration rates (below/near/above the observed drain
capacity, per fixture and family) into sustained and backlog-growth profiles,
warm (warm-up then measurement) and cold (measurement only, fresh process),
and N repetitions. Every layout replays the same trace per cell; the layout
order alternates by repetition to spread drift. Neither command sends traffic.
"""

import argparse
import hashlib
import itertools
import json
from pathlib import Path

from workload import validate

LAYOUTS = ("map", "rows")
FAMILIES = ("vector", "text", "combined")
MIX = {
    "insert_percent": 20,
    "remove_percent": 10,
    "delete_percent": 10,
    "hot_percent": 90,
    "hot_entities": 64,
}


def phase(name, seconds, write, strong, eventual):
    return {
        "name": name,
        "duration_s": seconds,
        "write_rps": write,
        "strong_rps": strong,
        "eventual_rps": eventual,
    }


def specification(phases, random_seed, top_k=10, **mix):
    spec = {"random_seed": random_seed, "top_k": top_k, **(MIX | mix), "phases": phases}
    validate(spec)
    return spec


def screens(*, write_rps=20, search_rps=5, duration_s=60, warmup_s=0, random_seed=1827):
    """`{screen: {"family", "spec"}}`; a nonzero warm-up makes every screen warm."""
    warm = (
        [phase("warmup", warmup_s, write_rps, search_rps, search_rps)]
        if warmup_s
        else []
    )

    def steady(write=write_rps, search=search_rps):
        return warm + [phase("measure", duration_s, write, search, search)]

    growth = warm + [
        phase(f"load-{m}x", duration_s, m * write_rps, search_rps, search_rps)
        for m in (1, 2, 4)
    ]
    return {
        name: {"family": family, "spec": specification(phases, random_seed, **mix)}
        for name, family, phases, mix in (
            ("vector", "vector", steady(), {}),
            ("text", "text", steady(), {}),
            ("combined", "combined", steady(), {}),
            ("searches", "combined", steady(search=4 * search_rps), {}),
            (
                "hot-updates",
                "combined",
                steady(),
                {
                    "insert_percent": 0,
                    "remove_percent": 0,
                    "delete_percent": 0,
                    "hot_percent": 100,
                    "hot_entities": 8,
                },
            ),
            (
                "removal-deletion",
                "combined",
                steady(),
                {
                    "insert_percent": 10,
                    "remove_percent": 40,
                    "delete_percent": 40,
                    "hot_percent": 50,
                },
            ),
            ("backlog", "combined", growth, {}),
        )
    }


def final(calibration, *, warmup_s=600, measure_s=1800, repetitions=3, layouts=LAYOUTS):
    """Final matrix from calibration `{"evidence": str, "rates": [...]}`.

    Each rate entry: `fixture` (`dbpedia` or `small`), `family`, positive
    integer `below`, sorted unique `near` list, `above`, `strong`, `eventual`
    (requests per second), with below < near < above.
    """
    if (
        set(calibration) != {"evidence", "rates"}
        or not str(calibration["evidence"]).strip()
    ):
        raise ValueError("expected a calibration evidence reference and rates")
    if (
        not calibration["rates"]
        or repetitions < 1
        or not set(layouts) <= set(LAYOUTS)
        or not layouts
    ):
        raise ValueError("need rates, at least one repetition, and known layouts")
    fields = {"fixture", "family", "below", "near", "above", "strong", "eventual"}
    seen = set()
    traces, runs = {}, []
    for entry in calibration["rates"]:
        if (
            set(entry) != fields
            or entry["fixture"] not in ("dbpedia", "small")
            or entry["family"] not in FAMILIES
        ):
            raise ValueError("invalid calibration entry")
        if (entry["fixture"], entry["family"]) in seen:
            raise ValueError("duplicate fixture/family calibration")
        seen.add((entry["fixture"], entry["family"]))
        near = entry["near"]
        numbers = [entry[k] for k in ("below", "above", "strong", "eventual")] + list(
            near
        )
        if not near or any(type(n) is not int or n < 1 for n in numbers):
            raise ValueError(
                "rates must be positive integers with at least one near rate"
            )
        if (
            near != sorted(set(near))
            or not entry["below"] < near[0] <= near[-1] < entry["above"]
        ):
            raise ValueError("rates must increase from below through near to above")
        strong, eventual = entry["strong"], entry["eventual"]
        profiles = {
            "below": [(measure_s, entry["below"])],
            **{f"near-{i}": [(measure_s, n)] for i, n in enumerate(near, 1)},
            "above": [(measure_s, entry["above"])],
            "growing": [(measure_s // 3, entry["above"] * m) for m in (1, 2, 4)],
        }
        for profile, cache, repetition in itertools.product(
            profiles, ("warm", "cold"), range(1, repetitions + 1)
        ):
            identity = (
                f"{entry['fixture']}-{entry['family']}-{profile}-{cache}-r{repetition}"
            )
            phases = (
                [phase("warmup", warmup_s, entry["below"], strong, eventual)]
                if cache == "warm"
                else []
            )
            phases += [
                phase(f"measure-{i}", seconds, rate, strong, eventual)
                for i, (seconds, rate) in enumerate(profiles[profile], 1)
            ]
            traces[identity] = {
                "family": entry["family"],
                "fixture": entry["fixture"],
                "spec": specification(phases, 1826 + repetition),
            }
            order = layouts if repetition % 2 else tuple(reversed(layouts))
            runs += [
                {
                    "id": f"{identity}-{layout}",
                    "trace": identity,
                    "layout": layout,
                    "fixture": entry["fixture"],
                    "family": entry["family"],
                    "profile": profile,
                    "cache": cache,
                    "repetition": repetition,
                    "seconds": sum(p["duration_s"] for p in phases),
                }
                for layout in order
            ]
    return {
        "status": "planned_not_measured",
        "calibration": calibration,
        "traces": traces,
        "runs": runs,
        "planned_load_hours": sum(r["seconds"] for r in runs) / 3600,
    }


def write(plan, output):
    """Writes one spec file per trace, then the plan with their hashes."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    hashes = {}
    for identity, trace in plan.pop("traces").items():
        data = (json.dumps(trace["spec"], indent=2) + "\n").encode()
        (output / f"{identity}.json").write_bytes(data)
        hashes[identity] = {k: v for k, v in trace.items() if k != "spec"} | {
            "sha256": hashlib.sha256(data).hexdigest()
        }
    plan["specifications"] = hashes
    (output / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    return plan


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    screening = commands.add_parser("screens")
    screening.add_argument("--output", type=Path, required=True)
    screening.add_argument("--write-rps", type=int, default=20)
    screening.add_argument("--search-rps", type=int, default=5)
    screening.add_argument("--duration-s", type=int, default=60)
    screening.add_argument("--warmup-s", type=int, default=0)
    screening.add_argument("--random-seed", type=int, default=1827)
    finals = commands.add_parser("final")
    finals.add_argument("--calibration", type=Path, required=True)
    finals.add_argument("--output", type=Path, required=True)
    finals.add_argument("--warmup-s", type=int, default=600)
    finals.add_argument("--measure-s", type=int, default=1800)
    finals.add_argument("--repetitions", type=int, default=3)
    finals.add_argument("--layouts", nargs="+", choices=LAYOUTS, default=list(LAYOUTS))
    args = parser.parse_args()
    if args.command == "screens":
        plan = {
            "status": "planned_not_measured",
            "traces": screens(
                write_rps=args.write_rps,
                search_rps=args.search_rps,
                duration_s=args.duration_s,
                warmup_s=args.warmup_s,
                random_seed=args.random_seed,
            ),
        }
    else:
        plan = final(
            json.loads(args.calibration.read_text()),
            warmup_s=args.warmup_s,
            measure_s=args.measure_s,
            repetitions=args.repetitions,
            layouts=tuple(args.layouts),
        )
    print(json.dumps(write(plan, args.output), indent=2))


if __name__ == "__main__":
    main()
