#!/usr/bin/env python3
"""Seed per layout and run one open-loop screen on a single Docker host.

seed
    Fresh volume -> writer with the layout -> `seed.py` (graph rows, index
    builds, sampled verification) -> clean stop -> `snapshot.tar`. Seed each
    layout from the same fixture; sequential ID allocation makes their
    `entity-ids.u64le` identical, which `run` verifies against the trace.
run
    Restore an independent copy of the layout's snapshot -> writer (and a
    reader sharing the volume when any searches go to it) -> replay the trace
    open-loop -> wait for the queue to drain (bounded) -> clean stop ->
    `summary.json`/`summary.md`. Every run is a fresh process on a fresh copy.
    `--cache warm` requires the trace to start with warm-up phases; `cold`
    forbids them and also drops the host page cache when permitted (Linux,
    root), recording whether it did.

Local disk only. For S3 (EC2), compose `node.py`, `clone_seed.py`,
`replay.py` and `summarize.py` per host as the README describes.
"""

import argparse
import json
import platform
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path

import node
import replay
import seed as seeding
import summarize
from dataset import Dataset

HARNESS = Path(__file__).resolve().parent
UNITS = {
    "B": 1,
    "KiB": 2**10,
    "MiB": 2**20,
    "GiB": 2**30,
    "kB": 1e3,
    "MB": 1e6,
    "GB": 1e9,
}


def harness_hashes():
    return {
        path.name: node.sha256(path)
        for path in sorted(HARNESS.glob("*.py"))
        if not path.name.startswith("test_")
    }


def seed(args):
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    name = f"hxbench-seed-{args.layout}-{uuid.uuid4().hex[:8]}"
    node.docker("volume", "create", name)
    try:
        node.start(
            name,
            args.image,
            "writer",
            args.layout,
            args.port,
            output / "writer-node",
            volume=name,
            sample_ms=args.sample_ms,
        )
        with Dataset(args.fixture, small_payload=args.small_payload) as dataset:
            report = seeding.seed(
                dataset,
                args.family,
                args.layout,
                f"http://127.0.0.1:{args.port}",
                output / "seed",
                batch_size=args.batch_size,
                build_timeout_s=args.build_timeout_s,
            )
        node.stop(name, output / "writer")
        archive = node.snapshot(name, output / "snapshot.tar")
    finally:
        node.remove(name, name)
    record = {
        "layout": args.layout,
        "family": args.family,
        "image": node.image_identity(args.image),
        "snapshot": archive,
        "ids_sha256": report["ids_sha256"],
        "seed_sha256": node.sha256(output / "seed" / "seed.json"),
        "fixture": report["fixture"],
        "harness": harness_hashes(),
    }
    (output / "seed-run.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record, indent=2))


def sample_docker_stats(roles, sink, stop):
    """`docker stats` snapshots of `{container: role}`, about every two seconds."""
    while not stop.is_set():
        rows = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *roles],
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        for row in map(json.loads, rows):
            used = row["MemUsage"].split("/")[0].strip()
            unit = used.lstrip("0123456789.")
            sink.write(
                json.dumps(
                    {
                        "unix_ms": time.time_ns() // 1_000_000,
                        "role": roles[row["Name"]],
                        "cpu_percent": float(row["CPUPerc"].rstrip("%")),
                        "memory_bytes": int(float(used[: -len(unit)]) * UNITS[unit]),
                    }
                )
                + "\n"
            )
        sink.flush()


def drop_page_cache():
    """Best-effort cold host cache; returns whether it happened."""
    target = Path("/proc/sys/vm/drop_caches")
    if platform.system() != "Linux":
        return False
    try:
        subprocess.run(["sync"], check=True)
        target.write_text("3\n")
        return True
    except OSError:
        return False


def run(args):
    seed_dir, trace_dir, output = Path(args.seed), Path(args.trace), Path(args.output)
    seed_run = json.loads((seed_dir / "seed-run.json").read_text())
    manifest = json.loads((trace_dir / "manifest.json").read_text())
    if seed_run["layout"] != args.layout:
        raise ValueError("a database must reopen with the layout that wrote it")
    if seed_run["ids_sha256"] != manifest["ids_sha256"]:
        raise ValueError(
            "seed entity IDs differ from the trace's; traces would not match"
        )
    if node.sha256(trace_dir / "trace.jsonl") != manifest["trace_sha256"]:
        raise ValueError("trace file differs from its manifest")
    if node.sha256(seed_dir / "snapshot.tar") != seed_run["snapshot"]["sha256"]:
        raise ValueError("snapshot differs from its seed record")
    warmed = manifest["phases"][0][0].startswith("warmup")
    if warmed != (args.cache == "warm"):
        raise ValueError("warm runs need a warm-up phase; cold runs must not have one")
    output.mkdir(parents=True, exist_ok=False)
    shutil.copy(trace_dir / "manifest.json", output / "trace-manifest.json")
    tag = uuid.uuid4().hex[:8]
    writer, reader = f"hxbench-writer-{tag}", f"hxbench-reader-{tag}"
    config = {
        "status": "incomplete",
        "screen": args.screen,
        "layout": args.layout,
        "cache": args.cache,
        "repetition": args.repetition,
        "image": node.image_identity(args.image),
        "seed": seed_run | {"directory": str(seed_dir)},
        "trace": {
            "directory": str(trace_dir),
            "sha256": manifest["trace_sha256"],
            "family": manifest["family"],
            "specification": manifest["specification"],
        },
        "strong_on": args.strong_on,
        "eventual_on": args.eventual_on,
        "sample_ms": args.sample_ms,
        "workers": args.workers,
        "drain_timeout_s": args.drain_timeout_s,
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "harness": harness_hashes(),
        "started_unix_ns": time.time_ns(),
    }
    config["page_cache_dropped"] = args.cache == "cold" and drop_page_cache()
    (output / "run.json").write_text(json.dumps(config, indent=2) + "\n")
    with_reader = "reader" in (args.strong_on, args.eventual_on)
    stop = threading.Event()
    stats = (output / "docker-stats.jsonl").open("x") if args.docker_stats else None
    sampler = threading.Thread(
        target=sample_docker_stats,
        args=(
            {writer: "writer"} | ({reader: "reader"} if with_reader else {}),
            stats,
            stop,
        ),
    )
    try:
        config["restore"] = node.restore(seed_dir / "snapshot.tar", writer, args.image)
        node.start(
            writer,
            args.image,
            "writer",
            args.layout,
            args.port,
            output / "writer-node",
            volume=writer,
            sample_ms=args.sample_ms,
        )
        endpoints = {"writer": f"http://127.0.0.1:{args.port}"}
        if with_reader:
            node.start(
                reader,
                args.image,
                "reader",
                args.layout,
                args.port + 1,
                output / "reader-node",
                volume=writer,
                sample_ms=args.sample_ms,
            )
            endpoints["reader"] = f"http://127.0.0.1:{args.port + 1}"
        if stats is not None:
            sampler.start()
        config["replay"] = replay.run(
            trace_dir / "trace.jsonl",
            output / "replay",
            {
                replay.Kind.WRITE: endpoints["writer"],
                replay.Kind.STRONG: endpoints[args.strong_on],
                replay.Kind.EVENTUAL: endpoints[args.eventual_on],
            },
            dict.fromkeys(replay.Kind, args.workers),
            timeout_s=args.socket_timeout_s,
        )
        config["drained"] = node.wait_drained(writer, args.drain_timeout_s)
        stop.set()
        if with_reader:
            node.stop(reader, output / "reader")
        node.stop(writer, output / "writer")
        summary = summarize.summarize(output)
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        (output / "summary.md").write_text(
            summarize.markdown(summarize.aggregate([summary]))
        )
        config["status"] = "complete"
    finally:
        stop.set()
        if sampler.is_alive():
            sampler.join()
        if stats is not None:
            stats.close()
        config["finished_unix_ns"] = time.time_ns()
        (output / "run.json").write_text(json.dumps(config, indent=2) + "\n")
        if not args.keep:
            node.remove(reader)
            node.remove(writer, writer)
    print((output / "summary.md").read_text())


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("seed", "run"):
        sub = commands.add_parser(command)
        sub.add_argument("--image", required=True)
        sub.add_argument("--layout", choices=seeding.LAYOUTS, required=True)
        sub.add_argument("--output", type=Path, required=True)
        sub.add_argument("--port", type=int, default=18700)
        sub.add_argument("--sample-ms", type=int, default=1000)
    planting = commands.choices["seed"]
    planting.add_argument("--fixture", type=Path, required=True)
    planting.add_argument("--small-payload", action="store_true")
    planting.add_argument("--family", choices=seeding.FAMILIES, default="combined")
    planting.add_argument("--batch-size", type=int, default=64)
    planting.add_argument("--build-timeout-s", type=int, default=86400)
    running = commands.choices["run"]
    running.add_argument("--seed", type=Path, required=True)
    running.add_argument("--trace", type=Path, required=True)
    running.add_argument("--screen", required=True)
    running.add_argument("--cache", choices=("warm", "cold"), default="warm")
    running.add_argument("--repetition", type=int, default=1)
    running.add_argument("--strong-on", choices=("writer", "reader"), default="writer")
    running.add_argument(
        "--eventual-on", choices=("writer", "reader"), default="writer"
    )
    running.add_argument("--workers", type=int, default=64)
    running.add_argument("--socket-timeout-s", type=float, default=120)
    running.add_argument("--drain-timeout-s", type=int, default=600)
    running.add_argument("--docker-stats", action="store_true")
    running.add_argument(
        "--keep", action="store_true", help="keep containers and volume"
    )
    args = parser.parse_args()
    if args.command == "seed":
        seed(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
