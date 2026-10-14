#!/usr/bin/env python3
"""Collect one Linux cgroup-v2 measurement window without changing its limits.

Run on the database host as root. The retained memory.peak descriptor is reset
once at the start; its high-water mark includes spikes between samples. Never
reset between samples: that would leave a read/reset gap. CPU is cumulative
cgroup time, including descendants, rather than host-wide CPU utilization.
A stopped/replaced container or a counter regression makes the capture fail.
"""

import argparse
import hashlib
import json
import os
import platform
import subprocess
import time
from pathlib import Path


def counters(path):
    result = {}
    for line in path.read_text().splitlines():
        fields = line.split()
        if (
            len(fields) != 2
            or fields[0] in result
            or not fields[1].isascii()
            or not fields[1].isdigit()
        ):
            raise ValueError(f"invalid counters: {path}")
        result[fields[0]] = int(fields[1])
    if not result:
        raise ValueError(f"empty counters: {path}")
    return result


def identity(pid, proc=Path("/proc"), root=Path("/sys/fs/cgroup")):
    # comm can contain spaces or parentheses; fields after its final ')' begin
    # with state (field 3), making starttime (field 22) offset 19.
    stat = (proc / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
    if stat[0] in {"Z", "X"}:
        raise ValueError("container process has exited")
    start = int(stat[19])
    lines = (proc / str(pid) / "cgroup").read_text().splitlines()
    if len(lines) != 1 or not lines[0].startswith("0::/"):
        raise ValueError("a unified cgroup-v2 hierarchy is required")
    relative = Path(lines[0][4:])
    if not relative.parts or ".." in relative.parts:
        raise ValueError("a non-root cgroup is required")
    path = root / relative
    info = path.stat()
    return {
        "pid": pid,
        "start_ticks": start,
        "cgroup": str(path),
        "device": info.st_dev,
        "inode": info.st_ino,
    }


def peak_value(fd):
    os.lseek(fd, 0, os.SEEK_SET)
    value = os.read(fd, 128).strip()
    if not value.isdigit():
        raise ValueError("invalid memory.peak")
    return int(value)


def sample(group, fd, previous=None):
    begin = time.monotonic_ns()
    cpu = counters(group / "cpu.stat")
    events = counters(group / "memory.events")
    for field in ("usage_usec", "user_usec", "system_usec"):
        if field not in cpu:
            raise ValueError(f"missing CPU counter: {field}")
    if previous:
        for name, values in (("cpu", cpu), ("memory_events", events)):
            if not previous[name].keys() <= values.keys() or any(
                values[key] < value for key, value in previous[name].items()
            ):
                raise ValueError(f"{name} counter regression")
    current = (group / "memory.current").read_text().strip()
    if not current.isascii() or not current.isdigit():
        raise ValueError("invalid memory.current")
    peak = peak_value(fd)
    if previous and peak < previous["peak_bytes"]:
        raise ValueError("memory peak regressed")
    return {
        "monotonic_begin_ns": begin,
        "unix_ns": time.time_ns(),
        "monotonic_end_ns": time.monotonic_ns(),
        "cpu": cpu,
        "memory_events": events,
        "current_bytes": int(current),
        "peak_bytes": peak,
    }


def collect(
    container,
    output,
    duration_ns,
    interval_ns,
    *,
    proc=Path("/proc"),
    root=Path("/sys/fs/cgroup"),
    start_unix_ns=None,
):
    if duration_ns <= 0 or interval_ns <= 0:
        raise ValueError("duration and interval must be positive")
    inspected = json.loads(
        subprocess.check_output(["docker", "inspect", container], text=True)
    )
    info = inspected[0]
    if not info["State"]["Running"] or info["State"]["Pid"] <= 0:
        raise ValueError("container is not running")
    pid = info["State"]["Pid"]
    clock_before = time.monotonic_ns()
    clock_unix = time.time_ns()
    clock_after = time.monotonic_ns()
    scheduled = None
    if start_unix_ns is not None:
        if (
            type(start_unix_ns) is not int
            or not 0 < start_unix_ns - clock_unix <= 3_600_000_000_000
        ):
            raise ValueError("scheduled start must be in the next hour")
        scheduled = (clock_before + clock_after) // 2 + start_unix_ns - clock_unix
    expected = identity(pid, proc, root)
    group = Path(expected["cgroup"])
    metadata = {
        "schema": 1,
        "clock_monotonic_before_ns": clock_before,
        "clock_unix_ns": clock_unix,
        "clock_monotonic_after_ns": clock_after,
        "scheduled_start_unix_ns": start_unix_ns,
        "scheduled_start_monotonic_ns": scheduled,
        "container_id": info["Id"],
        "image_id": info["Image"],
        "identity": expected,
        "kernel": platform.release(),
        "collector_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "duration_ns": duration_ns,
        "interval_ns": interval_ns,
        "cpu_scope": "cgroup including descendants; one core is 100 percent",
        "memory_scope": "cgroup including descendants and charged file cache",
        "peak_scope": "retained descriptor reset through final sample",
    }
    output.mkdir(parents=True, exist_ok=False)
    # Exclusive output and a start record remain as incomplete evidence on error.
    with (output / "resources.jsonl").open("x") as stream:

        def emit(record):
            stream.write(json.dumps(record, sort_keys=True) + "\n")
            stream.flush()

        emit({"type": "start", **metadata})
        fd = None
        try:
            fd = os.open(group / "memory.peak", os.O_RDWR | os.O_CLOEXEC)
            if scheduled is not None:
                while (remaining := scheduled - time.monotonic_ns()) > 0:
                    time.sleep(min(remaining / 1e9, 0.25))
                if identity(pid, proc, root) != expected:
                    raise ValueError("container changed before scheduled capture")
            lifetime_peak = peak_value(fd)
            reset_begin = time.monotonic_ns()
            if os.write(fd, b"0") != 1:
                raise OSError("short memory peak reset")
            reset_end = time.monotonic_ns()
            first = sample(group, fd)
            first["sample"] = 0
            emit({"type": "sample", **first})
            start = first["monotonic_begin_ns"]
            deadline = (start if scheduled is None else scheduled) + duration_ns
            if start >= deadline:
                raise ValueError("missed the entire scheduled capture window")
            previous = first
            count = 1
            skipped = 0
            next_at = min(start + interval_ns, deadline)
            while True:
                time.sleep(max(0, next_at - time.monotonic_ns()) / 1e9)
                if identity(pid, proc, root) != expected:
                    raise ValueError("container process or cgroup changed")
                current = sample(group, fd, previous)
                current.update(
                    sample=count,
                    scheduled_monotonic_ns=next_at,
                    lateness_ns=max(0, current["monotonic_begin_ns"] - next_at),
                )
                emit({"type": "sample", **current})
                count += 1
                previous = current
                if current["monotonic_begin_ns"] >= deadline:
                    break
                # Skip missed ticks rather than emitting a burst of fake samples.
                tick = (current["monotonic_end_ns"] - start) // interval_ns + 1
                desired = start + tick * interval_ns
                skipped += max(0, (desired - next_at) // interval_ns - 1)
                next_at = min(desired, deadline)
            elapsed = current["monotonic_begin_ns"] - start
            cpu = {
                key: current["cpu"][key] - value for key, value in first["cpu"].items()
            }
            report = {
                **metadata,
                "status": "complete",
                "samples": count,
                "skipped_ticks": skipped,
                "elapsed_ns": elapsed,
                "start_unix_ns": first["unix_ns"],
                "start_lateness_ns": 0
                if scheduled is None
                else max(0, start - scheduled),
                "end_unix_ns": current["unix_ns"],
                "peak_reset_begin_ns": reset_begin,
                "peak_reset_end_ns": reset_end,
                "prior_lifetime_peak_bytes": lifetime_peak,
                "peak_bytes": current["peak_bytes"],
                "cpu_delta": cpu,
                "average_cpu_percent": cpu["usage_usec"] * 100_000 / elapsed,
                "memory_events_delta": {
                    key: current["memory_events"][key] - value
                    for key, value in first["memory_events"].items()
                },
                "release_or_performance_acceptance": False,
            }
            emit({"type": "end", **report})
            (output / "resources.json").write_text(json.dumps(report, indent=2) + "\n")
            return report
        except BaseException as error:
            emit(
                {
                    "type": "incomplete",
                    "error": type(error).__name__,
                    "detail": str(error),
                }
            )
            raise
        finally:
            if fd is not None:
                os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--interval-ms", type=int, default=1000)
    parser.add_argument("--start-unix-ns", type=int)
    args = parser.parse_args()
    collect(
        args.container,
        args.output,
        args.duration_seconds * 1_000_000_000,
        args.interval_ms * 1_000_000,
        start_unix_ns=args.start_unix_ns,
    )


if __name__ == "__main__":
    main()
