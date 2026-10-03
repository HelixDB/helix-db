#!/usr/bin/env python3
"""Start, stop, snapshot and restore one benchmark server container on this host.

Run on each database host (directly, or through SSM on EC2). Storage is one of:

* a Docker named volume (`--volume`): local disk, which a writer and a reader
  on the same host can share;
* S3 (`--s3-env FILE --db-path PREFIX`): FILE holds `KEY=VALUE` lines for
  `S3_BUCKET`, `S3_REGION`, optional `AWS_ENDPOINT`/`AWS_ALLOW_HTTP` and, when no
  instance role applies, credentials. Values reach Docker through a private
  env file, never process arguments; recorded configuration redacts secrets.
  S3 storage always caches on local disk, at `/var/cache/helix` in the
  container's writable layer, so a fresh container starts cold. The startup
  cache warm is turned off (`HELIX_DISK_CACHE_WARM=off`): its background S3
  reads would overlap the replay and its counters. FILE may override that.

Every container gets `--ulimit nofile=65536:65536`; the S3 disk cache needs at
least 26,600 open files at its default budget.

`drain` waits until the writer's latest sample shows no pending operation.
`stop` sends SIGTERM, requires a clean exit, and saves stdout (samples, and the
final post-close sample), stderr, `docker inspect`, and the parsed samples.
`snapshot`/`restore` copy a cleanly stopped writer's data directory through
`docker cp`, so no utility image is needed. S3 prefixes are copied with
`clone_seed.py` instead. The database must reopen with the layout that wrote it.
"""

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import samples

DATA_DIR = "/var/lib/helix"
NOFILE = 65536
SECRETS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


def docker(*args, **options):
    return subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True, **options
    ).stdout


def inspect(kind, name):
    return json.loads(docker(kind, "inspect", name))[0]


def image_identity(image):
    info = inspect("image", image)
    return {
        "name": image,
        "id": info["Id"],
        "repo_digests": info.get("RepoDigests") or [],
        "architecture": info["Architecture"],
    }


def read_env_file(path):
    env = {}
    for line in Path(path).read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            key, separator, value = line.partition("=")
            if not separator or not key.strip():
                raise ValueError("env files hold KEY=VALUE lines")
            env[key.strip()] = value
    return env


def redacted(env):
    return {k: "<redacted>" if k in SECRETS else v for k, v in sorted(env.items())}


def wait_ready(name, port, timeout_s):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urlopen(f"http://127.0.0.1:{port}/readyz", timeout=5) as response:
                if response.status == 200:
                    return json.load(response)
        except (URLError, ConnectionError, OSError, ValueError):
            pass
        if not inspect("container", name)["State"]["Running"]:
            break
        time.sleep(0.2)
    raise RuntimeError(f"{name} did not become ready; see `docker logs {name}`")


def start(
    name,
    image,
    role,
    layout,
    port,
    output,
    *,
    volume=None,
    s3_env=None,
    db_path=None,
    network=None,
    bind="127.0.0.1",
    sample_ms=1000,
    ready_timeout_s=300,
):
    """Runs one detached server and records its exact configuration."""
    if role not in ("writer", "reader") or layout not in ("map", "rows"):
        raise ValueError("invalid role or layout")
    if (volume is None) == (s3_env is None) or (s3_env is None) != (db_path is None):
        raise ValueError("use exactly one of --volume or --s3-env with --db-path")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    env = {
        "HELIX_BENCHMARK_ROLE": role,
        "HELIX_BENCHMARK_SAMPLE_MS": str(sample_ms),
        "HELIX_INDEX_QUEUE_LAYOUT": layout,
    }
    command = ["run", "-d", "--name", name, "-p", f"{bind}:{port}:8080"]
    command += ["--ulimit", f"nofile={NOFILE}:{NOFILE}"]
    if volume is not None:
        env["HELIX_DATA_DIR"] = DATA_DIR
        command += ["--mount", f"type=volume,source={volume},target={DATA_DIR}"]
    else:
        env |= (
            {"HELIX_DISK_CACHE_WARM": "off"}
            | read_env_file(s3_env)
            | {"DB_PATH": db_path}
        )
    if network is not None:
        command += ["--network", network]
    with tempfile.TemporaryDirectory() as private:
        env_file = Path(private) / "env"
        env_file.write_text("".join(f"{k}={v}\n" for k, v in env.items()))
        os.chmod(env_file, 0o600)
        container = docker(*command, "--env-file", str(env_file), image).strip()
    record = {
        "name": name,
        "container_id": container,
        "role": role,
        "layout": layout,
        "port": port,
        "image": image_identity(image),
        "storage": {"volume": volume}
        if volume is not None
        else {"s3_prefix": db_path, "bucket": env.get("S3_BUCKET")},
        "env": redacted(env),
        "nofile": NOFILE,
        "started_unix_ns": time.time_ns(),
    }
    record["readyz"] = wait_ready(name, port, ready_timeout_s)
    record["ready_unix_ns"] = time.time_ns()
    (output / "node.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


def latest(name):
    """The most recent sample in the container's log tail, or None."""
    tail = subprocess.run(
        ["docker", "logs", "--tail", "200", name],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    ).stdout
    found = [
        line
        for line in tail.splitlines()
        if line.startswith("{") and samples.MARKER in line
    ]
    return json.loads(found[-1]) if found else None


def wait_drained(name, timeout_s):
    """Polls the latest sample until no operation is pending; False on timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        sample = latest(name)
        if sample is not None and sample["queue"]["pending_operations"] == 0:
            return True
        time.sleep(1)
    return False


def stop(name, output, *, timeout_s=120):
    """SIGTERM, then preserve logs and samples; raises unless the exit was clean."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    docker("stop", "-t", str(timeout_s), name)
    info = inspect("container", name)
    (output / "inspect.json").write_text(json.dumps(info, indent=2) + "\n")
    with (
        (output / "stdout.log").open("x") as stdout,
        (output / "stderr.log").open("x") as stderr,
    ):
        subprocess.run(
            ["docker", "logs", name], stdout=stdout, stderr=stderr, check=True
        )
    with (output / "stdout.log").open() as stdout:
        stream = samples.parse(stdout)
    with (output / "samples.jsonl").open("x") as sink:
        sink.writelines(json.dumps(s, separators=(",", ":")) + "\n" for s in stream)
    with (output / "timeseries.jsonl").open("x") as sink:
        sink.writelines(json.dumps(row) + "\n" for row in samples.timeseries(stream))
    state = info["State"]
    anomalies = samples.check(stream)
    report = {
        "clean": state["ExitCode"] == 0 and not state["OOMKilled"],
        "exit_code": state["ExitCode"],
        "oom_killed": state["OOMKilled"],
        "finished_at": state["FinishedAt"],
        "samples": len(stream),
        "anomaly_count": len(anomalies),
        "anomalies": anomalies[:100],
        "final_queue": stream[-1]["queue"],
    }
    (output / "stop.json").write_text(json.dumps(report, indent=2) + "\n")
    if not report["clean"]:
        raise RuntimeError(f"{name} did not stop cleanly: {report['exit_code']}")
    return report


def snapshot(name, archive):
    """Tars a cleanly stopped writer's data directory into a new file."""
    info = inspect("container", name)
    env = dict(entry.split("=", 1) for entry in info["Config"]["Env"])
    state = info["State"]
    if (
        state["Running"]
        or state["ExitCode"] != 0
        or state["OOMKilled"]
        or env.get("HELIX_BENCHMARK_ROLE") != "writer"
        or env.get("HELIX_DATA_DIR") != DATA_DIR
    ):
        raise ValueError("snapshot needs a cleanly stopped local-disk writer")
    with Path(archive).open("xb") as sink:
        subprocess.run(
            ["docker", "cp", f"{name}:{DATA_DIR}", "-"], stdout=sink, check=True
        )
    return {
        "layout": env["HELIX_INDEX_QUEUE_LAYOUT"],
        "sha256": sha256(archive),
        "bytes": Path(archive).stat().st_size,
    }


def restore(archive, volume, image):
    """Extracts a snapshot into a new, independent named volume."""
    if (
        subprocess.run(
            ["docker", "volume", "inspect", volume], capture_output=True
        ).returncode
        == 0
    ):
        raise ValueError(f"volume {volume} already exists")
    docker("volume", "create", volume)
    holder = f"{volume}-restore"
    docker(
        "create",
        "--name",
        holder,
        "--mount",
        f"type=volume,source={volume},target={DATA_DIR}",
        image,
    )
    try:
        with Path(archive).open("rb") as source:
            subprocess.run(
                ["docker", "cp", "-a", "-", f"{holder}:{Path(DATA_DIR).parent}"],
                stdin=source,
                check=True,
                capture_output=True,
            )
    finally:
        docker("rm", holder)
    return {"volume": volume, "snapshot_sha256": sha256(archive)}


def remove(name=None, volume=None):
    """Removes a container and/or volume, ignoring ones already gone."""
    if name is not None:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    if volume is not None:
        subprocess.run(["docker", "volume", "rm", "-f", volume], capture_output=True)


def sha256(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    begin = commands.add_parser("start")
    begin.add_argument("--name", required=True)
    begin.add_argument("--image", required=True)
    begin.add_argument("--role", choices=("writer", "reader"), required=True)
    begin.add_argument("--layout", choices=("map", "rows"), required=True)
    begin.add_argument("--port", type=int, required=True)
    begin.add_argument("--output", type=Path, required=True)
    begin.add_argument("--volume")
    begin.add_argument("--s3-env", type=Path)
    begin.add_argument("--db-path")
    begin.add_argument("--network")
    begin.add_argument("--bind", default="127.0.0.1", help="0.0.0.0 on EC2 nodes")
    begin.add_argument("--sample-ms", type=int, default=1000)
    end = commands.add_parser("stop")
    end.add_argument("--name", required=True)
    end.add_argument("--output", type=Path, required=True)
    end.add_argument("--timeout-s", type=int, default=120)
    freeze = commands.add_parser("snapshot")
    freeze.add_argument("--name", required=True)
    freeze.add_argument("--output", type=Path, required=True)
    thaw = commands.add_parser("restore")
    thaw.add_argument("--snapshot", type=Path, required=True)
    thaw.add_argument("--volume", required=True)
    thaw.add_argument("--image", required=True)
    drain = commands.add_parser("drain")
    drain.add_argument("--name", required=True)
    drain.add_argument("--timeout-s", type=int, default=600)
    drop = commands.add_parser("remove")
    drop.add_argument("--name")
    drop.add_argument("--volume")
    args = parser.parse_args()
    if args.command == "start":
        result = start(
            args.name,
            args.image,
            args.role,
            args.layout,
            args.port,
            args.output,
            volume=args.volume,
            s3_env=args.s3_env,
            db_path=args.db_path,
            network=args.network,
            bind=args.bind,
            sample_ms=args.sample_ms,
        )
    elif args.command == "stop":
        result = stop(args.name, args.output, timeout_s=args.timeout_s)
    elif args.command == "snapshot":
        result = snapshot(args.name, args.output)
    elif args.command == "restore":
        result = restore(args.snapshot, args.volume, args.image)
    elif args.command == "drain":
        result = wait_drained(args.name, args.timeout_s)
        print(json.dumps({"drained": result}))
        raise SystemExit(0 if result else 1)
    else:
        result = remove(args.name, args.volume)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
