#!/usr/bin/env python3
"""Exercise idle vector hydration against the disposable Compose MinIO stack."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

# Load the checkout's dependency-free DSL without requiring an installed SDK.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdks/python/src/helixdb"))
import dsl


def query(port, batch):
    request = Request(
        f"http://127.0.0.1:{port}/v2/query",
        data=batch.to_query_bytes(),
        headers={"content-type": "application/json", **({"x-helix-await-durable": "true"} if isinstance(batch, dsl.WriteBatch) else {})},
    )
    try:
        with urlopen(request, timeout=120) as response:
            return json.load(response)
    except HTTPError as error:
        raise AssertionError(error.read().decode()) from error


def run(args):
    created = query(args.port, dsl.write_batch().var_as(
        "index", dsl.g().create_vector_index_nodes(
            "IdleVectorFixture", "embedding", 3, dsl.VectorDistanceMetric.EUCLIDEAN,
        ),
    ).returning(["index"]))
    receipt = created["index"]
    if receipt["kind"] in ("accepted", "existing_operation"):
        deadline = time.monotonic() + 120
        while True:
            status = query(args.port, dsl.read_batch().var_as(
                "operation", dsl.g().get_index_operation(receipt["operation_id"]),
            ).returning(["operation"]))["operation"]["status"]
            if status == "succeeded":
                break
            if status in ("failed", "blocked", "aborted") or time.monotonic() > deadline:
                raise AssertionError(f"index creation: {status}")
            time.sleep(0.1)

    # Cross the default memtable threshold so reopening hydrates actual SSTs.
    for start in range(0, 512, 16):
        batch = dsl.write_batch()
        for index in range(start, start + 16):
            batch = batch.var_as(f"node{index}", dsl.g().add_n("IdleVectorFixture", {
                "ordinal": index, "embedding": [float(index), 1.0, 0.0],
                "padding": "x" * (256 * 1024),
            }))
        query(args.port, batch)

    search = dsl.read_batch().var_as("hits", dsl.g().vector_search_nodes(
        "IdleVectorFixture", "embedding", [0.0, 1.0, 0.0], 1,
    ).value_map(["ordinal"])).returning(["hits"])
    expected = {"hits": [{"ordinal": 0}]}
    assert query(args.port, search) == expected

    container = subprocess.check_output([
        "docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={args.project}",
        "--filter", "label=com.docker.compose.service=helix",
    ], text=True).strip()
    assert container, "disposable Compose server must exist"
    trace_name = f"{args.project}-vector-trace"
    with tempfile.TemporaryFile() as trace_file:
        trace = subprocess.Popen([
            "docker", "run", "--rm", "--name", trace_name,
            "--network", f"{args.project}_default", "--entrypoint", "/bin/sh", args.mc_image,
            "-c", "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && "
            "mc admin trace --json --verbose --all local",
        ], stdout=trace_file, stderr=subprocess.STDOUT)
        try:
            time.sleep(2)
            assert trace.poll() is None, "MinIO trace must stay connected"
            subprocess.run(["docker", "restart", container], check=True, stdout=subprocess.DEVNULL)
            deadline = time.monotonic() + 120
            while True:
                try:
                    with urlopen(f"http://127.0.0.1:{args.port}/readyz", timeout=2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.2)
            # Measure the SST ranges used by catalog/operation reads separately.
            # Never run vector search here: it could conceal redundant hydration.
            time.sleep(12)
            metadata_start = os.fstat(trace_file.fileno()).st_size
            query(args.port, dsl.read_batch().var_as(
                "operation", dsl.g().get_index_operation(receipt["operation_id"]),
            ).returning(["operation"]))
            time.sleep(0.25)  # Allow the metadata trace records to reach stdout.
            metadata_end = os.fstat(trace_file.fileno()).st_size
            time.sleep(17)  # At least three five-second refresh intervals, with no clients.
            assert trace.poll() is None, "trace exited during the idle interval"
        finally:
            subprocess.run(["docker", "rm", "-f", trace_name], check=False, stdout=subprocess.DEVNULL)
            trace.wait(timeout=15)
        # Read only after the writer exits. Seeking a shared stdout descriptor
        # while mc runs can overwrite the trace or split a UTF-8/JSON record.
        trace_file.seek(0)
        captured = trace_file.read()
        warm_end = captured.rfind(b"\n", 0, metadata_start) + 1
        idle_start = captured.rfind(b"\n", 0, metadata_end) + 1
        complete_end = captured.rfind(b"\n") + 1

        def sst_ranges(raw):
            ranges = []
            for line in raw.decode().splitlines():
                # Docker/mc may print startup notices before the JSON stream.
                if not line.startswith("{"):
                    continue
                event = json.loads(line)
                if event["api"] == "s3.GetObject" and "/compacted/" in event["path"]:
                    ranges.append((event["path"], event["request"]["headers"]["Range"]))
            return ranges

        warm = set(sst_ranges(captured[:warm_end]))
        metadata = set(sst_ranges(captured[warm_end:idle_start]))
        idle = sst_ranges(captured[idle_start:complete_end])
        assert warm - metadata, "warm phase must read SST ranges outside the catalog"
        unexpected = set(idle) - metadata
        assert not unexpected, f"idle refresh fetched non-catalog SST ranges: {unexpected}"
        print(f"Idle trace: {len(idle)} catalog SST GETs, zero vector-data SST GETs")

    assert query(args.port, search) == expected
    query(args.port, dsl.write_batch().var_as("node", dsl.g().add_n("IdleVectorFixture", {
        "ordinal": 512, "embedding": [-1.0, 1.0, 0.0],
    })))
    updated_search = dsl.read_batch().var_as("hits", dsl.g().vector_search_nodes(
        "IdleVectorFixture", "embedding", [-1.0, 1.0, 0.0], 1,
    ).value_map(["ordinal"])).returning(["hits"])
    assert query(args.port, updated_search) == {"hits": [{"ordinal": 512}]}
    time.sleep(6)
    assert query(args.port, updated_search) == {"hits": [{"ordinal": 512}]}
    print("Vector idle regression passed: zero vector-data SST GETs across three refresh intervals; searches pass before and after a write")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--mc-image", required=True)
    run(parser.parse_args())
