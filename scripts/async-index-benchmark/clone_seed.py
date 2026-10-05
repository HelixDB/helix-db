#!/usr/bin/env python3
"""Copy a closed, exclusively owned benchmark database into an empty S3 prefix.

Run on the seed writer host. No other process may reopen the source or use the
destination during this operation. This is benchmark setup, not a database
checkpoint API or a live-copy mechanism. Every copied body is SHA-256 verified.
The copy keeps the source's queue layout: start every server on the destination
with the `layout` recorded in `clone.json`.
"""

import argparse
import concurrent.futures
import hashlib
import itertools
import json
import re
import subprocess
from pathlib import Path


def inventory(client, bucket, prefix):
    objects = {}
    token = None
    seen = set()
    while True:
        page = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix + "/",
            **({"ContinuationToken": token} if token else {}),
        )
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.startswith(prefix + "/") or key in objects:
                raise ValueError("invalid or duplicate inventory key")
            if type(item["Size"]) is not int or item["Size"] < 0 or not item["ETag"]:
                raise ValueError("invalid inventory size or ETag")
            objects[key] = {"etag": item["ETag"], "size": item["Size"]}
        if not page.get("IsTruncated", False):
            return objects
        token = page["NextContinuationToken"]
        if not token or token in seen:
            raise ValueError("invalid inventory continuation")
        seen.add(token)


def body_hash(client, bucket, key, etag, size):
    response = client.get_object(Bucket=bucket, Key=key, IfMatch=etag)
    body = response["Body"]
    digest = hashlib.sha256()
    length = 0
    try:
        for chunk in body.iter_chunks(chunk_size=1024 * 1024):
            digest.update(chunk)
            length += len(chunk)
    finally:
        body.close()
    if length != size or response["ContentLength"] != size:
        raise ValueError("object body length differs from inventory")
    return digest.hexdigest()


def clone(client, bucket, source, destination, container, output, *, workers=4):
    for prefix in (source, destination):
        if (
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9/_-]*", prefix)
            or prefix.endswith("/")
            or "//" in prefix
        ):
            raise ValueError("use canonical benchmark prefixes without trailing slash")
    if (
        source == destination
        or source.startswith(destination + "/")
        or destination.startswith(source + "/")
    ):
        raise ValueError("source and destination must not overlap")
    if type(workers) is not int or not 1 <= workers <= 32:
        raise ValueError("copy workers must be from 1 through 32")
    inspect = json.loads(
        subprocess.check_output(["docker", "inspect", container], text=True)
    )[0]
    state = inspect["State"]
    env = dict(entry.split("=", 1) for entry in inspect["Config"]["Env"])
    if (
        state["Running"]
        or state["Status"] != "exited"
        or state["ExitCode"] != 0
        or state["OOMKilled"]
    ):
        raise ValueError("source writer must have stopped cleanly")
    if (
        env.get("DB_PATH") != source
        or env.get("S3_BUCKET") != bucket
        or env.get("HELIX_BENCHMARK_ROLE") != "writer"
        or env.get("HELIX_INDEX_QUEUE_LAYOUT") not in ("map", "rows")
    ):
        raise ValueError("closed container does not own this source prefix")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    report = {
        "schema": 1,
        "status": "incomplete",
        "bucket": bucket,
        "source": source,
        "destination": destination,
        "workers": workers,
        "layout": env["HELIX_INDEX_QUEUE_LAYOUT"],
        "container_id": inspect["Id"],
        "image_id": inspect["Image"],
        "source_finished_at": state["FinishedAt"],
        "release_or_performance_acceptance": False,
    }
    marker = output / "clone.json"
    marker.write_text(json.dumps(report, indent=2) + "\n")
    before = inventory(client, bucket, source)
    (output / "source-inventory.json").write_text(json.dumps(before, indent=2) + "\n")
    if not before or any(item["size"] > 5 * 1024**3 for item in before.values()):
        raise ValueError("source is empty or contains objects requiring multipart copy")
    if inventory(client, bucket, destination):
        raise ValueError("destination prefix is not empty")

    def copy(key):
        item = before[key]
        suffix = key[len(source) + 1 :]
        target = destination + "/" + suffix
        original_hash = body_hash(client, bucket, key, item["etag"], item["size"])
        response = client.copy_object(
            Bucket=bucket,
            Key=target,
            CopySource={"Bucket": bucket, "Key": key},
            CopySourceIfMatch=item["etag"],
        )
        etag = response["CopyObjectResult"]["ETag"]
        copied_hash = body_hash(client, bucket, target, etag, item["size"])
        if copied_hash != original_hash:
            raise ValueError("copied object content differs from source")
        return {
            "suffix": suffix,
            "bytes": item["size"],
            "sha256": copied_hash,
            "destination_etag": etag,
        }

    copied = {}
    with (
        (output / "objects.jsonl").open("x") as sink,
        concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool,
    ):
        # Bound submitted futures as well as active S3 requests.
        for batch in itertools.batched(sorted(before), workers):
            for item in pool.map(copy, batch):
                sink.write(json.dumps(item, sort_keys=True) + "\n")
                sink.flush()
                copied[destination + "/" + item["suffix"]] = {
                    "size": item["bytes"],
                    "etag": item["destination_etag"],
                }
    if inventory(client, bucket, source) != before:
        raise ValueError("source changed during copy")
    if inventory(client, bucket, destination) != copied:
        raise ValueError("destination inventory differs from verified copy")
    # Verify that the known source container was not restarted during the copy.
    final = json.loads(
        subprocess.check_output(["docker", "inspect", container], text=True)
    )[0]
    if final["Id"] != inspect["Id"] or final["State"] != state:
        raise ValueError("source container changed during copy")
    with (output / "objects.jsonl").open("rb") as stream:
        manifest_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    report.update(
        status="copied_and_verified",
        objects=len(copied),
        bytes=sum(item["size"] for item in copied.values()),
        objects_sha256=manifest_hash,
        reopen_verified=False,
    )
    marker.write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--source", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    # Use the botocore pinned with the benchmark host's AWS CLI virtualenv.
    try:
        from botocore import session as aws_session
    except ImportError:
        from awscli.botocore import session as aws_session

    client = aws_session.get_session().create_client("s3", region_name=args.region)
    try:
        print(
            json.dumps(
                clone(
                    client,
                    args.bucket,
                    args.source,
                    args.destination,
                    args.container,
                    args.output,
                    workers=args.workers,
                ),
                indent=2,
            )
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
