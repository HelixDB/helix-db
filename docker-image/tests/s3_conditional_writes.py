#!/usr/bin/env python3
"""Fail loudly unless an S3 endpoint enforces the conditional writes SlateDB uses.

SlateDB creates objects with `If-None-Match: *` and replaces them with
`If-Match: <etag>` through object_store's S3 conditional put. A store that
ignores either header lets two writers both succeed and silently loses data,
so every rejected write must return exactly HTTP 412 and leave the object
unchanged, while the matching writes must succeed.

Requests are SigV4-signed by curl inside the given container, so the probe
needs no S3 client on the host.
"""

import argparse
import subprocess
import sys
import uuid

WRONG_ETAG = '"00000000000000000000000000000000"'


class ProbeFailure(Exception):
    pass


def request(args, endpoint, method, key, body=None, header=None):
    """Sends one signed request and returns its status, ETag, and body."""
    command = [
        "docker", "exec", args.container, "curl", "-sS", "--max-time", "30",
        "--aws-sigv4", f"aws:amz:{args.region}:s3",
        "--user", f"{args.access_key}:{args.secret_key}",
        "-X", method, "-o", "-", "-w", "\n%{http_code} %header{etag}",
    ]
    if header is not None:
        command += ["-H", header]
    if body is not None:
        command += ["-H", "Content-Type: application/octet-stream", "--data-binary", body]
    command.append(f"{endpoint}/{args.bucket}/{key}")
    result = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
    if result.returncode != 0:
        raise ProbeFailure(
            f"{method} {key} via {endpoint} did not complete: {result.stderr.strip()}"
        )
    payload, _, trailer = result.stdout.rpartition("\n")
    status, _, etag = trailer.partition(" ")
    if not status.isdigit():
        raise ProbeFailure(f"{method} {key} via {endpoint} printed no status: {result.stdout!r}")
    return int(status), etag, payload


def expect(label, status, expected, payload=""):
    print(f"  {label}: HTTP {status} (expected {expected})")
    if status != expected:
        detail = f": {payload.strip()}" if payload.strip() else ""
        raise ProbeFailure(f"{label} returned HTTP {status}, expected {expected}{detail}")


def expect_body(label, payload, expected):
    print(f"  {label}: {payload!r} (expected {expected!r})")
    if payload != expected:
        raise ProbeFailure(f"{label} read {payload!r}, expected {expected!r}")


def probe(args, endpoint):
    key = f"{args.prefix}/{uuid.uuid4().hex}"
    print(f"Conditional write probe via {endpoint} on {args.bucket}/{key}")

    status, created_etag, payload = request(
        args, endpoint, "PUT", key, "created", "If-None-Match: *"
    )
    expect("create with If-None-Match: * on a new key", status, 200, payload)
    if not created_etag:
        raise ProbeFailure("create with If-None-Match: * returned no ETag")

    status, _, payload = request(args, endpoint, "PUT", key, "duplicate", "If-None-Match: *")
    expect("(a) create with If-None-Match: * on an existing key", status, 412, payload)

    status, _, payload = request(args, endpoint, "PUT", key, "stale", f"If-Match: {WRONG_ETAG}")
    expect("(b) replace with If-Match: <wrong etag>", status, 412, payload)

    status, _, payload = request(args, endpoint, "GET", key)
    expect("read after rejected writes", status, 200, payload)
    expect_body("object after rejected writes", payload, "created")

    status, _, payload = request(
        args, endpoint, "PUT", key, "replaced", f"If-Match: {created_etag}"
    )
    expect("replace with If-Match: <current etag>", status, 200, payload)

    status, _, payload = request(args, endpoint, "GET", key)
    expect("read after matching replace", status, 200, payload)
    expect_body("object after matching replace", payload, "replaced")

    status, _, payload = request(args, endpoint, "DELETE", key)
    expect("delete probe object", status, 204, payload)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--container", required=True, help="container that runs curl")
    parser.add_argument(
        "--endpoint", action="append", required=True, help="S3 endpoint URL; repeatable"
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--secret-key", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--prefix", default="conditional-write-probe")
    args = parser.parse_args(argv)
    try:
        for endpoint in args.endpoint:
            probe(args, endpoint)
    except ProbeFailure as failure:
        print(f"CONDITIONAL WRITE PROBE FAILED: {failure}", file=sys.stderr)
        print(
            "SlateDB needs S3 conditional writes; this object store would silently lose data.",
            file=sys.stderr,
        )
        return 1
    print(f"Conditional write probe passed on {len(args.endpoint)} endpoint(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
