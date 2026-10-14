#!/usr/bin/env python3
"""Fail loudly unless an S3 endpoint enforces the conditional writes SlateDB uses.

SlateDB creates objects with `If-None-Match: *` and replaces them with
`If-Match: <etag>` through object_store's S3 conditional put. A store that
ignores either header lets two writers both succeed and silently loses data,
so every rejected write must return exactly HTTP 412 and leave the object
unchanged, while the matching writes must succeed.

SlateDB's writer and compactor advance the same manifest from separate tasks,
so the store must also settle racing writes atomically: when several writers
create one new key, or replace it with the same ETag, at the same moment,
exactly one may succeed and every other must get HTTP 412. One curl process
sends each race over a connection per racer, opened together. A race that
passes cannot prove a store atomic, but one that fails proves it is not.

Requests are SigV4-signed by curl inside the given container, so the probe
needs no S3 client on the host.
"""

import argparse
import collections
import subprocess
import sys
import uuid

WRONG_ETAG = '"00000000000000000000000000000000"'
RACERS = 8


class ProbeFailure(Exception):
    pass


def transfer(args, url, method, write_out, output="-", body=None, header=None):
    """Returns curl's options for one signed request."""
    options = [
        "--max-time", "30",
        "--aws-sigv4", f"aws:amz:{args.region}:s3",
        "--user", f"{args.access_key}:{args.secret_key}",
        "-X", method, "-o", output, "-w", write_out,
    ]
    if header is not None:
        options += ["-H", header]
    if body is not None:
        options += ["-H", "Content-Type: application/octet-stream", "--data-binary", body]
    return options + [url]


def curl(args, endpoint, label, options):
    """Runs curl with the given options inside the container and returns its output."""
    command = ["docker", "exec", args.container, "curl", "-sS", *options]
    result = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
    if result.returncode != 0:
        raise ProbeFailure(f"{label} via {endpoint} did not complete: {result.stderr.strip()}")
    return result.stdout


def request(args, endpoint, method, key, body=None, header=None):
    """Sends one signed request and returns its status, ETag, and body."""
    url = f"{endpoint}/{args.bucket}/{key}"
    write_out = "\n%{http_code} %header{etag}"
    stdout = curl(
        args, endpoint, f"{method} {key}",
        transfer(args, url, method, write_out, body=body, header=header),
    )
    payload, _, trailer = stdout.rpartition("\n")
    status, _, etag = trailer.partition(" ")
    if not status.isdigit():
        raise ProbeFailure(f"{method} {key} via {endpoint} printed no status: {stdout!r}")
    return int(status), etag, payload


def race(args, endpoint, key, header, name):
    """Sends RACERS conditional PUTs at once and returns each racer's status by body.

    Every body is new to the key: S3 ETags hash the content, so a racer that
    rewrote the current bytes would keep the ETag and let a second If-Match
    succeed legitimately. `--parallel-immediate` makes curl open every
    racer's connection up front instead of waiting to reuse one, so the
    requests reach the store together.
    """
    url = f"{endpoint}/{args.bucket}/{key}"
    bodies = [f"{name}-{racer}" for racer in range(RACERS)]
    options = ["--parallel", "--parallel-immediate", "--parallel-max", str(RACERS)]
    for index, body in enumerate(bodies):
        options += ["--next"] if index else []
        options += transfer(
            args, url, "PUT", f"{body} %{{http_code}}\n", output="/dev/null",
            body=body, header=header,
        )
    label = f"racing PUT {key}"
    stdout = curl(args, endpoint, label, options)
    statuses = {}
    for line in stdout.splitlines():
        body, _, status = line.partition(" ")
        if body not in bodies or body in statuses or not status.isdigit():
            raise ProbeFailure(f"{label} via {endpoint} printed {line!r}: {stdout!r}")
        statuses[body] = int(status)
    if len(statuses) != RACERS:
        raise ProbeFailure(
            f"{label} via {endpoint} reported {len(statuses)} of {RACERS} racers: {stdout!r}"
        )
    return statuses


def expect(label, status, expected, payload=""):
    print(f"  {label}: HTTP {status} (expected {expected})")
    if status != expected:
        detail = f": {payload.strip()}" if payload.strip() else ""
        raise ProbeFailure(f"{label} returned HTTP {status}, expected {expected}{detail}")


def expect_body(label, payload, expected):
    print(f"  {label}: {payload!r} (expected {expected!r})")
    if payload != expected:
        raise ProbeFailure(f"{label} read {payload!r}, expected {expected!r}")


def expect_one_winner(label, statuses):
    """Requires exactly one racer to succeed and every other to get HTTP 412."""
    counts = collections.Counter(statuses.values())
    summary = ", ".join(f"{count} x HTTP {status}" for status, count in sorted(counts.items()))
    print(f"  {label}: {summary} (expected 1 x HTTP 200, {RACERS - 1} x HTTP 412)")
    if counts != {200: 1, 412: RACERS - 1}:
        raise ProbeFailure(
            f"{label} returned {summary}, expected exactly one HTTP 200 and HTTP 412 for the rest"
        )
    return next(body for body, status in statuses.items() if status == 200)


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

    race_key = f"{args.prefix}/{uuid.uuid4().hex}"
    winner = expect_one_winner(
        f"(c) {RACERS} concurrent creates with If-None-Match: * on a new key",
        race(args, endpoint, race_key, "If-None-Match: *", "create"),
    )
    status, race_etag, payload = request(args, endpoint, "GET", race_key)
    expect("read after concurrent creates", status, 200, payload)
    expect_body("object after concurrent creates", payload, winner)
    if not race_etag:
        raise ProbeFailure("read after concurrent creates returned no ETag")

    winner = expect_one_winner(
        f"(d) {RACERS} concurrent replaces with If-Match: <current etag>",
        race(args, endpoint, race_key, f"If-Match: {race_etag}", "replace"),
    )
    status, _, payload = request(args, endpoint, "GET", race_key)
    expect("read after concurrent replaces", status, 200, payload)
    expect_body("object after concurrent replaces", payload, winner)

    status, _, payload = request(args, endpoint, "DELETE", race_key)
    expect("delete race object", status, 204, payload)


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
