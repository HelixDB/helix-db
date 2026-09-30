#!/usr/bin/env python3
"""Replay a fixed HTTP trace without turning overload into a closed-loop test.

This transport records request outcomes, not indexed entity throughput; server
samples supply publication counts. No retries are performed, including after a
lost write response. Outcomes (see `OUTCOMES`) keep definite rejections,
backpressure (429), conflicts (409), client overload, socket timeouts and
uncertain writes apart. The correlation header is diagnostic only.
"""

import argparse
import hashlib
import http.client
import json
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from urllib.parse import urlsplit


class Kind(str, Enum):
    WRITE = "write"
    STRONG = "strong"
    EVENTUAL = "eventual"


# acknowledged/search_ok: HTTP 200. conflict: 409 transaction_conflict.
# backpressure: 429 index_backpressure. rejected: any other 4xx, a definite
# refusal. unknown_write: 5xx, malformed reply or transport failure after the
# request may have reached the writer, so the commit is uncertain. timeout:
# socket inactivity limit reached (for writes, also uncertain). http_error /
# request_error: read-side equivalents. client_overload: no free client slot,
# the offer was never sent.
OUTCOMES = {
    Kind.WRITE: (
        "acknowledged",
        "conflict",
        "backpressure",
        "rejected",
        "unknown_write",
        "timeout",
        "client_overload",
    ),
    Kind.STRONG: (
        "search_ok",
        "conflict",
        "backpressure",
        "http_error",
        "request_error",
        "timeout",
        "client_overload",
    ),
}
OUTCOMES[Kind.EVENTUAL] = OUTCOMES[Kind.STRONG]


def reject_constant(value):
    raise ValueError(f"non-JSON numeric constant: {value}")


def integer(value, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError("expected a nonnegative integer")
    return value


@dataclass(frozen=True)
class Offer:
    identity: int
    at_ns: int
    kind: Kind
    body: bytes

    @classmethod
    def decode(cls, line):
        row = json.loads(line, parse_constant=reject_constant)
        if not isinstance(row, dict) or set(row) != {"id", "at_ns", "kind", "payload"}:
            raise ValueError("invalid trace fields")
        kind = Kind(row["kind"])
        payload = row["payload"]
        request_type = "write" if kind is Kind.WRITE else "read"
        consistency = "eventual" if kind is Kind.EVENTUAL else "strong"
        if not isinstance(payload, dict) or payload.get("request_type") != request_type:
            raise ValueError("trace kind disagrees with request_type")
        if payload.get("search_consistency", consistency) != consistency:
            raise ValueError("trace kind disagrees with search_consistency")
        body = json.dumps(
            payload | {"search_consistency": consistency},
            separators=(",", ":"),
            allow_nan=False,
            ensure_ascii=False,
        ).encode()
        return cls(integer(row["id"]), integer(row["at_ns"]), kind, body)


def offers(source, digest, max_line_bytes):
    previous_ns = 0
    identity = 0
    while line := source.readline(max_line_bytes + 1):
        if len(line) > max_line_bytes:
            raise ValueError("trace line exceeds configured limit")
        digest.update(line)
        offer = Offer.decode(line)
        if offer.identity != identity or offer.at_ns < previous_ns:
            raise ValueError("trace IDs must be contiguous and offsets must be ordered")
        identity += 1
        previous_ns = offer.at_ns
        yield offer


class Ledger:
    def __init__(self, output):
        self.output = output
        self.lock = threading.Lock()
        self.counts = Counter()

    def write(self, record):
        encoded = json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n"
        with self.lock:
            self.output.write(encoded)
            if record["event"] == "result":
                self.counts[record["kind"] + ":" + record["outcome"]] += 1


class Failure:
    def __init__(self):
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.error = None

    def set(self, error):
        with self.lock:
            if self.error is None:
                self.error = error
            self.stop.set()


class Pool:
    def __init__(
        self, endpoint, workers, timeout, response_limit, ledger, failure, start_ns
    ):
        self.endpoint = endpoint
        self.timeout = timeout
        self.response_limit = response_limit
        self.ledger = ledger
        self.failure = failure
        self.start_ns = start_ns
        self.executor = ThreadPoolExecutor(max_workers=workers)
        # Includes active and queued work: there is no unbounded executor queue.
        self.slots = threading.BoundedSemaphore(workers)
        self.lock = threading.Lock()
        self.connections = {}
        self.active = set()

    def submit(self, offer, request_id):
        if not self.slots.acquire(blocking=False):
            self.ledger.write(
                {
                    "event": "result",
                    "id": offer.identity,
                    "kind": offer.kind.value,
                    "request_id": request_id,
                    "at_ns": offer.at_ns,
                    "finished_ns": time.monotonic_ns() - self.start_ns,
                    "outcome": "client_overload",
                }
            )
            return
        try:
            future = self.executor.submit(self.request, offer, request_id)
        except BaseException:
            self.slots.release()
            raise
        with self.lock:
            self.active.add(future)
        future.add_done_callback(self.completed)

    def completed(self, future):
        try:
            future.result()
        except BaseException as error:  # noqa: BLE001 - propagate every failed worker to the coordinator
            self.failure.set(error)
        finally:
            with self.lock:
                self.active.remove(future)
            self.slots.release()

    def request(self, offer, request_id):
        thread = threading.get_ident()
        with self.lock:
            connection = self.connections.get(thread)
            if connection is None:
                cls = (
                    http.client.HTTPSConnection
                    if self.endpoint.scheme == "https"
                    else http.client.HTTPConnection
                )
                connection = cls(
                    self.endpoint.hostname, self.endpoint.port, timeout=self.timeout
                )
                self.connections[thread] = connection
        headers = {
            "content-type": "application/json",
            "x-helix-benchmark-request-id": request_id,
        }
        if offer.kind is Kind.WRITE:
            headers["x-helix-await-durable"] = "true"
        record = {
            "event": "result",
            "id": offer.identity,
            "kind": offer.kind.value,
            "request_id": request_id,
            "at_ns": offer.at_ns,
            "body_sha256": hashlib.sha256(offer.body).hexdigest(),
            "body_bytes": len(offer.body),
            "status": None,
        }
        started = time.monotonic_ns()
        record["started_ns"] = started - self.start_ns
        try:
            connection.request("POST", "/v2/query", body=offer.body, headers=headers)
            response = connection.getresponse()
            record["status"] = response.status
            body = response.read(self.response_limit + 1)
            finished = time.monotonic_ns()
            record["response_bytes"] = len(body)
            if len(body) > self.response_limit:
                raise ValueError("response exceeds configured limit")
            record["response_sha256"] = hashlib.sha256(body).hexdigest()
            payload = json.loads(body, parse_constant=reject_constant)
            if not isinstance(payload, dict):
                raise ValueError("query response is not an object")  # noqa: TRY004 - invalid wire data
            if response.status == 200:
                record["outcome"] = (
                    "acknowledged" if offer.kind is Kind.WRITE else "search_ok"
                )
            elif (
                response.status == 409
                and payload.get("error") == "transaction_conflict"
                and ("retryable" not in payload or payload["retryable"] is True)
            ):
                record["outcome"] = "conflict"
            elif (
                response.status == 429
                and payload.get("error") == "index_backpressure"
                and payload.get("retryable") is True
            ):
                record["outcome"] = "backpressure"
            elif offer.kind is not Kind.WRITE:
                record["outcome"] = "http_error"
            else:
                record["outcome"] = (
                    "rejected" if 400 <= response.status < 500 else "unknown_write"
                )
                record["error"] = str(payload.get("error"))[:64]
        except (OSError, http.client.HTTPException, ValueError) as error:
            finished = time.monotonic_ns()
            # Do not log response content or exception messages containing data.
            record["error_type"] = type(error).__name__
            if isinstance(error, TimeoutError):
                record["outcome"] = "timeout"
            else:
                record["outcome"] = (
                    "unknown_write" if offer.kind is Kind.WRITE else "request_error"
                )
            connection.close()
            with self.lock:
                del self.connections[thread]
        record["finished_ns"] = finished - self.start_ns
        record["latency_ns"] = finished - started
        record["scheduled_latency_ns"] = record["finished_ns"] - offer.at_ns
        record["dispatch_lag_ns"] = record["started_ns"] - offer.at_ns
        self.ledger.write(record)

    def close(self):
        self.executor.shutdown(wait=True)
        for connection in self.connections.values():
            connection.close()


def run(
    trace,
    output,
    endpoints,
    workers,
    *,
    run_id=None,
    start_delay_s=1,
    start_unix_ns=None,
    timeout_s=120,
    max_response_bytes=16 * 1024 * 1024,
    max_line_bytes=16 * 1024 * 1024,
):
    """Validate before network I/O; retain all outcomes and reject damaged evidence.

    Pools are separate for writes, strong reads and eventual reads. Socket timeout
    is an inactivity timeout, not an end-to-end deadline. HTTP failures do not
    trigger retries. Unexpected local failures leave an incomplete ledger.
    """
    parsed = {}
    if set(endpoints) != set(Kind) or set(workers) != set(Kind):
        raise ValueError("endpoints and worker limits must specify every request kind")
    for kind in Kind:
        endpoint = urlsplit(endpoints[kind])
        if (
            endpoint.scheme not in ("http", "https")
            or not endpoint.hostname
            or endpoint.username is not None
            or endpoint.password is not None
            or endpoint.query
            or endpoint.fragment
            or endpoint.path not in ("", "/")
        ):
            raise ValueError("endpoint must be an HTTP(S) base URL without credentials")
        _ = endpoint.port  # Validate ports before starting any requests.
        parsed[kind] = endpoint
        integer(workers[kind], 1)
    if start_unix_ns is not None:
        integer(start_unix_ns, 1)
    integer(max_line_bytes, 1)
    integer(max_response_bytes, 1)
    if not (0 <= start_delay_s < 3600 and 0 < timeout_s <= 3600):
        raise ValueError("invalid delay or socket timeout")
    identity = uuid.uuid4() if run_id is None else uuid.UUID(run_id)
    with Path(trace).open("rb") as source:
        expected_digest = hashlib.sha256()
        planned = sum(1 for _ in offers(source, expected_digest, max_line_bytes))
        if not planned:
            raise ValueError("empty trace")
        source.seek(0)
        Path(output).mkdir(parents=True, exist_ok=False)
        with (Path(output) / "requests.jsonl").open("x", buffering=1) as sink:
            ledger = Ledger(sink)
            failure = Failure()
            clock_before = time.monotonic_ns()
            clock_unix = time.time_ns()
            clock_after = time.monotonic_ns()
            clock_mono = (clock_before + clock_after) // 2
            if (
                start_unix_ns is not None
                and not 0 < start_unix_ns - clock_unix <= 3_600_000_000_000
            ):
                raise ValueError(
                    "scheduled start must remain in the next hour after trace validation"
                )
            delay_ns = (
                int(start_delay_s * 1_000_000_000)
                if start_unix_ns is None
                else start_unix_ns - clock_unix
            )
            start_ns = clock_mono + delay_ns
            ledger.write(
                {
                    "event": "start",
                    "schema": 1,
                    "run_id": str(identity),
                    "trace_sha256": expected_digest.hexdigest(),
                    "planned": planned,
                    "clock_monotonic_ns": clock_mono,
                    "clock_monotonic_before_ns": clock_before,
                    "clock_monotonic_after_ns": clock_after,
                    "scheduled_start_unix_ns": start_unix_ns,
                    "clock_unix_ns": clock_unix,
                    "start_monotonic_ns": start_ns,
                    "endpoints": endpoints,
                    "workers": workers,
                    "socket_timeout_s": timeout_s,
                    "max_response_bytes": max_response_bytes,
                    "max_line_bytes": max_line_bytes,
                    "runner_sha256": hashlib.sha256(
                        Path(__file__).read_bytes()
                    ).hexdigest(),
                }
            )
            pools = {
                kind: Pool(
                    parsed[kind],
                    workers[kind],
                    timeout_s,
                    max_response_bytes,
                    ledger,
                    failure,
                    start_ns,
                )
                for kind in Kind
            }
            observed_digest = hashlib.sha256()
            offered = 0
            try:
                for offer in offers(source, observed_digest, max_line_bytes):
                    while not failure.stop.is_set():
                        remaining = start_ns + offer.at_ns - time.monotonic_ns()
                        if remaining <= 0:
                            break
                        failure.stop.wait(min(remaining / 1_000_000_000, 1))
                    if failure.stop.is_set():
                        break
                    request_id = str(uuid.uuid5(identity, str(offer.identity)))
                    pools[offer.kind].submit(offer, request_id)
                    offered += 1
            finally:
                for pool in pools.values():
                    pool.close()
            if failure.error is not None:
                raise RuntimeError(
                    "load worker failed; ledger is incomplete"
                ) from failure.error
            if (
                offered != planned
                or observed_digest.digest() != expected_digest.digest()
            ):
                raise ValueError("trace changed during replay")
            if sum(ledger.counts.values()) != planned:
                raise ValueError("not every offer has an outcome")
            end_clock_before = time.monotonic_ns()
            end_clock_unix = time.time_ns()
            end_clock_after = time.monotonic_ns()
            footer = {
                "event": "end",
                "clock_monotonic_before_ns": end_clock_before,
                "clock_unix_ns": end_clock_unix,
                "clock_monotonic_after_ns": end_clock_after,
                "run_id": str(identity),
                "offered": offered,
                "counts": dict(ledger.counts),
                "client_retries": 0,
                "finished_ns": time.monotonic_ns() - start_ns,
            }
            ledger.write(footer)
            return footer


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--writer", required=True)
    parser.add_argument("--reader", help="search endpoint; defaults to the writer")
    parser.add_argument("--strong", help="strong-search endpoint override")
    parser.add_argument("--eventual", help="eventual-search endpoint override")
    parser.add_argument("--write-workers", type=int, default=64)
    parser.add_argument("--strong-workers", type=int, default=64)
    parser.add_argument("--eventual-workers", type=int, default=64)
    parser.add_argument("--socket-timeout", type=float, default=120)
    parser.add_argument("--start-unix-ns", type=int)
    args = parser.parse_args()
    print(
        json.dumps(
            run(
                args.trace,
                args.output,
                {
                    Kind.WRITE: args.writer,
                    Kind.STRONG: args.strong or args.reader or args.writer,
                    Kind.EVENTUAL: args.eventual or args.reader or args.writer,
                },
                {
                    Kind.WRITE: args.write_workers,
                    Kind.STRONG: args.strong_workers,
                    Kind.EVENTUAL: args.eventual_workers,
                },
                timeout_s=args.socket_timeout,
                start_unix_ns=args.start_unix_ns,
            )
        )
    )
