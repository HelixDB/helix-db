#!/usr/bin/env python3
"""Validate asynchronous vector/text index queues through a complete image.

Each scenario starts disposable containers from IMAGE with local-disk storage
unless it names S3, which uses a pinned VersityGW S3 gateway (POSIX backend).

correctness
    Queued writes are visible to strong searches immediately, eventual
    searches converge once the worker publishes, and results stay exact after
    a graceful restart and after SIGKILL during ingest. The graph read back
    after each restart is the oracle, so unacknowledged writes that committed
    before the kill are accounted for.

s3
    The correctness scenario against S3-compatible storage.

benchmark
    Requires an image built with `build.sh --async-index-benchmark`. A writer
    on S3-compatible storage ingests documents; its JSON samples must account for every
    queued operation (committed == acknowledged, every acknowledgement timed
    or censored, each write shape timing at least one; the ledger censors an
    acknowledgement that races ahead of its producer's commit return), report
    merge costs, SlateDB metrics, and signed
    S3 connector attempts. A reader container on the same bucket must converge to the same
    exact search results and reject writes.

member-limit
    Writes during a long initial build accumulate in the hidden generation's
    queue (it is published only after activation). Batches that divide the
    250,000-member limit fill it exactly; the next batch is rejected whole with
    a retryable 429 `index_backpressure`, and succeeds once activation
    publishes the backlog. This exercises the full-image member boundary and
    the per-transaction operand encoding at that boundary.

Usage:
    docker-image/tests/index_queue_contracts.py --image IMAGE [--scenario ...]
"""

import argparse
import json
import math
from pathlib import Path
import random
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Load the checkout's dependency-free DSL without requiring an installed SDK.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdks/python/src/helixdb"))
import dsl  # noqa: E402

MEMBER_LIMIT = 250_000


# Public MinIO images are no longer pullable; VersityGW serves the S3 API
# (including conditional writes) over a POSIX directory.
S3_GATEWAY_IMAGE = ("versity/versitygw:v1.8.0"
                    "@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499")


class S3Store:
    """One disposable S3-compatible gateway and bucket on a private network."""

    def __init__(self, name):
        self.name = name
        self.network = f"{name}-net"
        subprocess.run(["docker", "network", "create", self.network], check=True, capture_output=True)
        subprocess.run(
            [
                "docker", "run", "-d", "--name", name, "--network", self.network,
                "--network-alias", "s3", "-p", "127.0.0.1::9000",
                "--entrypoint", "/bin/sh", S3_GATEWAY_IMAGE, "-c",
                "mkdir -p /data/helix-db && exec versitygw --access helix --secret helix-secret"
                " --port :9000 posix /data",
            ],
            check=True,
            capture_output=True,
        )
        published = subprocess.run(
            ["docker", "port", name, "9000/tcp"], check=True, capture_output=True, text=True,
        ).stdout.split()[0]
        deadline = time.monotonic() + 60
        while True:
            try:
                urlopen(f"http://{published}/", timeout=2)
                return
            except HTTPError:
                return  # Any S3 error response means the gateway is serving.
            except (URLError, ConnectionError, OSError):
                if time.monotonic() > deadline:
                    raise AssertionError("the S3 gateway never started")
                time.sleep(0.2)

    def env(self):
        return {
            "S3_BUCKET": "helix-db",
            "S3_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "helix",
            "AWS_SECRET_ACCESS_KEY": "helix-secret",
            "AWS_ENDPOINT": "http://s3:9000",
            "AWS_ALLOW_HTTP": "true",
        }

    def remove(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        subprocess.run(["docker", "network", "rm", self.network], capture_output=True)


class Server:
    """One disposable container on a named data volume or on S3."""

    def __init__(self, image, port, name, s3=None, env=None, volume=None):
        self.image = image
        self.port = port
        self.name = name
        self.s3 = s3
        self.env = dict(env or {})
        # A shared volume is owned (and removed) by the server that created it.
        self.owns_volume = volume is None and s3 is None
        self.volume = volume if volume is not None else f"{name}-data"
        if self.owns_volume:
            subprocess.run(["docker", "volume", "create", self.volume], check=True, capture_output=True)

    def start(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        command = ["docker", "run", "-d", "--name", self.name, "-p", f"{self.port}:8080"]
        if self.s3 is None:
            command += [
                "-e", "HELIX_DATA_DIR=/var/lib/helix",
                "--mount", f"type=volume,source={self.volume},target=/var/lib/helix",
            ]
        else:
            command += ["--network", self.s3.network]
            for key, value in self.s3.env().items():
                command += ["-e", f"{key}={value}"]
        for key, value in self.env.items():
            command += ["-e", f"{key}={value}"]
        subprocess.run(command + [self.image], check=True, capture_output=True)
        self.wait_ready()

    def wait_ready(self, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                with urlopen(f"http://127.0.0.1:{self.port}/readyz", timeout=5) as response:
                    if response.status == 200:
                        return
            except (URLError, ConnectionError, OSError):
                pass
            time.sleep(0.2)
        raise AssertionError(f"{self.name} did not become ready:\n{self.logs()}")

    def restart(self):
        subprocess.run(["docker", "restart", "-t", "60", self.name], check=True, capture_output=True)
        self.wait_ready()

    def kill(self):
        subprocess.run(["docker", "kill", "--signal", "KILL", self.name], check=True, capture_output=True)
        subprocess.run(["docker", "start", self.name], check=True, capture_output=True)
        self.wait_ready()

    def logs(self):
        return subprocess.run(["docker", "logs", "--tail", "80", self.name], capture_output=True, text=True).stdout

    def samples(self):
        """Every benchmark sample the container printed to stdout, in order."""
        stdout = subprocess.run(["docker", "logs", self.name], capture_output=True, text=True).stdout
        found = []
        for line in stdout.splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict) and value.get("helix_benchmark_sample") == 1:
                found.append(value)
        return found

    def remove(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        if self.owns_volume:
            subprocess.run(["docker", "volume", "rm", "-f", self.volume], capture_output=True)


class Rejected(Exception):
    def __init__(self, status, body):
        super().__init__(f"{status}: {body}")
        self.status = status
        self.body = body


def post(port, request, write):
    headers = {"content-type": "application/json"}
    if write:
        headers["x-helix-await-durable"] = "true"
    http = Request(f"http://127.0.0.1:{port}/v2/query", data=request.to_json_bytes(), headers=headers)
    try:
        with urlopen(http, timeout=600) as response:
            return json.load(response)
    except HTTPError as error:
        body = error.read().decode()
        try:
            body = json.loads(body)
        except ValueError:
            pass
        raise Rejected(error.code, body) from error


def read(port, batch, consistency="strong"):
    request = dsl.QueryRequest.read(batch).with_search_consistency(
        dsl.SearchConsistency(consistency)
    )
    return post(port, request, write=False)


def write(port, batch, retries=100):
    for _ in range(retries):
        try:
            return post(port, dsl.QueryRequest.write(batch), write=True)
        except Rejected as error:
            if error.status == 409:
                continue
            raise
    raise AssertionError("write kept conflicting")


def create_index(port, traversal):
    receipt = write(port, dsl.write_batch().var_as("index", traversal).returning(["index"]))["index"]
    return receipt.get("operation_id")


def wait_operation(port, operation, timeout=1800):
    deadline = time.monotonic() + timeout
    while True:
        status = read(port, dsl.read_batch().var_as(
            "operation", dsl.g().get_index_operation(operation),
        ).returning(["operation"]))["operation"]
        if status["status"] == "succeeded":
            return
        if status["status"] not in ("queued", "running") or time.monotonic() > deadline:
            raise AssertionError(f"index operation {operation}: {status}")
        time.sleep(0.2)


def add_docs(port, label, docs):
    """Adds documents in one write batch and returns their node IDs in order."""
    batch = dsl.write_batch()
    names = []
    for ordinal, doc in enumerate(docs):
        name = f"n{ordinal}"
        names.append(name)
        batch = batch.var_as(name, dsl.g().add_n(label, doc))
    result = write(port, batch.returning(names))
    return [result[name][0]["$id"] for name in names]


def all_docs(port):
    rows = read(port, dsl.read_batch().var_as("docs", dsl.g().n_with_label("Doc").value_map(
        ["$id", "tenant", "embedding", "body"],
    )).returning(["docs"]))["docs"] or []
    return {row["$id"]: row for row in rows}


def vector_hits(port, query, tenant, consistency):
    hits = read(port, dsl.read_batch().var_as("hits", dsl.g().vector_search_nodes(
        "Doc", "embedding", query, 10, tenant,
    )).returning(["hits"]), consistency)["hits"] or []
    return [hit["$id"] for hit in hits]


def text_hits(port, term, consistency):
    hits = read(port, dsl.read_batch().var_as("hits", dsl.g().text_search_nodes(
        "Doc", "body", term, MAX_RESULTS, None,
    )).returning(["hits"]), consistency)["hits"] or []
    return {hit["$id"] for hit in hits}


QUERIES = [[1.5, 2.0, 0.5, 3.0], [9.0, 1.0, 4.0, 0.0], [0.0, 0.0, 0.0, 0.0], [5.5, 5.5, 5.5, 5.5]]
# Group terms keep result sets below the documented 800-result cap, so they are
# compared exactly; larger sets must return exactly 800 matching documents.
TERMS = ["shared", "alpha", "beta", "rewritten", "g0", "g7", "g15"]
MAX_RESULTS = 800


def mismatches(port, docs, consistency):
    found = []
    for tenant in ("a", "b"):
        for query in QUERIES:
            distances = {
                doc_id: sum((x - y) ** 2 for x, y in zip(doc["embedding"], query))
                for doc_id, doc in docs.items()
                if doc["tenant"] == tenant
            }
            expected = sorted(distances.values())[:10]
            actual = vector_hits(port, query, tenant, consistency)
            # Rank by distance, tolerating f32 rounding among near-equal ties.
            ranked = [distances.get(doc_id) for doc_id in actual]
            if len(actual) != len(expected) or any(
                distance is None or abs(distance - want) > 1e-4 * max(1.0, want)
                for distance, want in zip(ranked, expected)
            ):
                found.append(f"vector {tenant} {query} {consistency}: {actual} ranks {ranked} != {expected}")
    for term in TERMS:
        expected = {doc_id for doc_id, doc in docs.items() if term in doc["body"].split()}
        actual = text_hits(port, term, consistency)
        exact = len(expected) <= MAX_RESULTS
        if (actual != expected) if exact else (len(actual) != MAX_RESULTS or not actual <= expected):
            found.append(f"text {term!r} {consistency}: {len(actual)} found, {len(expected)} expected"
                         f"{'' if exact else ' (capped)'}")
    return found


def assert_exact(port, docs, label):
    strong = mismatches(port, docs, "strong")
    assert not strong, f"{label}: strong search differs:\n" + "\n".join(strong)
    deadline = time.monotonic() + 300
    while True:
        eventual = mismatches(port, docs, "eventual")
        if not eventual:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"{label}: eventual search never converged:\n" + "\n".join(eventual))
        time.sleep(0.5)


def doc(rng, ordinal):
    return {
        "tenant": "a" if ordinal % 2 == 0 else "b",
        "embedding": [round(rng.uniform(0, 10), 3) for _ in range(4)],
        "body": f"doc {ordinal} g{ordinal % 16} {'alpha' if ordinal % 3 == 0 else 'beta'} shared",
    }


def correctness(image, port, s3=None):
    server = Server(image, port, f"helix-queue-correctness-{random.randrange(1 << 30)}", s3)
    try:
        server.start()
        rng = random.Random(7)
        wait_operation(port, create_index(port, dsl.g().create_vector_index_nodes(
            "Doc", "embedding", 4, dsl.VectorDistanceMetric.EUCLIDEAN, "tenant",
        )))
        wait_operation(port, create_index(port, dsl.g().create_text_index_nodes("Doc", "body")))

        docs = {}
        for start in range(0, 5_000, 500):
            batch = [doc(rng, ordinal) for ordinal in range(start, start + 500)]
            for doc_id, value in zip(add_docs(port, "Doc", batch), batch):
                docs[doc_id] = value
        ids = sorted(docs)
        for doc_id in rng.sample(ids, 300):
            embedding = [round(rng.uniform(0, 10), 3) for _ in range(4)]
            write(port, dsl.write_batch().var_as("u", dsl.g().n(dsl.NodeRef.id(doc_id)).set_property("embedding", embedding)))
            docs[doc_id]["embedding"] = embedding
        for doc_id in rng.sample(ids, 50):
            tenant = "b" if docs[doc_id]["tenant"] == "a" else "a"
            write(port, dsl.write_batch().var_as("m", dsl.g().n(dsl.NodeRef.id(doc_id)).set_property("tenant", tenant)))
            docs[doc_id]["tenant"] = tenant
        for doc_id in rng.sample(ids, 100):
            if doc_id in docs:
                body = f"rewritten {doc_id} shared"
                write(port, dsl.write_batch().var_as("b", dsl.g().n(dsl.NodeRef.id(doc_id)).set_property("body", body)))
                docs[doc_id]["body"] = body
        for doc_id in rng.sample(ids, 50):
            write(port, dsl.write_batch().var_as("d", dsl.g().n(dsl.NodeRef.id(doc_id)).drop()))
            docs.pop(doc_id, None)
        assert_exact(port, docs, "after ingest")

        server.restart()
        assert all_docs(port).keys() == docs.keys(), "graceful restart preserves the graph"
        assert_exact(port, docs, "after graceful restart")

        stop = threading.Event()
        acknowledged = []

        def ingest():
            ordinal = 10_000
            while not stop.is_set():
                batch = [doc(rng, ordinal + index) for index in range(200)]
                try:
                    acknowledged.extend(add_docs(port, "Doc", batch))
                except (Rejected, URLError, ConnectionError, OSError):
                    return
                ordinal += 200

        worker = threading.Thread(target=ingest)
        worker.start()
        time.sleep(3)
        server.kill()
        stop.set()
        worker.join()
        recovered = all_docs(port)
        missing = set(acknowledged) - recovered.keys()
        assert not missing, f"SIGKILL lost {len(missing)} acknowledged writes"
        truth = {
            doc_id: {"tenant": row["tenant"], "embedding": row["embedding"], "body": row["body"]}
            for doc_id, row in recovered.items()
        }
        assert_exact(port, truth, "after SIGKILL")
        print(f"correctness: {len(truth)} docs exact after restart and SIGKILL "
              f"({len(acknowledged)} acknowledged during the kill window)")
    except Exception:
        print(server.logs(), file=sys.stderr)
        raise
    finally:
        server.remove()


def with_s3(run, *args):
    s3 = S3Store(f"helix-queue-s3-{random.randrange(1 << 30)}")
    try:
        return run(*args, s3)
    finally:
        s3.remove()


def wait_published(server, committed, timeout=600):
    """Waits for a writer sample in which all `committed` enqueued operations
    are acknowledged, so an earlier drained sample cannot be mistaken for it."""
    deadline = time.monotonic() + timeout
    while True:
        samples = server.samples()
        queue = samples[-1]["queue"] if samples else {}
        if queue.get("committed_operations") == committed and queue["pending_operations"] == 0:
            return samples[-1]
        if time.monotonic() > deadline:
            raise AssertionError(f"queue never drained {committed} operations: {samples[-1:]}")
        time.sleep(0.5)


def benchmark(image, port, s3):
    suffix = random.randrange(1 << 30)
    writer = Server(image, port, f"helix-queue-bench-writer-{suffix}", s3,
                    {"HELIX_BENCHMARK_ROLE": "writer", "HELIX_BENCHMARK_SAMPLE_MS": "200"})
    reader = Server(image, port + 1, f"helix-queue-bench-reader-{suffix}", s3,
                    {"HELIX_BENCHMARK_ROLE": "reader", "HELIX_BENCHMARK_SAMPLE_MS": "200"})
    try:
        writer.start()
        rng = random.Random(13)
        wait_operation(port, create_index(port, dsl.g().create_vector_index_nodes(
            "Doc", "embedding", 4, dsl.VectorDistanceMetric.EUCLIDEAN, "tenant",
        )))
        wait_operation(port, create_index(port, dsl.g().create_text_index_nodes("Doc", "body")))
        docs = {}
        for start in range(0, 2_000, 200):
            batch = [doc(rng, ordinal) for ordinal in range(start, start + 200)]
            docs.update(zip(add_docs(port, "Doc", batch), batch))
        # One vector and one text operation per insert. Draining here splits
        # the lag histogram between the two write shapes.
        inserted = wait_published(writer, 2 * 2_000)
        for doc_id in rng.sample(sorted(docs), 100):
            body = f"rewritten {doc_id} shared"
            write(port, dsl.write_batch().var_as("b", dsl.g().n(dsl.NodeRef.id(doc_id)).set_property("body", body)))
            docs[doc_id]["body"] = body
        assert_exact(port, docs, "benchmark writer")
        # One more text operation per rewrite.
        expected = 2 * 2_000 + 100
        last = wait_published(writer, expected)

        queue = last["queue"]
        assert queue["acknowledged_operations"] == expected, queue
        assert queue["published_operations"] == expected, queue
        # No restart and every write succeeded, so the producer observed every
        # enqueue commit.
        assert queue["discovered_operations"] == 0, queue
        # An acknowledgement is then censored only when publication read the
        # enqueue before its producer's commit returned (typically both commits
        # became durable in one WAL flush); every other one is timed exactly.
        censored = queue["censored_acknowledgements"]
        assert last["lag"]["count"] == expected - censored, (last["lag"], queue)
        # How many transactions race depends on flush timing and scheduling,
        # so no fixed censored share is principled. A race censors only the
        # transactions it overlaps, while a producer that stopped observing its
        # commits censors all of them, so both write shapes (ten 400-operation
        # insert transactions, a hundred one-operation rewrites) must time one.
        timed_inserts = inserted["lag"]["count"]
        timed_rewrites = last["lag"]["count"] - timed_inserts
        assert timed_inserts > 0 and timed_rewrites > 0, (inserted["lag"], last["lag"])
        assert last["lag"]["max_micros"] > 0, last["lag"]
        merge = last["merge"]
        assert merge["partial"]["merges"] + merge["resolved"]["merges"] > 0, merge
        assert any(metric["name"].startswith("slatedb.") for metric in last["storage"]), last["storage"]
        s3 = [row for row in last["io"] if row["service"] == "s3"]
        assert any(row["method"] == "PUT" and row["connector_attempts"] > 0 for row in s3), last["io"]
        assert any(row["response_body_bytes_delivered"] > 0 for row in s3), last["io"]
        samples = writer.samples()
        assert [sample["sample"] for sample in samples] == list(range(len(samples))), "samples are contiguous"
        for earlier, later in zip(samples, samples[1:]):
            assert later["queue"]["acknowledged_operations"] >= earlier["queue"]["acknowledged_operations"]

        reader.start()
        assert_exact(port + 1, docs, "benchmark reader")
        try:
            add_docs(port + 1, "Doc", [doc(rng, 99_999)])
        except Rejected as error:
            assert error.status >= 400, error
        else:
            raise AssertionError("the reader accepted a write")
        reader_sample = reader.samples()[-1]
        assert reader_sample["role"] == "reader", reader_sample
        assert reader_sample["queue"]["committed_operations"] == 0, reader_sample
        print(f"benchmark: {expected} operations committed and acknowledged, "
              f"{censored} censored, {timed_inserts} insert and {timed_rewrites} rewrite timed; "
              f"lag p50 >= {lag_quantile(last['lag'], 0.5)} us, "
              f"p99 >= {lag_quantile(last['lag'], 0.99)} us; reader exact")
    except Exception:
        print(writer.logs(), file=sys.stderr)
        print(reader.logs(), file=sys.stderr)
        raise
    finally:
        reader.remove()
        writer.remove()


def lag_quantile(lag, quantile):
    """Lower bound of the histogram bucket holding `quantile` (nearest rank)."""
    rank = max(1, math.ceil(quantile * lag["count"]))
    seen = 0
    for floor, count in sorted((int(floor), count) for floor, count in lag["buckets"].items()):
        seen += count
        if seen >= rank:
            return floor
    return None


def member_limit(image, port, existing, batch_size):
    if MEMBER_LIMIT % batch_size:
        raise SystemExit("batch size must divide the member limit")
    server = Server(image, port, f"helix-queue-limit-{random.randrange(1 << 30)}")
    try:
        server.start()
        rng = random.Random(11)
        started = time.monotonic()
        for start in range(0, existing, 5_000):
            add_docs(port, "Big", [
                {"embedding": [rng.random() for _ in range(4)]}
                for _ in range(min(5_000, existing - start))
            ])
        print(f"member-limit: loaded {existing} existing rows in {time.monotonic() - started:.1f}s")

        build = create_index(port, dsl.g().create_vector_index_nodes(
            "Big", "embedding", 4, dsl.VectorDistanceMetric.EUCLIDEAN,
        ))
        started = time.monotonic()
        accepted = 0
        rejection = None
        batch = None
        while accepted <= MEMBER_LIMIT:
            batch = [{"embedding": [rng.random() for _ in range(4)]} for _ in range(batch_size)]
            try:
                add_docs(port, "Big", batch)
            except Rejected as error:
                rejection = error
                break
            accepted += batch_size
        status = read(port, dsl.read_batch().var_as(
            "operation", dsl.g().get_index_operation(build),
        ).returning(["operation"]))["operation"]
        if rejection is None:
            raise AssertionError(f"no backpressure after {accepted} queued members; build {status['status']}")
        if status["status"] == "succeeded":
            raise AssertionError("the build finished before the boundary; raise --existing")
        body = rejection.body
        assert rejection.status == 429, rejection
        assert body["error"] == "index_backpressure" and body.get("retryable") is True, body
        assert accepted == MEMBER_LIMIT, f"rejected after {accepted} members, expected exactly {MEMBER_LIMIT}"
        print(f"member-limit: {accepted} members accepted in {time.monotonic() - started:.1f}s, "
              f"next batch rejected: {body['msg']}")

        started = time.monotonic()
        # A 100K-row vector build under concurrent writes takes about an hour.
        wait_operation(port, build, timeout=7_200)
        print(f"member-limit: build activated {time.monotonic() - started:.1f}s after the boundary")
        started = time.monotonic()
        deadline = started + 1800
        while True:
            try:
                add_docs(port, "Big", batch)
                break
            except Rejected as error:
                if error.status != 429 or time.monotonic() > deadline:
                    raise
                time.sleep(1)
        print(f"member-limit: rejected batch accepted {time.monotonic() - started:.1f}s after activation")
        hits = read(port, dsl.read_batch().var_as("hits", dsl.g().vector_search_nodes(
            "Big", "embedding", batch[0]["embedding"], 1,
        )).returning(["hits"]))["hits"]
        assert hits and math.isclose(hits[0]["$distance"], 0.0, abs_tol=1e-6), hits
    except Exception:
        print(server.logs(), file=sys.stderr)
        raise
    finally:
        server.remove()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--image", required=True)
    parser.add_argument("--port", type=int, default=18480)
    parser.add_argument("--scenario", choices=["correctness", "s3", "member-limit", "benchmark", "all"],
                        default="all", help="`all` runs every scenario except `benchmark`")
    parser.add_argument("--existing", type=int, default=100_000,
                        help="rows indexed by the long build in the member-limit scenario")
    parser.add_argument("--batch-size", type=int, default=2_000)
    args = parser.parse_args()
    if args.scenario in ("correctness", "all"):
        correctness(args.image, args.port)
    if args.scenario in ("s3", "all"):
        with_s3(correctness, args.image, args.port + 2)
    if args.scenario in ("member-limit", "all"):
        member_limit(args.image, args.port + 1, args.existing, args.batch_size)
    if args.scenario == "benchmark":
        with_s3(benchmark, args.image, args.port + 3)


if __name__ == "__main__":
    main()
