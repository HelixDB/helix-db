#!/usr/bin/env python3
"""Seed a disposable benchmark database before measurement; never replay unknown writes.

Graph rows are inserted before index creation. All builds must finish before the
seed is published as complete. Failures leave evidence, not a resumable success.
Entity IDs are allocated sequentially by a fresh database, so seeding the same
fixture in the same order yields the same `entity-ids.u64le` for every queue
layout; traces depend on that file and runs verify its hash.
"""

import argparse
import hashlib
import http.client
import json
import re
import struct
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from dataset import Dataset, fixture

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdks/python/src/helixdb"))
import dsl  # noqa: E402

LABEL = "AsyncBenchmarkFixture"
FAMILIES = ("vector", "text", "combined")
LAYOUTS = ("map", "rows")


class Client:
    """Sequential setup requests; no retries or automatic unknown-outcome recovery."""

    def __init__(self, endpoint, evidence):
        url = urlsplit(endpoint)
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.path not in ("", "/")
            or url.query
            or url.fragment
        ):
            raise ValueError("expected an HTTP(S) base URL without credentials")
        cls = (
            http.client.HTTPSConnection
            if url.scheme == "https"
            else http.client.HTTPConnection
        )
        self.connection = cls(url.hostname, url.port, timeout=240)
        self.evidence = evidence

    def query(self, batch, *, consistency="strong"):
        request_id = str(uuid.uuid4())
        headers = {"content-type": "application/json"}
        if isinstance(batch, dsl.WriteBatch):
            headers["x-helix-await-durable"] = "true"
        started = time.time_ns()
        self.evidence.write(
            json.dumps({"event": "start", "request_id": request_id, "unix_ns": started})
            + "\n"
        )
        self.evidence.flush()
        self.connection.request(
            "POST",
            "/v2/query",
            body=batch.to_query_bytes(
                search_consistency=dsl.SearchConsistency(consistency)
            ),
            headers=headers,
        )
        response = self.connection.getresponse()
        body = response.read(16 * 1024 * 1024 + 1)
        self.evidence.write(
            json.dumps(
                {
                    "event": "response",
                    "request_id": request_id,
                    "status": response.status,
                    "unix_ns": time.time_ns(),
                    "body_sha256": hashlib.sha256(body).hexdigest(),
                }
            )
            + "\n"
        )
        self.evidence.flush()
        if response.status != 200 or len(body) > 16 * 1024 * 1024:
            raise ValueError("seed request failed; do not blindly resume this database")
        result = json.loads(body)
        if not isinstance(result, dict):
            raise ValueError("invalid seed response")  # noqa: TRY004 - malformed wire data
        return result

    def close(self):
        self.connection.close()


def verify_indexes(client, dataset, family, receipts, output, build_timeout_s):
    """Read build progress and sampled graph/search state; never issue writes."""
    deadline = time.monotonic() + build_timeout_s
    while True:
        statuses = [
            client.query(
                dsl.read_batch()
                .var_as(
                    "operation", dsl.g().get_index_operation(receipt["operation_id"])
                )
                .returning(["operation"])
            )["operation"]
            for receipt in receipts
        ]
        (output / "index-status.json").write_text(json.dumps(statuses, indent=2) + "\n")
        if all(status["status"] == "succeeded" for status in statuses):
            break
        if any(
            status["status"] not in ("queued", "running", "succeeded")
            for status in statuses
        ):
            raise ValueError("seed index build failed")
        if time.monotonic() >= deadline:
            raise TimeoutError("seed index build still incomplete")
        time.sleep(1)
    # Check direct graph data and index membership against source rows.
    # Traversal-scoped searches avoid ambiguous top-k ties in DBpedia.
    with (output / "entity-ids.u64le").open("rb") as ids:
        for ordinal in sorted({0, dataset.rows // 2, dataset.rows - 1}):
            ids.seek(ordinal * 8)
            entity = struct.unpack("<Q", ids.read(8))[0]
            document = dataset.document(ordinal)
            traversal = dsl.g().n(entity)
            query = dsl.read_batch().var_as(
                "graph", traversal.value_map(["ordinal", "embedding", "body"])
            )
            expected = {"graph": [document]}
            if family in ("vector", "combined"):
                query = query.var_as(
                    "vector",
                    traversal.vector_search(
                        LABEL, "embedding", document["embedding"], 1
                    ).id(),
                )
                expected["vector"] = [entity]
            word = re.search(r"[a-zA-Z]{4,}", document["body"])
            if family in ("text", "combined") and word is not None:
                query = query.var_as(
                    "text",
                    traversal.text_search(LABEL, "body", word.group().lower(), 1).id(),
                )
                expected["text"] = [entity]
            for consistency in ("strong", "eventual"):
                if (
                    client.query(
                        query.returning(list(expected)), consistency=consistency
                    )
                    != expected
                ):
                    raise ValueError("seed search/source verification failed")
    return statuses


def seed(
    dataset, family, layout, endpoint, output, *, batch_size=64, build_timeout_s=86400
):
    """Create immutable ordinal/ID evidence only after graph and index validation.

    `layout` records the writer's `HELIX_INDEX_QUEUE_LAYOUT`; the caller owns
    the writer and must have started it with that layout.
    """
    if (
        layout not in LAYOUTS
        or family not in FAMILIES
        or type(batch_size) is not int
        or not 1 <= batch_size <= 512
    ):
        raise ValueError("invalid family or seed batch size")
    if not 0 < build_timeout_s <= 7 * 86400:
        raise ValueError("invalid build timeout")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    report = {
        "status": "incomplete",
        "family": family,
        "layout": layout,
        "rows": dataset.rows,
        "dimension": dataset.dimension,
        "fixture": dataset.manifest,
        "batch_size": batch_size,
        "build_timeout_s": build_timeout_s,
        "started_unix_ns": time.time_ns(),
        "endpoint": endpoint,
        "seed_sha256": fixture.sha256(Path(__file__)),
    }
    (output / "seed.json").write_text(json.dumps(report, indent=2) + "\n")
    with (output / "requests.jsonl").open("x", buffering=1) as evidence:
        client = Client(endpoint, evidence)
        try:
            count_query = (
                dsl.read_batch()
                .var_as("count", dsl.g().n_with_label(LABEL).count())
                .returning(["count"])
            )
            if client.query(count_query)["count"] != 0:
                raise ValueError("benchmark label is not empty")
            with (output / "entity-ids.u64le").open("xb") as ids:
                for start in range(0, dataset.rows, batch_size):
                    batch = dsl.write_batch()
                    names = []
                    for ordinal in range(start, min(start + batch_size, dataset.rows)):
                        name = f"n{ordinal}"
                        names.append(name)
                        batch = batch.var_as(
                            name, dsl.g().add_n(LABEL, dataset.document(ordinal)).id()
                        )
                    result = client.query(batch.returning(names))
                    if set(result) != set(names):
                        raise ValueError("seed result omitted an entity")
                    for name in names:
                        values = result[name]
                        if (
                            not isinstance(values, list)
                            or len(values) != 1
                            or type(values[0]) is not int
                            or not 0 <= values[0] < 2**64
                        ):
                            raise ValueError("invalid returned entity ID")
                        ids.write(struct.pack("<Q", values[0]))
                    ids.flush()
                    print(
                        json.dumps(
                            {
                                "seeded_rows": start + len(names),
                                "total_rows": dataset.rows,
                            }
                        ),
                        flush=True,
                    )
            if client.query(count_query)["count"] != dataset.rows:
                raise ValueError("seed graph count mismatch")
            indexes = []
            if family in ("vector", "combined"):
                indexes.append(
                    dsl.g().create_vector_index_nodes(
                        LABEL,
                        "embedding",
                        dataset.dimension,
                        dsl.VectorDistanceMetric.COSINE,
                    )
                )
            if family in ("text", "combined"):
                indexes.append(dsl.g().create_text_index_nodes(LABEL, "body"))
            receipts = []
            for traversal in indexes:
                receipt = client.query(
                    dsl.write_batch().var_as("index", traversal).returning(["index"])
                )["index"]
                if receipt["kind"] != "accepted":
                    raise ValueError("seed requires fresh index creation")
                receipts.append(receipt)
            (output / "index-receipts.json").write_text(
                json.dumps(receipts, indent=2) + "\n"
            )
            statuses = verify_indexes(
                client, dataset, family, receipts, output, build_timeout_s
            )
            report.update(
                status="complete",
                completed_unix_ns=time.time_ns(),
                ids_sha256=fixture.sha256(output / "entity-ids.u64le"),
                index_receipts=receipts,
                index_status=statuses,
            )
            (output / "seed.json").write_text(json.dumps(report, indent=2) + "\n")
        finally:
            client.close()
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--family", choices=FAMILIES, required=True)
    parser.add_argument("--layout", choices=LAYOUTS, required=True)
    parser.add_argument("--writer", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--small-payload", action="store_true")
    args = parser.parse_args()
    with Dataset(args.fixture, small_payload=args.small_payload) as dataset:
        seed(
            dataset,
            args.family,
            args.layout,
            args.writer,
            args.output,
            batch_size=args.batch_size,
        )
