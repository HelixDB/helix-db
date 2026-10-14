#!/usr/bin/env python3
"""Generate identical open-loop payloads and mutation evidence for paired runs.

One trace is generated per screen/repetition and replayed unchanged against
every queue layout. It references seed entities by the IDs in the seed's
`entity-ids.u64le`; a run refuses a seed whose ID file differs from the one the
trace was generated from. Warm-up phases (names starting with `warmup`) must
precede all measured phases and are excluded from measurement statistics.
"""

import argparse
import heapq
import json
import random
import re
import struct
from collections import Counter
from pathlib import Path

from dataset import Dataset, fixture
from replay import Kind, integer
from seed import LABEL, dsl

NANOSECONDS = 1_000_000_000
# Documented product cap on results per text search and unrestricted vector k.
MAX_TOP_K = 800
SPEC_FIELDS = {
    "random_seed",
    "phases",
    "insert_percent",
    "remove_percent",
    "delete_percent",
    "hot_percent",
    "hot_entities",
    "top_k",
}
PHASE_FIELDS = {"name", "duration_s", "write_rps", "strong_rps", "eventual_rps"}


def validate(spec):
    """Reject malformed specifications before any output exists."""
    if set(spec) != SPEC_FIELDS:
        raise ValueError("unexpected trace specification fields")
    integer(spec["random_seed"])
    integer(spec["hot_entities"], 1)
    if not 1 <= integer(spec["top_k"], 1) <= MAX_TOP_K:
        raise ValueError(f"top_k must not exceed the {MAX_TOP_K}-result cap")
    for field in ("insert_percent", "remove_percent", "delete_percent", "hot_percent"):
        if integer(spec[field]) > 100:
            raise ValueError("percentage exceeds 100")
    if spec["insert_percent"] + spec["remove_percent"] + spec["delete_percent"] > 100:
        raise ValueError("write percentages exceed 100")
    phases = spec["phases"]
    if not isinstance(phases, list) or not phases:
        raise ValueError("at least one phase is required")
    names = set()
    measured = False
    for phase in phases:
        if not isinstance(phase, dict) or set(phase) != PHASE_FIELDS:
            raise ValueError("unexpected phase fields")
        name = phase["name"]
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("phase names must be nonempty and unique")
        names.add(name)
        if name.startswith("warmup") and measured:
            raise ValueError("warm-up phases must precede measured phases")
        measured |= not name.startswith("warmup")
        integer(phase["duration_s"], 1)
        if not any(integer(phase[f"{kind.value}_rps"]) for kind in Kind):
            raise ValueError("each phase must offer work")
    if not measured:
        raise ValueError("at least one measured (non-warm-up) phase is required")


def phase_bounds(phases):
    """Returns `(name, start_ns, end_ns)` per phase on the trace clock."""
    bounds = []
    offset = 0
    for phase in phases:
        end = offset + phase["duration_s"] * NANOSECONDS
        bounds.append((phase["name"], offset, end))
        offset = end
    return bounds


def schedule(phases):
    """Integer arithmetic: exactly rate * duration offers, no float drift."""
    for (name, offset, _), phase in zip(phase_bounds(phases), phases, strict=True):
        streams = [
            offers_at(offset, phase["duration_s"], phase[f"{kind.value}_rps"], kind)
            for kind in Kind
        ]
        for at_ns, kind in heapq.merge(*streams):
            yield name, at_ns, Kind(kind)


def offers_at(offset, duration, rate, kind):
    for i in range(duration * rate):
        yield offset + i * NANOSECONDS // rate, kind.value


def search_term(body):
    word = re.search(r"[a-zA-Z]{4,}", body)
    return word.group().lower() if word is not None else "entity"


def generate(dataset, seed_directory, specification, output, *, workload_family=None):
    """Complete manifests are written last; partial traces are never valid input.

    Writes are inserts, index-property removals, whole-entity deletes, or
    updates (hot or cold). Updates and removals share the first three quarters
    of the seed, so removed properties are later re-added by updates; deletes
    use the last quarter. With a family isolated from a combined seed, every
    mutation touches only that family's property (deletes become removals).
    """
    spec = specification
    validate(spec)
    seed_directory = Path(seed_directory)
    seed_path = seed_directory / "seed.json"
    seed = json.loads(seed_path.read_text())
    id_path = seed_directory / "entity-ids.u64le"
    if (
        seed["status"] != "complete"
        or seed["rows"] != dataset.rows
        or seed["dimension"] != dataset.dimension
        or seed["fixture"] != dataset.manifest
        or seed["family"] not in ("vector", "text", "combined")
        or fixture.sha256(id_path) != seed["ids_sha256"]
        or id_path.stat().st_size != dataset.rows * 8
    ):
        raise ValueError("seed does not match the verified fixture and entity IDs")
    family = seed["family"] if workload_family is None else workload_family
    if family not in ("vector", "text", "combined") or (
        seed["family"] != "combined" and family != seed["family"]
    ):
        raise ValueError("workload family is not available in the seed")
    isolate = seed["family"] == "combined" and family != "combined"
    properties = {"vector": ["embedding"], "text": ["body"]}.get(
        family, ["embedding", "body"]
    )
    delete_start = dataset.rows * 3 // 4
    hot = spec["hot_entities"]
    if not hot < delete_start < dataset.rows:
        raise ValueError("fixture must have distinct hot, cold and delete cohorts")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    rng = random.Random(spec["random_seed"])
    insert_end = spec["insert_percent"]
    remove_end = insert_end + spec["remove_percent"]
    delete_end = remove_end + spec["delete_percent"]
    counts = Counter()
    inserted = 0
    deleted = 0
    identity = -1
    with (
        id_path.open("rb") as ids,
        (output / "trace.jsonl").open("x") as trace,
        (output / "operations.jsonl").open("x") as operations,
    ):

        def entity_id(ordinal):
            ids.seek(ordinal * 8)
            return struct.unpack("<Q", ids.read(8))[0]

        for identity, (phase, at_ns, kind) in enumerate(schedule(spec["phases"])):
            annotation = {"id": identity, "phase": phase, "at_ns": at_ns}
            source_ordinal = rng.randrange(dataset.rows)
            document = dataset.document(source_ordinal)
            if kind is Kind.WRITE:
                roll = rng.randrange(100)
                operation = (
                    "insert"
                    if roll < insert_end
                    else "remove"
                    if roll < remove_end
                    else "delete"
                    if roll < delete_end
                    else "update"
                )
                if operation == "insert":
                    ordinal = dataset.rows + inserted
                    inserted += 1
                    excluded = {"embedding", "body"} - set(properties)
                    fields = document | {"ordinal": ordinal}
                    traversal = dsl.g().add_n(
                        LABEL, {k: v for k, v in fields.items() if k not in excluded}
                    )
                elif operation == "delete":
                    # A quarter of deletes revisit a small cohort; repeated
                    # deletes are no-ops that enqueue no index work.
                    cohort = dataset.rows - delete_start
                    ordinal = delete_start + (
                        rng.randrange(min(hot, cohort))
                        if rng.randrange(4) == 0
                        else deleted % cohort
                    )
                    deleted += 1
                    traversal = dsl.g().n(entity_id(ordinal))
                    if isolate:
                        for name in properties:
                            traversal = traversal.remove_property(name)
                    else:
                        traversal = traversal.drop()
                else:
                    ordinal = (
                        rng.randrange(hot)
                        if rng.randrange(100) < spec["hot_percent"]
                        else rng.randrange(hot, delete_start)
                    )
                    traversal = dsl.g().n(entity_id(ordinal))
                    if operation == "remove":
                        # Combined workloads remove one indexed property.
                        traversal = traversal.remove_property(rng.choice(properties))
                    else:
                        # Include same-source restores as well as replacements.
                        if rng.randrange(4) == 0:
                            source_ordinal = ordinal
                            document = dataset.document(source_ordinal)
                        for name in properties:
                            traversal = traversal.set_property(name, document[name])
                annotation.update(operation=operation, ordinal=ordinal)
                batch = dsl.write_batch().var_as("mutation", traversal)
            else:
                batch = dsl.read_batch()
                if "embedding" in properties:
                    batch = batch.var_as(
                        "vectors",
                        dsl.g()
                        .vector_search_nodes(
                            LABEL, "embedding", document["embedding"], spec["top_k"]
                        )
                        .id(),
                    )
                if "body" in properties:
                    batch = batch.var_as(
                        "text",
                        dsl.g()
                        .text_search_nodes(
                            LABEL, "body", search_term(document["body"]), spec["top_k"]
                        )
                        .id(),
                    )
                batch = batch.returning(
                    [
                        n
                        for p, n in (("embedding", "vectors"), ("body", "text"))
                        if p in properties
                    ]
                )
                annotation["operation"] = "search"
            payload = json.loads(batch.to_query_bytes())
            payload["search_consistency"] = (
                "eventual" if kind is Kind.EVENTUAL else "strong"
            )
            row = {
                "id": identity,
                "at_ns": at_ns,
                "kind": kind.value,
                "payload": payload,
            }
            trace.write(
                json.dumps(
                    row, separators=(",", ":"), ensure_ascii=False, allow_nan=False
                )
                + "\n"
            )
            annotation.update(kind=kind.value, source_ordinal=source_ordinal)
            operations.write(json.dumps(annotation, separators=(",", ":")) + "\n")
            counts[f"{phase}:{kind.value}:{annotation['operation']}"] += 1
    if identity < 0:
        raise ValueError("specification offers no requests")
    manifest = {
        "schema": 2,
        "specification": spec,
        "phases": phase_bounds(spec["phases"]),
        "family": family,
        "seed_family": seed["family"],
        "mutation_scope": "indexed_property" if isolate else "whole_entity",
        "offered": identity + 1,
        "counts": dict(counts),
        "seed_sha256": fixture.sha256(seed_path),
        "ids_sha256": seed["ids_sha256"],
        "fixture": dataset.manifest,
        "generator_sha256": fixture.sha256(Path(__file__)),
        "trace_sha256": fixture.sha256(output / "trace.jsonl"),
        "operations_sha256": fixture.sha256(output / "operations.jsonl"),
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--small-payload", action="store_true")
    parser.add_argument("--workload-family", choices=("vector", "text", "combined"))
    args = parser.parse_args()
    with Dataset(args.fixture, small_payload=args.small_payload) as dataset:
        print(
            json.dumps(
                generate(
                    dataset,
                    args.seed,
                    json.loads(args.spec.read_text()),
                    args.output,
                    workload_family=args.workload_family,
                ),
                indent=2,
            )
        )
