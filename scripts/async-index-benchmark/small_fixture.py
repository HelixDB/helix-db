#!/usr/bin/env python3
"""Generate a reproducible small-payload fixture for the pending-member limit.

This synthetic fixture is separate from DBpedia. Each vector is 32 input bytes,
with exactly representable nonzero f32 coordinates derived from SHA-256. Text
has a fixed width and a shared search term. Generation is setup, not a workload.
"""

import argparse
import hashlib
import json
import struct
from pathlib import Path

NAME = "helix-async-small-payload"
DIMENSION = 8
REVISION = 1
MAX_ROWS = 1_000_000


def parameters(rows, seed):
    if type(rows) is not int or not 1 <= rows <= MAX_ROWS:
        raise ValueError("rows must be an integer from 1 through 1000000")
    if type(seed) is not int or not 0 <= seed < 2**64:
        raise ValueError("seed must be an unsigned 64-bit integer")


def row(seed, ordinal):
    digest = hashlib.sha256(
        b"helix-small-payload-v1" + struct.pack("<QI", seed, ordinal)
    ).digest()
    vector = struct.pack(
        "<8f",
        *((value - 32767.5) / 32768 for value in struct.unpack("<8H", digest[:16])),
    )
    text = {
        "ordinal": ordinal,
        "source_id": f"synthetic:{ordinal:08x}",
        "title": "record",
        "text": f"entity {ordinal:08x} group {ordinal % 256:03d}",
    }
    return vector, (json.dumps(text, separators=(",", ":")) + "\n").encode()


def sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def generate(output, *, rows=MAX_ROWS, seed=1827):
    parameters(rows, seed)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    vectors = output / "fixture.fbin"
    documents = output / "fixture.fbin.text.jsonl"
    with vectors.open("xb") as binary, documents.open("xb") as text:
        binary.write(struct.pack("<II", rows, DIMENSION))
        for ordinal in range(rows):
            vector, document = row(seed, ordinal)
            binary.write(vector)
            text.write(document)
    manifest = {
        "dataset": NAME,
        "revision": REVISION,
        "rows": rows,
        "dimension": DIMENSION,
        "seed": seed,
        "generator_sha256": sha256(Path(__file__)),
        "vector_input_bytes_per_member": 32,
        "text_input_bytes_per_member": 32,
        "vectors": {"file": vectors.name, "sha256": sha256(vectors)},
        "documents": {"file": documents.name, "sha256": sha256(documents)},
    }
    # The manifest is the completion marker. Failed generation leaves no marker;
    # do not resume or overwrite an existing output directory.
    Path(str(vectors) + ".manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    return manifest


def verify(path):
    path = Path(path)
    manifest = json.loads(Path(str(path) + ".manifest.json").read_text())
    parameters(manifest["rows"], manifest["seed"])
    if (
        manifest["dataset"] != NAME
        or type(manifest["revision"]) is not int
        or manifest["revision"] != REVISION
        or manifest["dimension"] != DIMENSION
        or manifest["vector_input_bytes_per_member"] != 32
        or manifest["text_input_bytes_per_member"] != 32
    ):
        raise ValueError("small fixture identity or payload contract mismatch")
    documents = Path(str(path) + ".text.jsonl")
    for field, artifact in (("vectors", path), ("documents", documents)):
        if manifest[field] != {"file": artifact.name, "sha256": sha256(artifact)}:
            raise ValueError("small fixture hash or filename mismatch")
    with path.open("rb") as vectors, documents.open("rb") as text:
        if vectors.read(8) != struct.pack("<II", manifest["rows"], DIMENSION):
            raise ValueError("small fixture header mismatch")
        for ordinal in range(manifest["rows"]):
            vector, document = row(manifest["seed"], ordinal)
            if vectors.read(32) != vector or text.readline(1024) != document:
                raise ValueError("small fixture row differs from deterministic source")
        if vectors.read(1) or text.read(1):
            raise ValueError("small fixture has trailing data")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=1827)
    args = parser.parse_args()
    generate(args.output, rows=args.rows, seed=args.seed)
    print(json.dumps(verify(args.output / "fixture.fbin"), indent=2))


if __name__ == "__main__":
    main()
