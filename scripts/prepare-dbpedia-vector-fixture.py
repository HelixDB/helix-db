#!/usr/bin/env python3
"""Prepare pinned DBpedia vectors, with optional aligned text and hash manifest.

Downloading and converting needs `certifi`, `numpy` and `pyarrow`; they are
imported lazily so `--verify` and `--prefix-of` (and benchmark harnesses that
import `verify_fixture`) run on the Python standard library alone.
"""

import argparse
import hashlib
import json
import platform
import shutil
import ssl
import struct
import tempfile
import urllib.request
from contextlib import ExitStack
from pathlib import Path

DATASET = "Qdrant/dbpedia-entities-openai3-text-embedding-3-large-1536-1M"
REVISION = "4a9731217921bc476a0f03544f11f22ae4903fa5"
SHARD_COUNT = 26
SHARDS = tuple(
    f"train-{shard_index:05d}-of-{SHARD_COUNT:05d}.parquet"
    for shard_index in range(SHARD_COUNT)
)
COLUMN = "text-embedding-3-large-1536-embedding"
DEFAULT_ROW_COUNT = 50_000
BENCHMARK_ROW_COUNT = 100_000
FULL_ROW_COUNT = 1_000_000
DIMENSION = 1_536
EXPECTED_50K_SHA256 = "43a6b640d8b10a0e32a102ebade0d50bc5b526f52d485e6a4117eff93d59253e"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_batch(batch, remaining, ordinal, vector_sink, text_sink=None):
    """Write aligned rows; reject malformed vectors and text before publication."""
    import numpy as np
    from pyarrow import compute

    required = [COLUMN] + (["_id", "title", "text"] if text_sink is not None else [])
    if any(batch.schema.get_field_index(name) < 0 for name in required):
        raise ValueError("missing required fixture column")
    batch = batch.slice(0, min(batch.num_rows, remaining))
    if batch.num_rows == 0:
        return 0
    vectors = batch.column(batch.schema.get_field_index(COLUMN))
    if (
        vectors.null_count
        or not compute.all(
            compute.equal(compute.list_value_length(vectors), DIMENSION)
        ).as_py()
    ):
        raise ValueError("null vector or wrong vector dimension")
    flattened = vectors.flatten()
    if flattened.null_count:
        raise ValueError("null vector component")
    with np.errstate(over="ignore", invalid="ignore"):
        values = flattened.to_numpy(zero_copy_only=False).astype("<f4", copy=False)
    if not np.isfinite(values).all():
        raise ValueError("non-finite float32 vector component")
    vector_sink.write(values.tobytes())
    if text_sink is not None:
        columns = [
            batch.column(batch.schema.get_field_index(name)).to_pylist()
            for name in ("_id", "title", "text")
        ]
        for offset, (source_id, title, text) in enumerate(zip(*columns, strict=True)):
            if not all(isinstance(value, str) for value in (source_id, title, text)):
                raise ValueError("source ID, title and text must be strings")
            text_sink.write(
                json.dumps(
                    {
                        "ordinal": ordinal + offset,
                        "source_id": source_id,
                        "title": title,
                        "text": text,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                + "\n"
            )
    return batch.num_rows


def verify_fixture(output):
    """Reject incomplete or mismatched paired artifacts before a benchmark run."""
    manifest = json.loads(
        Path(str(output) + ".manifest.json").read_text(encoding="utf-8")
    )
    parent = manifest.get("prefix_of")
    if (
        manifest["dataset"] != DATASET
        or manifest["revision"] != REVISION
        or manifest["dimension"] != DIMENSION
        or not (
            manifest["rows"] in (DEFAULT_ROW_COUNT, BENCHMARK_ROW_COUNT, FULL_ROW_COUNT)
            if parent is None
            else 0 < manifest["rows"] < parent["rows"]
        )
    ):
        raise ValueError(
            "fixture provenance or dimensions do not match the pinned dataset"
        )
    output = Path(output)
    for field, path in (
        ("vectors", output),
        ("documents", Path(str(output) + ".text.jsonl")),
    ):
        expected = manifest[field]
        if expected["file"] != path.name or sha256(path) != expected["sha256"]:
            raise ValueError(f"{field} artifact does not match its manifest")
    if (
        parent is None
        and manifest["rows"] == DEFAULT_ROW_COUNT
        and manifest["vectors"]["sha256"] != EXPECTED_50K_SHA256
    ):
        raise ValueError("50K vectors differ from the pinned subset")
    with output.open("rb") as vectors:
        header = vectors.read(8)
    if (
        header != struct.pack("<II", manifest["rows"], DIMENSION)
        or output.stat().st_size != 8 + manifest["rows"] * DIMENSION * 4
    ):
        raise ValueError("fbin header or length does not match its manifest")
    with Path(str(output) + ".text.jsonl").open(encoding="utf-8") as documents:
        count = 0
        for ordinal, line in enumerate(documents):
            document = json.loads(line)
            if (
                type(document["ordinal"]) is not int
                or document["ordinal"] != ordinal
                or not all(
                    isinstance(document[name], str)
                    for name in ("source_id", "title", "text")
                )
            ):
                raise ValueError("text row does not match its vector ordinal")
            count += 1
    if count != manifest["rows"]:
        raise ValueError("text row count does not match vector row count")
    return manifest


def write_prefix(parent, output, rows):
    """Publish the first `rows` rows of a verified paired fixture, no download.

    The manifest records the parent's hashes; like the full fixture, it is the
    publication marker and is written last. Existing outputs are never replaced.
    """
    source = verify_fixture(parent)
    if type(rows) is not int or not 0 < rows < source["rows"]:
        raise ValueError("a prefix must be smaller than its parent fixture")
    output = Path(output)
    text_output = Path(str(output) + ".text.jsonl")
    manifest_output = Path(str(output) + ".manifest.json")
    if any(path.exists() for path in (output, text_output, manifest_output)):
        raise FileExistsError("prefix fixture output already exists")
    with Path(parent).open("rb") as vectors, output.open("xb") as sink:
        vectors.seek(8)
        sink.write(struct.pack("<II", rows, DIMENSION))
        remaining = rows * DIMENSION * 4
        while remaining:
            chunk = vectors.read(min(remaining, 8 * 1024 * 1024))
            if not chunk:
                raise ValueError("parent fixture is truncated")
            sink.write(chunk)
            remaining -= len(chunk)
    with (
        Path(str(parent) + ".text.jsonl").open("rb") as documents,
        text_output.open("xb") as sink,
    ):
        for _ in range(rows):
            sink.write(documents.readline())
    manifest = {
        key: value
        for key, value in source.items()
        if key not in ("rows", "vectors", "documents", "prefix_of")
    } | {
        "rows": rows,
        "vectors": {"file": output.name, "sha256": sha256(output)},
        "documents": {"file": text_output.name, "sha256": sha256(text_output)},
        "prefix_of": {
            "rows": source["rows"],
            "vectors_sha256": source["vectors"]["sha256"],
            "documents_sha256": source["documents"]["sha256"],
        },
        "prefix_script_sha256": sha256(Path(__file__)),
    }
    manifest_output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return verify_fixture(output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--rows",
        choices=(DEFAULT_ROW_COUNT, BENCHMARK_ROW_COUNT, FULL_ROW_COUNT),
        default=DEFAULT_ROW_COUNT,
        type=int,
    )
    parser.add_argument(
        "--with-text",
        action="store_true",
        help="write aligned text JSONL and a hash manifest alongside fbin",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="verify the existing paired fixture without downloading",
    )
    parser.add_argument(
        "--prefix-of",
        type=Path,
        help="write the first --prefix-rows rows of this verified paired fixture",
    )
    parser.add_argument("--prefix-rows", type=int)
    args = parser.parse_args()
    if args.verify:
        verify_fixture(args.output)
        print(f"verified {args.output}")
        return
    if args.prefix_of is not None:
        manifest = write_prefix(args.prefix_of, args.output, args.prefix_rows)
        print(f"{args.output}: verified {manifest['rows']}-row prefix")
        return
    import certifi
    import numpy as np
    import pyarrow
    from pyarrow import parquet

    args.output.parent.mkdir(parents=True, exist_ok=True)
    partial_output = args.output.with_suffix(f"{args.output.suffix}.partial")
    partial_output.unlink(missing_ok=True)

    text_output = Path(str(args.output) + ".text.jsonl")
    text_partial = Path(str(text_output) + ".partial")
    manifest_output = Path(str(args.output) + ".manifest.json")
    manifest_partial = Path(str(manifest_output) + ".partial")
    script_digest = sha256(Path(__file__))
    source_hashes = []
    written = 0
    tls_context = ssl.create_default_context(cafile=certifi.where())
    try:
        with tempfile.TemporaryDirectory(prefix="helix-dbpedia-") as temporary:
            temporary = Path(temporary)
            with ExitStack() as outputs:
                sink = outputs.enter_context(partial_output.open("wb"))
                text_sink = (
                    outputs.enter_context(
                        text_partial.open("w", encoding="utf-8", newline="\n")
                    )
                    if args.with_text
                    else None
                )
                sink.write(struct.pack("<II", args.rows, DIMENSION))
                for shard in SHARDS:
                    source = temporary / shard
                    url = (
                        f"https://huggingface.co/datasets/{DATASET}/resolve/"
                        f"{REVISION}/data/{shard}"
                    )
                    print(f"downloading {shard}", flush=True)
                    with (
                        urllib.request.urlopen(
                            url, context=tls_context, timeout=120
                        ) as response,
                        source.open("wb") as source_file,
                    ):
                        shutil.copyfileobj(response, source_file)
                    source_hashes.append({"shard": shard, "sha256": sha256(source)})
                    columns = [COLUMN] + (
                        ["_id", "title", "text"] if args.with_text else []
                    )
                    for batch in parquet.ParquetFile(source).iter_batches(
                        batch_size=512, columns=columns
                    ):
                        remaining = args.rows - written
                        if remaining == 0:
                            break
                        written += write_batch(
                            batch, remaining, written, sink, text_sink
                        )
                    source.unlink()
                    print(f"prepared_rows={written}", flush=True)
                    if written == args.rows:
                        break

        if written != args.rows:
            raise RuntimeError(f"expected {args.rows} rows, wrote {written}")
        digest = sha256(partial_output)
        if args.rows == DEFAULT_ROW_COUNT and digest != EXPECTED_50K_SHA256:
            raise RuntimeError(f"unexpected fixture SHA-256: {digest}")
        if args.with_text:
            manifest = {
                "dataset": DATASET,
                "revision": REVISION,
                "rows": written,
                "dimension": DIMENSION,
                "vector_encoding": "fbin little-endian float32",
                "text_encoding": "UTF-8 JSONL",
                "document_text": "title + newline + text",
                "alignment": "zero-based ordinal equals fbin row offset",
                "vectors": {"file": args.output.name, "sha256": digest},
                "documents": {"file": text_output.name, "sha256": sha256(text_partial)},
                "source_shards": source_hashes,
                "converter": {
                    "script_sha256": script_digest,
                    "python": platform.python_version(),
                    "numpy": np.__version__,
                    "pyarrow": pyarrow.__version__,
                },
            }
            manifest_partial.write_text(
                json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
            )
        partial_output.replace(args.output)
        if args.with_text:
            text_partial.replace(text_output)
            # The manifest is the publication marker. Consumers must verify both
            # hashes; a crash between renames must not admit a mixed fixture.
            manifest_partial.replace(manifest_output)
        print(f"{args.output}: {args.rows}x{DIMENSION}, sha256={digest}")
    except BaseException:
        partial_output.unlink(missing_ok=True)
        if args.with_text:
            text_partial.unlink(missing_ok=True)
            manifest_partial.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    main()
