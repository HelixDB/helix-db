"""DBpedia fixture verification and verified prefix subsets (stdlib only)."""

import json
import struct
import tempfile
import unittest
from pathlib import Path

from dataset import Dataset, fixture

ROWS = 6


def write_parent(root):
    """A synthetic paired fixture that verifies as a prefix of the 1M set."""
    vectors = root / "parent.fbin"
    documents = Path(str(vectors) + ".text.jsonl")
    with vectors.open("wb") as sink:
        sink.write(struct.pack("<II", ROWS, fixture.DIMENSION))
        for ordinal in range(ROWS):
            sink.write(
                struct.pack(
                    f"<{fixture.DIMENSION}f", *[ordinal + 0.5] * fixture.DIMENSION
                )
            )
    documents.write_text(
        "".join(
            json.dumps(
                {
                    "ordinal": n,
                    "source_id": f"s{n}",
                    "title": f"Title {n}",
                    "text": f"text body {n}",
                }
            )
            + "\n"
            for n in range(ROWS)
        )
    )
    manifest = {
        "dataset": fixture.DATASET,
        "revision": fixture.REVISION,
        "rows": ROWS,
        "dimension": fixture.DIMENSION,
        "vectors": {"file": vectors.name, "sha256": fixture.sha256(vectors)},
        "documents": {"file": documents.name, "sha256": fixture.sha256(documents)},
        "prefix_of": {
            "rows": fixture.FULL_ROW_COUNT,
            "vectors_sha256": "v",
            "documents_sha256": "d",
        },
    }
    Path(str(vectors) + ".manifest.json").write_text(json.dumps(manifest))
    return vectors


class Prefix(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.parent = write_parent(self.root)

    def test_prefix_is_verified_and_readable(self):
        manifest = fixture.write_prefix(self.parent, self.root / "prefix.fbin", 4)
        self.assertEqual(manifest["rows"], 4)
        self.assertEqual(manifest["prefix_of"]["rows"], ROWS)
        self.assertEqual(
            manifest["prefix_of"]["vectors_sha256"], fixture.sha256(self.parent)
        )
        with Dataset(self.root / "prefix.fbin") as dataset:
            document = dataset.document(3)
        self.assertEqual(document["embedding"][:2], [3.5, 3.5])
        self.assertEqual(document["body"], "Title 3\ntext body 3")

    def test_rejects_oversized_existing_or_damaged_prefixes(self):
        for rows in (0, ROWS, ROWS + 1):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                fixture.write_prefix(self.parent, self.root / f"bad-{rows}.fbin", rows)
        fixture.write_prefix(self.parent, self.root / "prefix.fbin", 2)
        with self.assertRaises(FileExistsError):
            fixture.write_prefix(self.parent, self.root / "prefix.fbin", 2)
        text = Path(str(self.root / "prefix.fbin") + ".text.jsonl")
        text.write_text(text.read_text().replace("text body 1", "text body X"))
        with self.assertRaises(ValueError):
            fixture.verify_fixture(self.root / "prefix.fbin")

    def test_full_fixtures_need_a_pinned_row_count(self):
        manifest_path = Path(str(self.parent) + ".manifest.json")
        manifest = json.loads(manifest_path.read_text())
        del manifest["prefix_of"]
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaises(ValueError):
            fixture.verify_fixture(self.parent)


if __name__ == "__main__":
    unittest.main()
