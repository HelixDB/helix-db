"""Read verified paired fixtures without keeping all text/vectors in Python RAM."""

import importlib.util
import json
import mmap
import struct
from array import array
from contextlib import ExitStack
from pathlib import Path

import small_fixture

SPEC = importlib.util.spec_from_file_location(
    "benchmark_fixture", Path(__file__).parents[1] / "prepare-dbpedia-vector-fixture.py"
)
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


class Dataset:
    """One verified, immutable fixture; setup verification is outside measurement.

    Text offsets occupy eight bytes per row. File maps are read-only; a caller
    must not modify fixture files while this context is open.
    """

    def __init__(self, path, *, small_payload=False):
        self.small_payload = small_payload
        self.path = Path(path)
        self.resources = ExitStack()

    def __enter__(self):
        self.manifest = (
            small_fixture.verify(self.path)
            if self.small_payload
            else fixture.verify_fixture(self.path)
        )
        self.rows = self.manifest["rows"]
        self.dimension = self.manifest["dimension"]
        try:
            vectors = self.resources.enter_context(self.path.open("rb"))
            documents = self.resources.enter_context(
                Path(str(self.path) + ".text.jsonl").open("rb")
            )
            self.vectors = self.resources.enter_context(
                mmap.mmap(vectors.fileno(), 0, access=mmap.ACCESS_READ)
            )
            self.documents = self.resources.enter_context(
                mmap.mmap(documents.fileno(), 0, access=mmap.ACCESS_READ)
            )
            self.offsets = array("Q", [0])
            for line in documents:
                self.offsets.append(self.offsets[-1] + len(line))
            if len(self.offsets) != self.rows + 1:
                raise ValueError("fixture changed after verification")
            self.vector = struct.Struct(f"<{self.dimension}f")
            return self
        except BaseException:
            self.resources.close()
            raise

    def document(self, ordinal):
        if type(ordinal) is not int or not 0 <= ordinal < self.rows:
            raise ValueError("fixture ordinal out of range")
        text = json.loads(
            self.documents[self.offsets[ordinal] : self.offsets[ordinal + 1]]
        )
        return {
            "ordinal": ordinal,
            "embedding": list(
                self.vector.unpack_from(self.vectors, 8 + ordinal * self.dimension * 4)
            ),
            "body": text["title"] + "\n" + text["text"],
        }

    def __exit__(self, *exception):
        return self.resources.__exit__(*exception)
