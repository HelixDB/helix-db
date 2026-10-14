"""Trace specification/generation, replay trace validation and matrix planning."""

import hashlib
import io
import json
import struct
import tempfile
import unittest
from pathlib import Path

import matrix
import replay
import small_fixture
import workload
from dataset import Dataset

ROWS = 64


def spec(**changes):
    return {
        "random_seed": 7,
        "insert_percent": 20,
        "remove_percent": 20,
        "delete_percent": 20,
        "hot_percent": 50,
        "hot_entities": 8,
        "top_k": 10,
        "phases": [
            {
                "name": "warmup",
                "duration_s": 2,
                "write_rps": 5,
                "strong_rps": 1,
                "eventual_rps": 1,
            },
            {
                "name": "measure",
                "duration_s": 3,
                "write_rps": 20,
                "strong_rps": 2,
                "eventual_rps": 3,
            },
        ],
    } | changes


class Fixture(unittest.TestCase):
    """A 64-row synthetic fixture and a matching seed whose IDs are 1000 + ordinal."""

    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        root = Path(cls.temporary.name)
        cls.manifest = small_fixture.generate(root / "fixture", rows=ROWS)
        cls.fbin = root / "fixture" / "fixture.fbin"
        cls.seed = root / "seed"
        cls.seed.mkdir()
        ids = b"".join(struct.pack("<Q", 1000 + n) for n in range(ROWS))
        (cls.seed / "entity-ids.u64le").write_bytes(ids)
        cls.seed_json(family="combined")

    @classmethod
    def seed_json(cls, **changes):
        (cls.seed / "seed.json").write_text(
            json.dumps(
                {
                    "status": "complete",
                    "rows": ROWS,
                    "dimension": 8,
                    "fixture": cls.manifest,
                    "family": "combined",
                    "ids_sha256": hashlib.sha256(
                        (cls.seed / "entity-ids.u64le").read_bytes()
                    ).hexdigest(),
                }
                | changes
            )
        )

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def generate(self, name, specification=None, family=None):
        output = Path(self.temporary.name) / name
        with Dataset(self.fbin, small_payload=True) as dataset:
            manifest = workload.generate(
                dataset,
                self.seed,
                specification or spec(),
                output,
                workload_family=family,
            )
        rows = list(map(json.loads, (output / "trace.jsonl").read_text().splitlines()))
        operations = list(
            map(json.loads, (output / "operations.jsonl").read_text().splitlines())
        )
        return manifest, rows, operations, output


class Specification(unittest.TestCase):
    def test_accepts_the_reference_shape(self):
        workload.validate(spec())
        workload.validate(spec(top_k=workload.MAX_TOP_K))

    def test_rejects_invalid_specifications(self):
        phases = spec()["phases"]
        invalid = [
            spec() | {"extra": 1},
            {k: v for k, v in spec().items() if k != "remove_percent"},
            spec(top_k=801),
            spec(top_k=0),
            spec(insert_percent=101),
            spec(insert_percent=40, remove_percent=40, delete_percent=21),
            spec(phases=[]),
            spec(phases=[phases[0]]),
            spec(phases=[phases[1], phases[0]]),
            spec(phases=[phases[1], phases[1]]),
            spec(phases=[phases[1] | {"duration_s": 0}]),
            spec(
                phases=[
                    phases[1] | {"write_rps": 0, "strong_rps": 0, "eventual_rps": 0}
                ]
            ),
            spec(phases=[phases[1] | {"write_rps": 1.5}]),
            spec(phases=[phases[1] | {"extra": 1}]),
        ]
        for specification in invalid:
            with (
                self.subTest(specification=specification),
                self.assertRaises(ValueError),
            ):
                workload.validate(specification)

    def test_schedule_offers_exact_integer_counts_in_order(self):
        offers = list(workload.schedule(spec()["phases"]))
        self.assertEqual(len(offers), 2 * 7 + 3 * 25)
        self.assertEqual([at for _, at, _ in offers], sorted(at for _, at, _ in offers))
        measured = [o for o in offers if o[0] == "measure"]
        self.assertEqual(sum(kind is replay.Kind.WRITE for _, _, kind in measured), 60)
        self.assertTrue(
            all(2_000_000_000 <= at < 5_000_000_000 for _, at, _ in measured)
        )
        self.assertEqual(
            workload.phase_bounds(spec()["phases"]),
            [("warmup", 0, 2_000_000_000), ("measure", 2_000_000_000, 5_000_000_000)],
        )


class Generation(Fixture):
    def test_combined_trace_mix_cohorts_and_determinism(self):
        manifest, rows, operations, output = self.generate("combined")
        _, _, _, again = self.generate("combined-again")
        self.assertEqual(
            (output / "trace.jsonl").read_bytes(), (again / "trace.jsonl").read_bytes()
        )
        self.assertEqual(
            (manifest["offered"], len(rows), len(operations)), (89, 89, 89)
        )
        self.assertEqual(manifest["mutation_scope"], "whole_entity")
        self.assertEqual(
            manifest["phases"][-1], ("measure", 2_000_000_000, 5_000_000_000)
        )
        kinds = {op["operation"] for op in operations if op["kind"] == "write"}
        self.assertEqual(kinds, {"insert", "remove", "delete", "update"})
        for row, op in zip(rows, operations, strict=True):
            body = json.dumps(row["payload"])
            if op["operation"] == "delete":
                self.assertGreaterEqual(op["ordinal"], ROWS * 3 // 4)
                self.assertIn('"drop"', body)
                self.assertIn(f'"ids": [{1000 + op["ordinal"]}]', body)
            elif op["operation"] in ("update", "remove"):
                self.assertLess(op["ordinal"], ROWS * 3 // 4)
            elif op["operation"] == "insert":
                self.assertGreaterEqual(op["ordinal"], ROWS)
            else:
                self.assertIn("vector_search_nodes", body)
                self.assertIn("text_search_nodes", body)
                self.assertEqual(row["payload"]["search_consistency"], row["kind"])
        # The generated trace passes the replay's own validation.
        digest = hashlib.sha256()
        with (output / "trace.jsonl").open("rb") as source:
            self.assertEqual(len(list(replay.offers(source, digest, 1 << 20))), 89)
        self.assertEqual(digest.hexdigest(), manifest["trace_sha256"])

    def test_isolated_vector_family_touches_only_embeddings(self):
        manifest, rows, operations, _ = self.generate("vector", family="vector")
        self.assertEqual(manifest["mutation_scope"], "indexed_property")
        for row, op in zip(rows, operations, strict=True):
            body = json.dumps(row["payload"])
            self.assertNotIn('"body"', body)
            self.assertNotIn("text_search_nodes", body)
            if op["operation"] == "delete":
                self.assertNotIn('"drop"', body)
                self.assertIn("remove_property", body)

    def test_hot_updates_only_touch_the_hot_cohort(self):
        hot = spec(
            insert_percent=0, remove_percent=0, delete_percent=0, hot_percent=100
        )
        _, _, operations, _ = self.generate("hot", hot)
        writes = [op for op in operations if op["kind"] == "write"]
        self.assertTrue(writes)
        self.assertTrue(
            all(op["operation"] == "update" and op["ordinal"] < 8 for op in writes)
        )

    def test_rejects_mismatched_seeds_and_families(self):
        with self.assertRaises(ValueError):
            self.generate("too-hot", spec(hot_entities=ROWS))
        try:
            self.seed_json(ids_sha256="0" * 64)
            with self.assertRaises(ValueError):
                self.generate("bad-ids")
            self.seed_json(family="text")
            with self.assertRaises(ValueError):
                self.generate("bad-family", family="vector")
        finally:
            self.seed_json()


class ReplayTrace(unittest.TestCase):
    def offers(self, *rows, limit=1 << 20):
        data = "".join(json.dumps(row) + "\n" for row in rows).encode()
        return list(replay.offers(io.BytesIO(data), hashlib.sha256(), limit))

    def row(self, identity=0, at_ns=0, kind="write", **payload):
        request_type = "write" if kind == "write" else "read"
        return {
            "id": identity,
            "at_ns": at_ns,
            "kind": kind,
            "payload": {"request_type": request_type} | payload,
        }

    def test_valid_trace_adds_explicit_consistency(self):
        offers = self.offers(
            self.row(), self.row(1, 5, "eventual"), self.row(2, 5, "strong")
        )
        self.assertEqual(
            [o.kind for o in offers],
            [replay.Kind.WRITE, replay.Kind.EVENTUAL, replay.Kind.STRONG],
        )
        self.assertIn(b'"search_consistency":"eventual"', offers[1].body)

    def test_rejects_invalid_traces(self):
        invalid = [
            (self.row(1),),
            (self.row(), self.row(2)),
            (self.row(at_ns=5), self.row(1, 4)),
            (self.row(kind="strong") | {"payload": {"request_type": "write"}},),
            (self.row(kind="eventual", search_consistency="strong"),),
            (self.row() | {"extra": 1},),
            (self.row(at_ns=-1),),
            (self.row(kind="other"),),
        ]
        for rows in invalid:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.offers(*rows)
        with self.assertRaises(ValueError):
            self.offers(self.row(padding="x" * 100), limit=64)


class Planning(unittest.TestCase):
    def calibration(self, **changes):
        entry = {
            "fixture": "small",
            "family": "combined",
            "below": 10,
            "near": [20, 30],
            "above": 40,
            "strong": 2,
            "eventual": 3,
        } | changes
        return {"evidence": "calibration run reference", "rates": [entry]}

    def test_screens_cover_every_requested_workload(self):
        plans = matrix.screens(warmup_s=5, duration_s=9)
        self.assertEqual(
            set(plans),
            {
                "vector",
                "text",
                "combined",
                "searches",
                "hot-updates",
                "removal-deletion",
                "backlog",
            },
        )
        self.assertEqual(plans["vector"]["family"], "vector")
        self.assertEqual(plans["text"]["family"], "text")
        self.assertEqual(plans["hot-updates"]["spec"]["hot_percent"], 100)
        self.assertEqual(plans["removal-deletion"]["spec"]["remove_percent"], 40)
        rates = [p["write_rps"] for p in plans["backlog"]["spec"]["phases"]]
        self.assertEqual(rates, [20, 20, 40, 80])
        self.assertEqual(plans["searches"]["spec"]["phases"][-1]["strong_rps"], 20)
        for plan in plans.values():
            workload.validate(plan["spec"])
            self.assertEqual(plan["spec"]["phases"][0]["name"], "warmup")
        self.assertNotEqual(
            matrix.screens()["combined"]["spec"]["phases"][0]["name"], "warmup"
        )

    def test_final_matrix_shares_traces_across_layouts(self):
        plan = matrix.final(
            self.calibration(), warmup_s=60, measure_s=300, repetitions=3
        )
        # below, near-1, near-2, above, growing x warm/cold x 3 repetitions.
        self.assertEqual(len(plan["traces"]), 5 * 2 * 3)
        self.assertEqual(len(plan["runs"]), 5 * 2 * 3 * 2)
        by_trace = {}
        for run in plan["runs"]:
            by_trace.setdefault(run["trace"], []).append(run["layout"])
        self.assertTrue(
            all(sorted(layouts) == ["map", "rows"] for layouts in by_trace.values())
        )
        self.assertEqual(by_trace["small-combined-below-warm-r1"], ["map", "rows"])
        self.assertEqual(by_trace["small-combined-below-warm-r2"], ["rows", "map"])
        warm = plan["traces"]["small-combined-growing-warm-r1"]["spec"]["phases"]
        self.assertEqual([p["write_rps"] for p in warm], [10, 40, 80, 160])
        self.assertEqual(warm[0]["name"], "warmup")
        cold = plan["traces"]["small-combined-above-cold-r3"]["spec"]
        self.assertEqual([p["name"] for p in cold["phases"]], ["measure-1"])
        self.assertEqual(cold["random_seed"], 1829)
        self.assertAlmostEqual(
            plan["planned_load_hours"], (15 * 360 + 15 * 300) * 2 / 3600
        )

    def test_final_matrix_rejects_invalid_calibration(self):
        invalid = [
            {"rates": []},
            {"evidence": " ", "rates": self.calibration()["rates"]},
            self.calibration(near=[]),
            self.calibration(near=[30, 20]),
            self.calibration(below=20),
            self.calibration(above=30),
            self.calibration(strong=0),
            self.calibration(fixture="other"),
            self.calibration() | {"rates": self.calibration()["rates"] * 2},
        ]
        for calibration in invalid:
            with self.subTest(calibration=calibration), self.assertRaises(ValueError):
                matrix.final(calibration)
        with self.assertRaises(ValueError):
            matrix.final(self.calibration(), layouts=("buckets",))

    def test_write_records_specification_hashes(self):
        with tempfile.TemporaryDirectory() as root:
            plan = matrix.write({"traces": matrix.screens()}, Path(root) / "plan")
            written = Path(root) / "plan" / "combined.json"
            self.assertEqual(
                plan["specifications"]["combined"]["sha256"],
                hashlib.sha256(written.read_bytes()).hexdigest(),
            )
            self.assertEqual(plan["specifications"]["combined"]["family"], "combined")
            with self.assertRaises(FileExistsError):
                matrix.write({"traces": {}}, Path(root) / "plan")


if __name__ == "__main__":
    unittest.main()
