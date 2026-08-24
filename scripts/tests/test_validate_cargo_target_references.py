import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "validate-cargo-target-references.py"
SPEC = importlib.util.spec_from_file_location("cargo_target_references", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class CargoTargetReferenceTests(unittest.TestCase):
    def setUp(self):
        self.catalog = MODULE.Catalog()
        self.catalog.packages.add("db")
        self.catalog.targets["test"].add("valid_contracts")
        self.catalog.targets["bench"].add("valid_benchmark")
        self.catalog.targets["bin"].add("valid_binary")

    def validate(self, command):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow = root / "workflow.yml"
            workflow.write_text(f"steps:\n  - run: {command}\n")
            return MODULE.validate_references(root, self.catalog, [workflow])

    def test_valid_fixture_references_pass(self):
        self.assertEqual(
            self.validate(
                "cargo test -p db --test valid_contracts && "
                "cargo bench --bench valid_benchmark && "
                "cargo run --bin valid_binary"
            ),
            [],
        )

    def test_stale_fixture_reference_fails(self):
        self.assertEqual(
            self.validate("cargo test -p db --test removed_contracts"),
            ["workflow.yml: unknown Cargo test target 'removed_contracts'"],
        )

    def test_inventory_reports_missing_and_stale_rows(self):
        catalog = MODULE.Catalog()
        catalog.db_targets.add(
            MODULE.Target("db", "current", "test", "crates/db/tests/current.rs")
        )
        documented = MODULE.parse_inventory(
            "| `stale` | `test` | `crates/db/tests/stale.rs` | old |"
        )
        actual = {(target.name, target.kind, target.source) for target in catalog.db_targets}
        self.assertEqual(actual - documented, {("current", "test", "crates/db/tests/current.rs")})
        self.assertEqual(documented - actual, {("stale", "test", "crates/db/tests/stale.rs")})

    def test_yaml_folded_commands_keep_multiline_target_flags(self):
        text = """steps:
  - run: >-
      cargo test -p db
      --test valid_contracts
"""
        self.assertEqual(
            MODULE.yaml_run_commands(text),
            ["cargo test -p db --test valid_contracts"],
        )

    def test_codec_boundary_rules_accept_typed_v2_and_reject_obsolete_dispatch(self):
        self.assertEqual(
            MODULE.forbidden_codec_source(
                "crates/db/src/index_lifecycle/secondary.rs",
                "use crate::encoding::v2::values::SecondaryEqualityBitmapValue;",
            ),
            [],
        )
        self.assertEqual(
            MODULE.forbidden_codec_source(
                "crates/db/src/index_lifecycle/secondary.rs",
                "let value: WorkValue = decode_work_value(bytes)?;",
            ),
            [
                "crates/db/src/index_lifecycle/secondary.rs: forbidden obsolete identifier 'WorkValue'",
                "crates/db/src/index_lifecycle/secondary.rs: forbidden obsolete identifier 'decode_work_value'",
            ],
        )
        self.assertEqual(
            MODULE.forbidden_codec_source(
                "crates/db/src/index_lifecycle/secondary.rs",
                "use bytes::BufMut; bytes.put_u64(entity_id);",
            ),
            [
                "crates/db/src/index_lifecycle/secondary.rs: raw managed-index serialization must use encoding::v2"
            ],
        )
        self.assertEqual(
            MODULE.forbidden_codec_source(
                "crates/db/src/index_lifecycle/tenant_envelope_migration.rs",
                "use bytes::BufMut; bytes.put_u64(entity_id);",
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
