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

    def test_examples_are_catalogued_and_validated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.catalog.add_metadata(
                {
                    "packages": [{
                        "name": "db",
                        "targets": [{
                            "name": "scaling",
                            "kind": ["example"],
                            "src_path": str(root / "crates/db/examples/scaling.rs"),
                            "required-features": ["diagnostics"],
                        }],
                    }],
                },
                root,
            )
        self.assertIn("scaling", self.catalog.targets.get("example", set()))
        self.assertIn(
            MODULE.Target("db", "scaling", "example", "crates/db/examples/scaling.rs"),
            self.catalog.db_targets,
        )
        self.assertEqual(
            self.catalog.required_features[("db", "example", "scaling")],
            ("diagnostics",),
        )
        for separator in (" ", "="):
            with self.subTest(separator=separator):
                self.assertEqual(
                    self.validate(f"cargo run -p db --example{separator}scaling"), [],
                )
                self.assertEqual(
                    self.validate(f"cargo run -p db --example{separator}missing"),
                    ["workflow.yml: unknown Cargo example target 'missing'"],
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

    def test_feature_gated_tests_must_run_by_name(self):
        catalog = MODULE.Catalog()
        catalog.required_features.update({
            ("db", "test", "default_contracts"): (),
            ("db", "test", "listed_contracts"): ("production-coverage",),
            ("db", "test", "unlisted_contracts"): ("index-lifecycle-testing",),
            ("db", "test", "filtered_contracts"): ("index-lifecycle-testing",),
            ("db", "test", "scale_soak"): ("production-scale",),
        })
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / ".github" / "workflows"
            workflows.mkdir(parents=True)
            (workflows / "ci.yml").write_text(
                "steps:\n  - run: >-\n      cargo test -p db\n"
                "      --features production-coverage --test listed_contracts\n"
                "  - run: cargo test -p db --test filtered_contracts one_test -- --exact\n"
                "  - run: cargo clippy -p db --test unlisted_contracts\n"
            )
            self.assertEqual(
                MODULE.validate_feature_gated_tests_run(root, catalog),
                [
                    "db test target 'filtered_contracts' requires "
                    "index-lifecycle-testing but no workflow runs the whole target with --test",
                    "db test target 'unlisted_contracts' requires "
                    "index-lifecycle-testing but no workflow runs the whole target with --test",
                ],
            )

    def test_whole_target_runs_exclude_filters_and_non_test_commands(self):
        runs = MODULE.runs_whole_test_targets
        self.assertTrue(runs("cargo test -p db --features a,b --test t"))
        self.assertTrue(runs("cargo test --release -p db --test t -- --nocapture --test-threads=1"))
        self.assertTrue(
            runs("cargo nextest run --profile ci -p db --test t -E 'kind(test) or test(/x/)'")
        )
        self.assertTrue(runs("cargo test -p db --features ${{ matrix.features }} --test t"))
        self.assertFalse(runs("cargo test -p db --test t some_test"))
        self.assertFalse(runs("cargo test -p db --test t -- --exact some_test"))
        self.assertFalse(runs("cargo test -p db --test t -- --skip slow"))
        self.assertFalse(runs("cargo nextest run -p db --test t -E 'test(/x/)'"))
        self.assertFalse(runs("cargo test -p db --no-run --test t"))
        self.assertFalse(runs("cargo clippy -p db --test t -- -D warnings"))

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
