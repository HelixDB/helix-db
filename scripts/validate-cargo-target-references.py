#!/usr/bin/env python3
"""Validate Cargo package and target references in repository automation."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


REFERENCE = re.compile(
    r"(?<![A-Za-z0-9_-])(?P<flag>--package|--test|--bench|--bin|-p)"
    r"(?:=|\s+)(?P<name>[A-Za-z0-9_.-]+)"
)
INVENTORY_ROW = re.compile(
    r"^\| `(?P<name>[^`]+)` \| `(?P<kind>[^`]+)` \| `(?P<source>[^`]+)` \|"
)
MATRIX_FUZZ = re.compile(
    r"-\s+fuzz_dir:\s*(?P<directory>[^\s]+)\s+"
    r"target:\s*(?P<target>[A-Za-z0-9_.-]+)",
    re.MULTILINE,
)
MATRIX_TEST = re.compile(r"^\s+test:\s*(?P<test>[A-Za-z0-9_:.-]+)\s*$", re.MULTILINE)
FLAG_KIND = {"--test": "test", "--bench": "bench", "--bin": "bin"}
TARGET_KIND_ORDER = ("lib", "test", "bench", "bin")
CODEC_OWNERS = {
    "legacy/tenant_envelope.rs": "struct LegacyTenantEnvelope",
    "legacy/edge_property_pair.rs": "struct LegacyEdgePropertyPairKey",
    "legacy/index_catalog.rs": "enum LegacyDynamicIndexCatalogEntry",
    "legacy/text/storage_keys.rs": "enum LegacyTextMetadataElement",
    "legacy/text/manifest.rs": "enum LegacyTextManifestError",
    "legacy/text/live_state.rs": "struct LegacyTextLiveStateError",
    "legacy/text/version_counter.rs": "enum LegacyTextVersionCounterError",
    "legacy/vector/metadata.rs": "struct LegacyVectorIndexMetadata",
    "legacy/vector/transaction_guard.rs": "struct LegacyVectorTxnGuardKey",
    "keys/global.rs": "enum GlobalKey",
    "keys/lifecycle.rs": "struct IndexRecordKey",
    "keys/indexes/secondary.rs": "struct SecondaryEqualityEntryKey",
    "keys/indexes/text.rs": "struct TextManifestRootKey",
    "keys/indexes/vector/metadata.rs": "struct VectorPartitionMappingKey",
    "values/global.rs": "fn encode_metadata_value",
    "values/lifecycle/index_record.rs": "fn encode_index_record",
    "values/indexes/equality.rs": "struct SecondaryEqualityBitmapValue",
    "values/indexes/range.rs": "fn encode_entry",
    "values/indexes/text.rs": "fn encode_manifest_root",
    "values/indexes/vector/generation.rs": "fn encode_partition_mapping",
}
V2_SOURCE_FILES = {
    "mod.rs",
    "legacy/mod.rs",
    "legacy/tenant_envelope.rs",
    "legacy/edge_property_pair.rs",
    "legacy/index_catalog.rs",
    "legacy/text/mod.rs",
    "legacy/text/storage_keys.rs",
    "legacy/text/manifest.rs",
    "legacy/text/live_state.rs",
    "legacy/text/version_counter.rs",
    "legacy/vector/mod.rs",
    "legacy/vector/metadata.rs",
    "legacy/vector/transaction_guard.rs",
    "keys/mod.rs",
    "keys/codec.rs",
    "keys/scope.rs",
    "keys/data.rs",
    "keys/managed_index.rs",
    "keys/global.rs",
    "keys/lifecycle.rs",
    "keys/graph.rs",
    "keys/metadata.rs",
    "keys/indexes/mod.rs",
    "keys/indexes/secondary.rs",
    "keys/indexes/direction.rs",
    "keys/indexes/prefix.rs",
    "keys/indexes/property.rs",
    "keys/indexes/label.rs",
    "keys/indexes/text.rs",
    "keys/indexes/equality/mod.rs",
    "keys/indexes/equality/node.rs",
    "keys/indexes/equality/edge.rs",
    "keys/indexes/equality/scans.rs",
    "keys/indexes/range/mod.rs",
    "keys/indexes/range/node.rs",
    "keys/indexes/range/edge.rs",
    "keys/indexes/range/scans.rs",
    "keys/indexes/vector/mod.rs",
    "keys/indexes/vector/metadata.rs",
    "keys/indexes/vector/layer0.rs",
    "keys/indexes/vector/items.rs",
    "keys/indexes/vector/entry_candidates.rs",
    "keys/indexes/vector/simhash.rs",
    "keys/indexes/vector/upper_layers.rs",
    "keys/indexes/vector/reverse_edges.rs",
    "keys/indexes/vector/storage_prefixes.rs",
    "values/mod.rs",
    "values/codec.rs",
    "values/global.rs",
    "values/adjacency.rs",
    "values/edge_endpoints.rs",
    "values/id_allocation.rs",
    "values/property/mod.rs",
    "values/property/row.rs",
    "values/property/property.rs",
    "values/property/property_value.rs",
    "values/property/canonical_number.rs",
    "values/property/equality_index_value.rs",
    "values/property/range_index_value.rs",
    "values/lifecycle/mod.rs",
    "values/lifecycle/common.rs",
    "values/lifecycle/entity_state.rs",
    "values/lifecycle/index_record.rs",
    "values/lifecycle/operation_record.rs",
    "values/indexes/mod.rs",
    "values/indexes/secondary_entry.rs",
    "values/indexes/equality.rs",
    "values/indexes/range.rs",
    "values/indexes/text.rs",
    "values/indexes/vector/mod.rs",
    "values/indexes/vector/generation.rs",
    "values/indexes/vector/layer0.rs",
    "values/indexes/vector/entry_candidate.rs",
    "values/indexes/vector/item.rs",
    "values/indexes/vector/markers.rs",
    "values/indexes/vector/metadata.rs",
    "values/indexes/vector/neighbors.rs",
    "values/indexes/vector/simhash.rs",
}
V1_SOURCE_FILES = {
    "mod.rs",
    "keys/mod.rs",
    "indexes/mod.rs",
    "property/mod.rs",
    "values/mod.rs",
}
RAW_RUNTIME_CODEC_EXEMPTIONS = {
    # Canonical hash preimages are model data, not stored database values.
    "crates/db/src/index_lifecycle/work.rs",
    # V3 tenant framing remains private to the blocking V3-to-V4 migration.
    "crates/db/src/index_lifecycle/tenant_envelope_migration.rs",
}


@dataclass(frozen=True)
class Target:
    package: str
    name: str
    kind: str
    source: str


@dataclass
class Catalog:
    packages: set[str] = field(default_factory=set)
    targets: dict[str, set[str]] = field(
        default_factory=lambda: {kind: set() for kind in TARGET_KIND_ORDER}
    )
    target_owners: dict[tuple[str, str], set[str]] = field(default_factory=dict)
    required_features: dict[tuple[str, str, str], tuple[str, ...]] = field(
        default_factory=dict
    )
    db_targets: set[Target] = field(default_factory=set)

    def add_metadata(self, metadata: dict[str, object], root: Path) -> None:
        for package in metadata["packages"]:
            package_name = package["name"]
            self.packages.add(package_name)
            for raw_target in package["targets"]:
                kinds = set(raw_target["kind"])
                kind = next((candidate for candidate in TARGET_KIND_ORDER if candidate in kinds), None)
                if kind is None:
                    continue
                name = raw_target["name"]
                self.targets[kind].add(name)
                self.target_owners.setdefault((kind, name), set()).add(package_name)
                self.required_features[(package_name, kind, name)] = tuple(
                    raw_target.get("required-features", ())
                )
                if package_name == "db":
                    source_path = Path(raw_target["src_path"])
                    try:
                        source = source_path.relative_to(root).as_posix()
                    except ValueError:
                        source = source_path.as_posix()
                    self.db_targets.add(Target(package_name, name, kind, source))


@dataclass(frozen=True)
class TestFilter:
    source: str
    package: str
    target: str
    features: tuple[str, ...]
    name: str


def run(command: list[str], cwd: Path) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{' '.join(command)} failed:\n{completed.stdout}{completed.stderr}"
        )
    return completed.stdout


def workspace_manifests(root: Path) -> list[Path]:
    relative_paths = (
        "Cargo.toml",
        "bindings/uniffi-bindgen/Cargo.toml",
        "crates/db-testkit/fuzz/Cargo.toml",
        "crates/db/fuzz/Cargo.toml",
    )
    return [root / relative_path for relative_path in relative_paths]


def load_catalog(root: Path) -> Catalog:
    catalog = Catalog()
    for manifest in workspace_manifests(root):
        raw = run(
            [
                "cargo",
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--manifest-path",
                str(manifest),
            ],
            root,
        )
        catalog.add_metadata(json.loads(raw), root)
    return catalog


def yaml_run_commands(text: str) -> list[str]:
    lines = text.splitlines()
    commands = []
    line_index = 0
    while line_index < len(lines):
        match = re.match(
            r"^(?P<indent>\s*)(?:-\s+)?run:\s*(?P<body>.*)$", lines[line_index]
        )
        if match is None:
            line_index += 1
            continue
        body = match.group("body").strip()
        if body not in {"|", "|-", ">", ">-"}:
            commands.append(body)
            line_index += 1
            continue
        indentation = len(match.group("indent"))
        block = []
        line_index += 1
        while line_index < len(lines):
            line = lines[line_index]
            if line.strip() and len(line) - len(line.lstrip()) <= indentation:
                break
            block.append(line.strip())
            line_index += 1
        if body in {"|", "|-"}:
            commands.extend(shell_commands("\n".join(block)))
        else:
            commands.append(" ".join(block))
    return commands


def shell_commands(text: str) -> list[str]:
    joined = re.sub(r"\\\s*\n\s*", " ", text)
    return [line.strip() for line in joined.splitlines() if "cargo " in line]


def automation_files(root: Path) -> list[Path]:
    workflows = list((root / ".github" / "workflows").glob("*.yml"))
    workflows.extend((root / ".github" / "workflows").glob("*.yaml"))
    scripts = [
        path
        for path in (root / "scripts").iterdir()
        if path.suffix in {".sh", ".py"}
        and path.name != "validate-cargo-target-references.py"
    ]
    return sorted(workflows + scripts)


def commands(path: Path) -> list[str]:
    text = path.read_text()
    if path.suffix in {".yml", ".yaml"}:
        return yaml_run_commands(text)
    return shell_commands(text)


def validate_references(
    root: Path, catalog: Catalog, files: Iterable[Path]
) -> list[str]:
    errors = []
    for path in files:
        for command in commands(path):
            if "cargo " not in command:
                continue
            for match in REFERENCE.finditer(command):
                flag = match.group("flag")
                name = match.group("name")
                if flag in {"-p", "--package"}:
                    if name not in catalog.packages:
                        errors.append(f"{path.relative_to(root)}: unknown Cargo package {name!r}")
                    continue
                kind = FLAG_KIND[flag]
                if name not in catalog.targets[kind]:
                    errors.append(
                        f"{path.relative_to(root)}: unknown Cargo {kind} target {name!r}"
                    )
    return errors


def validate_fuzz_matrices(root: Path, catalog: Catalog) -> list[str]:
    errors = []
    for path in (root / ".github" / "workflows").glob("*.yml"):
        text = path.read_text()
        if "cargo fuzz run ${{ matrix.target }}" not in text:
            continue
        for match in MATRIX_FUZZ.finditer(text):
            directory = match.group("directory")
            target = match.group("target")
            manifest = root / directory / "Cargo.toml"
            if not manifest.is_file():
                errors.append(
                    f"{path.relative_to(root)}: fuzz workspace {directory!r} has no Cargo.toml"
                )
            if target not in catalog.targets["bin"]:
                errors.append(
                    f"{path.relative_to(root)}: unknown Cargo fuzz target {target!r}"
                )
    return errors


def parse_inventory(text: str) -> set[tuple[str, str, str]]:
    rows = set()
    for line in text.splitlines():
        match = INVENTORY_ROW.match(line)
        if match is not None:
            rows.add((match.group("name"), match.group("kind"), match.group("source")))
    return rows


def validate_inventory(root: Path, catalog: Catalog) -> list[str]:
    path = root / "crates" / "db" / "tests" / "TEST_TARGET_INVENTORY.md"
    documented = parse_inventory(path.read_text())
    actual = {(target.name, target.kind, target.source) for target in catalog.db_targets}
    errors = []
    for missing in sorted(actual - documented):
        errors.append(f"{path.relative_to(root)}: missing Cargo target {missing!r}")
    for stale in sorted(documented - actual):
        errors.append(f"{path.relative_to(root)}: stale Cargo target {stale!r}")
    return errors


def command_features(command: str) -> tuple[str, ...]:
    match = re.search(r"--features(?:=|\s+)(?P<features>[A-Za-z0-9_,.-]+)", command)
    if match is None:
        return ()
    return tuple(filter(None, match.group("features").split(",")))


def command_package(command: str, catalog: Catalog, target: str) -> str | None:
    matches = list(re.finditer(r"(?:-p|--package)(?:=|\s+)([A-Za-z0-9_.-]+)", command))
    if matches:
        return matches[-1].group(1)
    owners = catalog.target_owners.get(("test", target), set())
    return next(iter(owners)) if len(owners) == 1 else None


def referenced_test_filters(root: Path, catalog: Catalog) -> list[TestFilter]:
    filters = set()
    for path in automation_files(root):
        text = path.read_text()
        for command in commands(path):
            target_match = re.search(
                r"--test(?:=|\s+)(?P<target>[A-Za-z0-9_.-]+)", command
            )
            if target_match is None or "cargo test" not in command:
                continue
            target = target_match.group("target")
            package = command_package(command, catalog, target)
            if package is None:
                continue
            features = command_features(command) or catalog.required_features.get(
                (package, "test", target), ()
            )
            tail = command[target_match.end() :]
            filter_match = re.match(r"\s+(?!-)(?P<filter>[A-Za-z0-9_:.-]+)", tail)
            if filter_match is not None:
                filters.add(
                    TestFilter(
                        path.relative_to(root).as_posix(),
                        package,
                        target,
                        features,
                        filter_match.group("filter"),
                    )
                )
            if "${{ matrix.test }}" in tail:
                for name in MATRIX_TEST.findall(text):
                    filters.add(
                        TestFilter(
                            path.relative_to(root).as_posix(),
                            package,
                            target,
                            features,
                            name,
                        )
                    )
    return sorted(
        filters,
        key=lambda item: (item.package, item.target, item.features, item.name, item.source),
    )


def listed_tests(root: Path, group: tuple[str, str, tuple[str, ...]]) -> set[str]:
    package, target, features = group
    command = ["cargo", "test", "-p", package]
    if features:
        command.extend(["--features", ",".join(features)])
    command.extend(["--test", target, "--", "--list"])
    output = run(command, root)
    return {
        line.rsplit(": ", 1)[0]
        for line in output.splitlines()
        if line.endswith(": test")
    }


def validate_test_filters(root: Path, catalog: Catalog) -> list[str]:
    references = referenced_test_filters(root, catalog)
    listings = {}
    errors = []
    for reference in references:
        group = (reference.package, reference.target, reference.features)
        if group not in listings:
            listings[group] = listed_tests(root, group)
        if reference.name not in listings[group]:
            errors.append(
                f"{reference.source}: unknown exact test filter {reference.name!r} "
                f"for {reference.package}:{reference.target}"
            )
    return errors


def forbidden_codec_source(path: str, text: str) -> list[str]:
    errors = []
    for identifier in ("WorkValue", "decode_work_value"):
        if re.search(rf"\b{identifier}\b", text):
            errors.append(f"{path}: forbidden obsolete identifier {identifier!r}")
    if re.search(r"encoding::v1::(?:keys|values)::index_v2\b", text):
        errors.append(f"{path}: forbidden V1 index_v2 codec import")
    if re.search(r"\bmod\s+index_v2\s*;", text):
        errors.append(f"{path}: forbidden index_v2 runtime module")
    if (
        path.startswith("crates/db/src/index_lifecycle/")
        and path not in RAW_RUNTIME_CODEC_EXEMPTIONS
        and re.search(
            r"\b(?:BufMut|BytesMut|ValueEncoder|ValueDecoder)\b"
            r"|\.put_(?:u8|u16|u32|u64|f32|slice)\s*\(",
            text,
        )
    ):
        errors.append(f"{path}: raw managed-index serialization must use encoding::v2")
    if path.startswith("crates/db/src/encoding/v2/values/lifecycle/"):
        for dependency in (
            "decode_secondary_entry",
            "encode_secondary_entry",
            "values::indexes",
        ):
            if dependency in text:
                errors.append(
                    f"{path}: lifecycle values depend on index dispatch {dependency!r}"
                )
    return errors


def validate_codec_architecture(root: Path) -> list[str]:
    errors = []
    source_root = root / "crates" / "db" / "src"
    for path in source_root.rglob("*.rs"):
        errors.extend(
            forbidden_codec_source(path.relative_to(root).as_posix(), path.read_text())
        )
    v2 = source_root / "encoding" / "v2"
    actual_v2_files = {
        path.relative_to(v2).as_posix() for path in v2.rglob("*.rs")
    }
    for relative_path in sorted(V2_SOURCE_FILES - actual_v2_files):
        errors.append(f"crates/db/src/encoding/v2/{relative_path}: required V2 module is missing")
    for relative_path in sorted(actual_v2_files - V2_SOURCE_FILES):
        errors.append(f"crates/db/src/encoding/v2/{relative_path}: unexpected V2 module")
    for path in v2.rglob("*.rs"):
        text = path.read_text()
        if "v1::" in text or "encoding/v1" in text:
            errors.append(
                f"{path.relative_to(root)}: V2 source must not depend on V1"
            )
        if path.name != "mod.rs" and (path.parent / path.stem).is_dir():
            errors.append(
                f"{path.relative_to(root)}: use {path.stem}/mod.rs for a module with children"
            )

    legacy = v2 / "legacy"
    legacy_implementation = re.compile(
        r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:fn|struct|enum|trait|impl|const|static|type)\b",
        re.MULTILINE,
    )
    for path in legacy.rglob("mod.rs"):
        if legacy_implementation.search(path.read_text()):
            errors.append(
                f"{path.relative_to(root)}: legacy mod.rs must contain declarations and re-exports only"
            )

    legacy_codec_declaration = re.compile(
        r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:struct|enum)\s+"
        r"(?:LegacyTenantEnvelope|LegacyEdgePropertyPairKey|LegacyDynamicIndexCatalogEntry|"
        r"LegacyTextMetadataElement|LegacyTextManifestError|LegacyTextLiveStateError|"
        r"LegacyTextVersionCounterError|LegacyVectorIndexMetadata|LegacyVectorTxnGuardKey)\b"
        r"|^\s*const\s+(?:DYNAMIC_INDEX_PREFIX|TEXT_INDEX_MANIFEST_PREFIX|ACTIVE_TXN_GUARD)\b",
        re.MULTILINE,
    )
    for path in source_root.rglob("*.rs"):
        if path.is_relative_to(legacy):
            continue
        if legacy_codec_declaration.search(path.read_text()):
            errors.append(
                f"{path.relative_to(root)}: legacy codec implementation must live in encoding/v2/legacy"
            )

    fixture_encoders = {
        "encode_for_contract",
        "encode_row_for_contract",
        "encode_legacy_metadata_for_contract",
        "encode_active_txn_guard",
    }
    for path in legacy.rglob("*.rs"):
        lines = path.read_text().splitlines()
        for line_number, line in enumerate(lines):
            if not any(re.search(rf"\bfn\s+{name}\b", line) for name in fixture_encoders):
                continue
            previous = line_number - 1
            while previous >= 0 and not lines[previous].strip():
                previous -= 1
            gated = False
            while previous >= 0 and lines[previous].strip().startswith("#["):
                gated |= lines[previous].strip().startswith("#[cfg(")
                previous -= 1
            if not gated:
                errors.append(
                    f"{path.relative_to(root)}:{line_number + 1}: legacy fixture encoder is not compile-gated"
                )

    def legacy_dependency_allowed(relative_path: str) -> bool:
        return (
            relative_path.startswith("crates/db/src/encoding/v2/legacy/")
            or relative_path.startswith("crates/db/src/encoding/v1/")
            or relative_path in {
                "crates/db/src/encoding/v2/keys/data.rs",
                "crates/db/src/encoding/v2/keys/indexes/vector/mod.rs",
                "crates/db/src/encoding/v2/keys/indexes/vector/storage_prefixes.rs",
                "crates/db/src/fuzzing.rs",
                "crates/db/src/index_lifecycle/cursor_contracts.rs",
                "crates/db/src/index_lifecycle/tenant_envelope_migration.rs",
                "crates/db/src/index_lifecycle/vector.rs",
                "crates/db/src/index_lifecycle/vector/driver.rs",
                "crates/db/src/index_lifecycle_testing.rs",
                "crates/db/src/migration_parity.rs",
                "crates/db/src/migrations.rs",
                "crates/db/src/search/mod.rs",
                "crates/db/src/search/text/mod.rs",
                "crates/db/src/search/vector/index.rs",
                "crates/db/src/search/vector/mod.rs",
                "crates/db/src/search/vector/storage.rs",
            }
            or relative_path.startswith("crates/db/src/migrations/")
            or relative_path.startswith("crates/db/tests/")
        )

    fixture_encoder_call = re.compile(
        r"\b(?:encode_for_contract|encode_row_for_contract|"
        r"encode_legacy_metadata_for_contract|encode_active_txn_guard)\s*\("
    )
    for path in source_root.rglob("*.rs"):
        relative_path = path.relative_to(root).as_posix()
        text = path.read_text()
        if "encoding::v2::legacy" in text and not legacy_dependency_allowed(relative_path):
            errors.append(f"{relative_path}: legacy dependency is outside the compatibility allowlist")
        if path.is_relative_to(legacy) or relative_path.startswith("crates/db/src/migrations/"):
            continue
        production_text = text.split("#[cfg(test)]\nmod tests", 1)[0]
        if fixture_encoder_call.search(production_text) and relative_path not in {
            "crates/db/src/index_lifecycle_testing.rs",
            "crates/db/src/index_lifecycle/tenant_envelope_migration.rs",
            "crates/db/src/migration_parity.rs",
            "crates/db/src/migrations.rs",
            "crates/db/src/search/text/mod.rs",
        }:
            errors.append(f"{relative_path}: production source calls a legacy fixture encoder")

    v1 = source_root / "encoding" / "v1"
    actual_v1_files = {
        path.relative_to(v1).as_posix() for path in v1.rglob("*.rs")
    }
    for relative_path in sorted(V1_SOURCE_FILES - actual_v1_files):
        errors.append(f"crates/db/src/encoding/v1/{relative_path}: required V1 facade is missing")
    for relative_path in sorted(actual_v1_files - V1_SOURCE_FILES):
        errors.append(f"crates/db/src/encoding/v1/{relative_path}: V1 implementation logic remains")
    implementation = re.compile(
        r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:fn|struct|enum|trait|impl|const|static|type)\b",
        re.MULTILINE,
    )
    public_facade_item = re.compile(r"^\s*pub(?:\([^)]*\))?\s+(?:mod|use)\b")
    for path in v1.rglob("*.rs"):
        text = path.read_text()
        if implementation.search(text):
            errors.append(f"{path.relative_to(root)}: V1 facade contains implementation logic")
        lines = text.splitlines()
        for line_number, line in enumerate(lines):
            if not public_facade_item.search(line):
                continue
            attributes = "\n".join(lines[max(0, line_number - 8) : line_number])
            if re.search(r"#\[deprecated\([\s\S]*?\)\]\s*$", attributes) is None:
                errors.append(
                    f"{path.relative_to(root)}:{line_number + 1}: V1 facade item is not deprecated"
                )

    encoding_root = source_root / "encoding" / "mod.rs"
    encoding_text = encoding_root.read_text()
    if not re.search(
        r'#\[deprecated\(note = "use encoding::v2"\)\]\s+pub mod v1;',
        encoding_text,
    ):
        errors.append(f"{encoding_root.relative_to(root)}: root V1 module is not deprecated")

    def v1_import_allowed(relative_path: str) -> bool:
        return (
            relative_path == "crates/db/src/migration_parity.rs"
            or relative_path == "crates/db/src/migrations.rs"
            or relative_path.startswith("crates/db/src/migrations/")
            or relative_path == "crates/db/tests/public_encoding_compatibility.rs"
            or relative_path == "crates/db/tests/production_support/v1_migration.rs"
            or relative_path.startswith("crates/db/tests/production_support/v1_migration/")
            or relative_path
            == "crates/db/tests/production_support/migration_text_rebuild.rs"
        )

    for path in (root / "crates").rglob("*.rs"):
        relative_path = path.relative_to(root).as_posix()
        if "encoding::v1" in path.read_text() and not v1_import_allowed(relative_path):
            errors.append(f"{relative_path}: V1 import is outside migration code")

    for relative_path, owner in CODEC_OWNERS.items():
        path = v2 / relative_path
        if not path.is_file():
            errors.append(f"{path.relative_to(root)}: required codec module is missing")
            continue
        if owner not in path.read_text():
            errors.append(
                f"{path.relative_to(root)}: codec module does not own {owner!r}"
            )
    for old_path in (
        source_root / "encoding" / "v1" / "keys" / "index_v2.rs",
        source_root / "encoding" / "v1" / "values" / "index_v2",
        source_root / "index_v2",
    ):
        if old_path.exists():
            errors.append(f"{old_path.relative_to(root)}: obsolete codec/runtime path remains")
    for module in (
        v2 / "keys" / "indexes" / "mod.rs",
        v2 / "values" / "indexes" / "mod.rs",
    ):
        text = module.read_text()
        for family in ("equality", "range", "text", "vector"):
            if re.search(rf"\bmod\s+{family}\s*;", text) is None:
                errors.append(
                    f"{module.relative_to(root)}: missing {family!r} codec module declaration"
                )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-test-filters", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    catalog = load_catalog(root)
    errors = []
    errors.extend(validate_references(root, catalog, automation_files(root)))
    errors.extend(validate_fuzz_matrices(root, catalog))
    errors.extend(validate_inventory(root, catalog))
    errors.extend(validate_codec_architecture(root))
    if not args.skip_test_filters:
        errors.extend(validate_test_filters(root, catalog))
    if errors:
        print("Cargo target-reference validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("Cargo references and managed-index codec boundaries are valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
