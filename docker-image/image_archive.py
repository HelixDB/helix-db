#!/usr/bin/env python3
"""Inspect a Docker image archive without loading or executing it."""

import argparse
import json
import re
import stat
import sys
import tarfile
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple


EXPECTED_ENTRYPOINT = ["/bin/helix-server"]
EXPECTED_ENV = {
    "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    "HELIX_HTTP_ADDR": "0.0.0.0:8080",
    "HELIX_GRPC_ADDR": "127.0.0.1:8081",
    "DB_PATH": "db/",
    "RUST_LOG": "server=info,db=info,slatedb=info",
}
ALLOWED_INHERITED_ENV_KEYS = {"PATH"}
EXPECTED_LABELS = {
    "org.opencontainers.image.title": "helixdb",
    "org.opencontainers.image.description": "HelixDB single-process database server.",
    "org.opencontainers.image.licenses": "Apache-2.0",
    "org.opencontainers.image.source": "https://github.com/HelixDB/helix-db",
    "org.opencontainers.image.base.name": "gcr.io/distroless/cc-debian12:nonroot",
}
RUNTIME_OWNED_DIRECTORIES = ("var/lib/helix", "var/cache/helix")
REQUIRED_PATHS = {"bin/helix-server", "tmp", *RUNTIME_OWNED_DIRECTORIES}
FORBIDDEN_LEGACY_PATHS = {
    "bin/bash",
    "bin/gateway",
    "bin/grpcurl",
    "bin/hyperscale",
    "bin/sh",
    "bin/tini",
    "proto/gateway.protoset",
    "usr/bin/bash",
    "usr/bin/sh",
}

ALLOWED_SECRET_ENV_NAMES = {"SSL_CERT_FILE"}
FORBIDDEN_ENV_NAME_PATTERNS = [
    re.compile(
        r"(^|_)(SECRET|TOKEN|PASSWORD|PASS|CREDENTIAL|API_KEY|API_KEYS|ACCESS_KEY)(_|$)",
        re.IGNORECASE,
    )
]
FORBIDDEN_ENV_VALUE_PATTERNS = [
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"ASIA[0-9A-Z]{16}"),
    re.compile(r"aws_secret_access_key", re.IGNORECASE),
    re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"xox[baprs]-[A-Za-z0-9-]+"),
]
FORBIDDEN_PATH_PATTERNS = [
    re.compile(r"(^|/)\.env($|\.)", re.IGNORECASE),
    re.compile(r"(^|/)\.aws/credentials$", re.IGNORECASE),
    re.compile(r"(^|/)\.docker/config\.json$", re.IGNORECASE),
    re.compile(r"(^|/)\.npmrc$", re.IGNORECASE),
    re.compile(r"(^|/)\.netrc$", re.IGNORECASE),
    re.compile(r"(^|/)(id_rsa|id_dsa|id_ed25519)(\.pub)?$", re.IGNORECASE),
    re.compile(r"(^|/).+\.(pem|p12|pfx|key)$", re.IGNORECASE),
    re.compile(r"(^|/)(credentials?|secrets?)(/|$)", re.IGNORECASE),
]
TEXT_SECRET_PATTERNS = [
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"aws_secret_access_key", re.IGNORECASE),
    re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"xox[baprs]-[A-Za-z0-9-]+"),
    re.compile(
        r"(?i)(api[-_ ]?key|secret|token|password)\s*[:=]\s*['\"]?[A-Za-z0-9/+=._-]{8,}"
    ),
]
TEXT_EXTENSIONS = {
    ".conf",
    ".cfg",
    ".env",
    ".ini",
    ".json",
    ".md",
    ".sh",
    ".text",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
MAX_TEXT_SCAN_BYTES = 1024 * 1024


class ImageArchiveError(ValueError):
    """The archive violates the HelixDB image contract."""


def _normalize(path: str) -> str:
    normalized = path[2:] if path.startswith("./") else path
    return normalized.rstrip("/")


def _read_json_member(archive: tarfile.TarFile, name: str) -> object:
    try:
        extracted = archive.extractfile(name)
    except KeyError as error:
        raise ImageArchiveError("missing archive member: {}".format(name)) from error
    if extracted is None:
        raise ImageArchiveError("missing archive member: {}".format(name))
    try:
        return json.load(extracted)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ImageArchiveError("invalid JSON in {}: {}".format(name, error)) from error


def _manifest_and_config(
    archive: tarfile.TarFile,
) -> Tuple[Dict[str, object], Dict[str, object]]:
    manifest = _read_json_member(archive, "manifest.json")
    if not isinstance(manifest, list) or len(manifest) != 1:
        count = len(manifest) if isinstance(manifest, list) else "non-list"
        raise ImageArchiveError(
            "expected one image manifest entry, found {}".format(count)
        )
    entry = manifest[0]
    if not isinstance(entry, dict):
        raise ImageArchiveError("image manifest entry is not an object")
    config_name = entry.get("Config")
    if not isinstance(config_name, str):
        raise ImageArchiveError("image manifest has no config path")
    config = _read_json_member(archive, config_name)
    if not isinstance(config, dict):
        raise ImageArchiveError("image config is not an object")
    return entry, config


def _environment_map(entries: object) -> Dict[str, str]:
    if entries is None:
        return {}
    if not isinstance(entries, list):
        raise ImageArchiveError("image environment is not a list")
    environment = {}
    for entry in entries:
        if not isinstance(entry, str) or "=" not in entry:
            raise ImageArchiveError("malformed environment entry: {!r}".format(entry))
        key, value = entry.split("=", 1)
        environment[key] = value
    return environment


def _layer_names(entry: Dict[str, object]) -> Iterable[str]:
    layers = entry.get("Layers")
    if not isinstance(layers, list) or not all(isinstance(layer, str) for layer in layers):
        raise ImageArchiveError("image manifest layers are malformed")
    return layers


def inspect_archive(image_path: Path, expected_image_reference: Optional[str] = None) -> None:
    """Validate image metadata, filesystem contents, and binary linkage."""

    if not image_path.is_file():
        raise ImageArchiveError("image archive does not exist: {}".format(image_path))

    with tarfile.open(str(image_path), "r:*") as archive:
        entry, config = _manifest_and_config(archive)
        repository_tags = entry.get("RepoTags") or []
        if expected_image_reference is not None and expected_image_reference not in repository_tags:
            raise ImageArchiveError(
                "archive does not contain expected image reference {!r}: {}".format(
                    expected_image_reference, repository_tags
                )
            )

        image_config = config.get("config")
        if not isinstance(image_config, dict):
            raise ImageArchiveError("image config has no runtime config object")
        architecture = config.get("architecture")
        expected_glibc_loader = {
            "arm64": b"/lib/ld-linux-aarch64.so.1",
            "amd64": b"/lib64/ld-linux-x86-64.so.2",
        }.get(architecture)
        if expected_glibc_loader is None:
            raise ImageArchiveError("unsupported image architecture: {}".format(architecture))

        if image_config.get("Entrypoint") != EXPECTED_ENTRYPOINT:
            raise ImageArchiveError(
                "unexpected entrypoint: {}".format(image_config.get("Entrypoint"))
            )
        if image_config.get("WorkingDir") != "/home/nonroot":
            raise ImageArchiveError(
                "unexpected working directory: {}".format(image_config.get("WorkingDir"))
            )
        if image_config.get("User") not in ("65532", "65532:65532", "nonroot"):
            raise ImageArchiveError(
                "unexpected runtime user: {}".format(image_config.get("User"))
            )
        if image_config.get("StopSignal") != "SIGTERM":
            raise ImageArchiveError(
                "unexpected stop signal: {}".format(image_config.get("StopSignal"))
            )
        if image_config.get("ExposedPorts") != {"8080/tcp": {}}:
            raise ImageArchiveError(
                "unexpected exposed ports: {}".format(image_config.get("ExposedPorts"))
            )

        environment = _environment_map(image_config.get("Env"))
        missing_environment = sorted(set(EXPECTED_ENV) - set(environment))
        if missing_environment:
            raise ImageArchiveError(
                "missing expected environment keys: {}".format(", ".join(missing_environment))
            )
        unexpected_environment = sorted(
            set(environment) - set(EXPECTED_ENV) - ALLOWED_INHERITED_ENV_KEYS
        )
        if unexpected_environment:
            raise ImageArchiveError(
                "unexpected baked environment keys: {}".format(
                    ", ".join(unexpected_environment)
                )
            )
        for key, expected in EXPECTED_ENV.items():
            if environment.get(key) != expected:
                raise ImageArchiveError(
                    "unexpected environment {}: {!r}".format(key, environment.get(key))
                )

        labels = image_config.get("Labels") or {}
        if not isinstance(labels, dict):
            raise ImageArchiveError("image labels are not an object")
        for key, expected in EXPECTED_LABELS.items():
            if labels.get(key) != expected:
                raise ImageArchiveError(
                    "unexpected label {}: {!r}".format(key, labels.get(key))
                )

        layer_paths: Set[str] = set()
        directory_metadata: Dict[str, tarfile.TarInfo] = {}
        server_binary = None
        for layer_name in _layer_names(entry):
            try:
                layer_file = archive.extractfile(layer_name)
            except KeyError as error:
                raise ImageArchiveError("missing image layer: {}".format(layer_name)) from error
            if layer_file is None:
                raise ImageArchiveError("missing image layer: {}".format(layer_name))
            with tarfile.open(fileobj=layer_file, mode="r:*") as layer:
                for member in layer.getmembers():
                    member_name = _normalize(member.name)
                    layer_paths.add(member_name)
                    if member.isdir() and member_name in {"tmp", *RUNTIME_OWNED_DIRECTORIES}:
                        directory_metadata[member_name] = member
                    if member_name == "bin/helix-server" and member.isfile():
                        extracted = layer.extractfile(member)
                        if extracted is not None:
                            server_binary = extracted.read()

        missing_paths = sorted(REQUIRED_PATHS - layer_paths)
        if missing_paths:
            raise ImageArchiveError(
                "missing required image paths: {}".format(", ".join(missing_paths))
            )
        legacy_paths = sorted(FORBIDDEN_LEGACY_PATHS & layer_paths)
        if legacy_paths:
            raise ImageArchiveError(
                "legacy runtime paths remain in image: {}".format(", ".join(legacy_paths))
            )

        tmp_metadata = directory_metadata.get("tmp")
        if tmp_metadata is None or stat.S_IMODE(tmp_metadata.mode) != 0o1777:
            raise ImageArchiveError("/tmp is not a mode-1777 directory")
        for owned_path in RUNTIME_OWNED_DIRECTORIES:
            owned_metadata = directory_metadata.get(owned_path)
            if owned_metadata is None or (owned_metadata.uid, owned_metadata.gid) != (
                65532,
                65532,
            ):
                raise ImageArchiveError(
                    "/{} is not owned by the non-root runtime user".format(owned_path)
                )

        if server_binary is None:
            raise ImageArchiveError("could not read bin/helix-server from the image")
        if expected_glibc_loader not in server_binary:
            raise ImageArchiveError(
                "helix-server does not use the expected GNU libc loader for {}".format(
                    architecture
                )
            )
        if b"musl" in server_binary.lower():
            raise ImageArchiveError("helix-server contains a musl linkage marker")
        if b"/v2/query" not in server_binary:
            raise ImageArchiveError("helix-server does not contain the /v2/query endpoint")


def _looks_like_text(member_name: str, data: bytes) -> bool:
    if not data or len(data) > MAX_TEXT_SCAN_BYTES or b"\x00" in data:
        return False
    if Path(member_name).suffix.lower() in TEXT_EXTENSIONS:
        return True
    printable = sum(1 for byte in data if 32 <= byte <= 126 or byte in {9, 10, 13})
    return printable / len(data) > 0.95


def _scan_text_payload(member_name: str, data: bytes) -> None:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return
    for pattern in TEXT_SECRET_PATTERNS:
        match = pattern.search(text)
        if match:
            raise ImageArchiveError(
                "potential secret pattern {!r} found in {}: {!r}".format(
                    pattern.pattern, member_name, match.group(0)
                )
            )


def scan_archive_for_secrets(image_path: Path) -> None:
    """Reject credential-like environment, path, and text-layer content."""

    if not image_path.is_file():
        raise ImageArchiveError("image archive does not exist: {}".format(image_path))

    with tarfile.open(str(image_path), "r:*") as archive:
        entry, config = _manifest_and_config(archive)
        image_config = config.get("config")
        if not isinstance(image_config, dict):
            raise ImageArchiveError("image config has no runtime config object")
        environment = _environment_map(image_config.get("Env"))
        for key, value in environment.items():
            if key in ALLOWED_SECRET_ENV_NAMES:
                continue
            for pattern in FORBIDDEN_ENV_NAME_PATTERNS:
                if pattern.search(key):
                    raise ImageArchiveError(
                        "forbidden environment variable in image config: {}".format(key)
                    )
            for pattern in FORBIDDEN_ENV_VALUE_PATTERNS:
                if pattern.search(value):
                    raise ImageArchiveError(
                        "potential secret environment value for {}: {!r} matched".format(
                            key, pattern.pattern
                        )
                    )

        for layer_name in _layer_names(entry):
            try:
                layer_file = archive.extractfile(layer_name)
            except KeyError as error:
                raise ImageArchiveError("missing image layer: {}".format(layer_name)) from error
            if layer_file is None:
                raise ImageArchiveError("missing image layer: {}".format(layer_name))
            with tarfile.open(fileobj=layer_file, mode="r:*") as layer:
                for member in layer.getmembers():
                    member_name = _normalize(member.name)
                    for pattern in FORBIDDEN_PATH_PATTERNS:
                        if pattern.search(member_name):
                            raise ImageArchiveError(
                                "forbidden path pattern in image: {}".format(member_name)
                            )
                    if not member.isfile():
                        continue
                    extracted = layer.extractfile(member)
                    if extracted is None:
                        continue
                    data = extracted.read()
                    if _looks_like_text(member_name, data):
                        _scan_text_payload(member_name, data)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect_parser = commands.add_parser("inspect", help="validate image metadata and contents")
    inspect_parser.add_argument("archive", type=Path)
    inspect_parser.add_argument("--expected-image")
    scan_parser = commands.add_parser("scan", help="scan image metadata and layers for secrets")
    scan_parser.add_argument("archive", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "inspect":
            inspect_archive(args.archive, args.expected_image)
            print("Image inspection passed for {}".format(args.archive))
        else:
            scan_archive_for_secrets(args.archive)
            print("Secret scan passed for {}".format(args.archive))
    except (ImageArchiveError, tarfile.TarError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
