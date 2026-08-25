"""Validate license metadata and files in Python distribution archives."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from email.parser import BytesParser
from email.policy import default
from pathlib import Path, PurePosixPath
from typing import Callable

EXPECTED_EXPRESSION = "Apache-2.0"
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PACKAGE_ROOT.parents[1]
EXPECTED_LICENSE = (REPOSITORY_ROOT / "LICENSE").read_bytes()


def require_one(names: list[str], description: str, predicate: Callable[[str], bool]) -> str:
    matches = [name for name in names if predicate(name)]
    if len(matches) != 1:
        raise ValueError(f"expected one {description}, found {len(matches)}")
    return matches[0]


def validate_metadata(contents: bytes) -> None:
    """Validate the PEP 639 license contract in core package metadata."""

    metadata = BytesParser(policy=default).parsebytes(contents)
    expression = metadata.get("License-Expression")
    if expression != EXPECTED_EXPRESSION:
        raise ValueError(
            f"expected License-Expression {EXPECTED_EXPRESSION!r}, found {expression!r}"
        )

    classifiers = metadata.get_all("Classifier", [])
    license_classifiers = [value for value in classifiers if value.startswith("License ::")]
    if license_classifiers:
        raise ValueError(f"deprecated license classifiers found: {license_classifiers!r}")

    license_files = metadata.get_all("License-File", [])
    if "LICENSE" not in license_files:
        raise ValueError(f"expected License-File 'LICENSE', found {license_files!r}")


def validate_wheel(path: Path) -> None:
    """Validate one built wheel without extracting it to disk."""

    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        metadata_name = require_one(
            names,
            "wheel METADATA file",
            lambda name: name.endswith(".dist-info/METADATA"),
        )
        license_name = require_one(
            names,
            "wheel LICENSE file",
            lambda name: name.endswith(".dist-info/licenses/LICENSE"),
        )
        validate_metadata(archive.read(metadata_name))
        if archive.read(license_name) != EXPECTED_LICENSE:
            raise ValueError("packaged wheel LICENSE does not match sdks/python/LICENSE")


def validate_sdist(path: Path) -> None:
    """Validate one built source distribution without extracting it to disk."""

    with tarfile.open(path, mode="r:gz") as archive:
        names = archive.getnames()
        metadata_name = require_one(
            names,
            "source distribution PKG-INFO file",
            lambda name: len(PurePosixPath(name).parts) == 2 and name.endswith("/PKG-INFO"),
        )
        license_name = require_one(
            names,
            "source distribution LICENSE file",
            lambda name: len(PurePosixPath(name).parts) == 2 and name.endswith("/LICENSE"),
        )

        metadata_file = archive.extractfile(metadata_name)
        license_file = archive.extractfile(license_name)
        if metadata_file is None or license_file is None:
            raise ValueError("package metadata or license is not a regular file")

        validate_metadata(metadata_file.read())
        if license_file.read() != EXPECTED_LICENSE:
            raise ValueError("packaged source LICENSE does not match sdks/python/LICENSE")


def validate_artifact(path: Path) -> None:
    if path.suffix == ".whl":
        validate_wheel(path)
    elif path.name.endswith(".tar.gz"):
        validate_sdist(path)
    else:
        raise ValueError("expected a .whl or .tar.gz package")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the Python SDK license metadata and packaged license files."
    )
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()

    if (PACKAGE_ROOT / "LICENSE").read_bytes() != EXPECTED_LICENSE:
        print("sdks/python/LICENSE does not match the repository LICENSE")
        return 1

    failed = False
    for artifact in args.artifacts:
        try:
            validate_artifact(artifact)
        except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
            failed = True
            print(f"{artifact}: {error}")
        else:
            print(f"{artifact}: license metadata and files are valid")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
