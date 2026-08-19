from __future__ import annotations

import subprocess
import sys
import tarfile
import unittest
import zipfile
from pathlib import Path

import tomllib

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"
PACKAGE_ROOT = PYPROJECT.parent
REPO_ROOT = PACKAGE_ROOT.parent.parent
EMBEDDED_PYPROJECT = REPO_ROOT / "bindings" / "uniffi" / "pyproject.toml"

try:
    import build  # noqa: F401

    HAS_BUILD = True
except ImportError:
    HAS_BUILD = False


def _license_config(pyproject: Path = PYPROJECT) -> tuple[str, list[str]]:
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = data["project"]
    return project["license"], project.get("license-files", [])


class LicenseMetadataTest(unittest.TestCase):
    """The Python SDK is Apache-2.0, matching the repository root, the Rust
    SDK, and the TypeScript SDK. These checks catch the metadata and the
    license drifting apart again, independent of whether a build is run."""

    def test_pyproject_declares_apache_license(self) -> None:
        license_expr, license_files = _license_config()
        self.assertEqual(license_expr, "Apache-2.0")
        self.assertIn("LICENSE", license_files)

    def test_pyproject_has_no_conflicting_license_classifier(self) -> None:
        data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
        classifiers = data["project"].get("classifiers", [])
        license_classifiers = [c for c in classifiers if c.startswith("License ::")]
        self.assertEqual(
            license_classifiers,
            [],
            "PEP 639 license expressions must not be combined with "
            "'License ::' classifiers",
        )

    def test_license_file_matches_repository_root(self) -> None:
        root_license = PACKAGE_ROOT.parent.parent / "LICENSE"
        sdk_license = PACKAGE_ROOT / "LICENSE"
        self.assertTrue(sdk_license.is_file(), "sdks/python/LICENSE is missing")
        self.assertIn("Apache License", sdk_license.read_text(encoding="utf-8"))
        self.assertIn("Version 2.0", root_license.read_text(encoding="utf-8"))

    def test_embedded_package_also_declares_apache_license(self) -> None:
        """bindings/uniffi (helix-db-embedded) is pip-installable alongside
        this package and must carry the same license grant."""

        license_expr, license_files = _license_config(EMBEDDED_PYPROJECT)
        self.assertEqual(license_expr, "Apache-2.0")
        self.assertIn("LICENSE", license_files)
        embedded_license = EMBEDDED_PYPROJECT.parent / "LICENSE"
        self.assertTrue(embedded_license.is_file(), "bindings/uniffi/LICENSE is missing")
        self.assertIn("Apache License", embedded_license.read_text(encoding="utf-8"))


@unittest.skipUnless(HAS_BUILD, "requires the 'build' package (pip install build)")
class BuiltArtifactLicenseTest(unittest.TestCase):
    """Builds a real sdist and wheel and inspects them directly, since
    metadata alone does not guarantee the license text ships in the
    installed artifact."""

    @classmethod
    def setUpClass(cls) -> None:
        import tempfile

        cls._tmpdir = tempfile.TemporaryDirectory()
        out_dir = Path(cls._tmpdir.name)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--sdist",
                "--wheel",
                "--outdir",
                str(out_dir),
                str(PACKAGE_ROOT),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        cls.wheel = next(out_dir.glob("*.whl"))
        cls.sdist = next(out_dir.glob("*.tar.gz"))

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmpdir.cleanup()

    def test_wheel_contains_license_file(self) -> None:
        with zipfile.ZipFile(self.wheel) as archive:
            names = archive.namelist()
            license_entries = [
                name
                for name in names
                if name.endswith(".dist-info/licenses/LICENSE")
            ]
            self.assertTrue(
                license_entries,
                f"no licenses/LICENSE entry in wheel; contents: {names}",
            )
            text = archive.read(license_entries[0]).decode("utf-8")
            self.assertIn("Apache License", text)
            self.assertIn("Version 2.0", text)

    def test_wheel_metadata_declares_apache_license(self) -> None:
        with zipfile.ZipFile(self.wheel) as archive:
            metadata_name = next(
                name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
            )
            metadata = archive.read(metadata_name).decode("utf-8")
        self.assertIn("License-Expression: Apache-2.0", metadata)
        self.assertNotIn("License :: OSI Approved :: MIT License", metadata)

    def test_sdist_contains_license_file(self) -> None:
        with tarfile.open(self.sdist) as archive:
            names = archive.getnames()
            license_entries = [name for name in names if name.endswith("/LICENSE")]
            self.assertTrue(
                license_entries,
                f"no LICENSE file in sdist; contents: {names}",
            )
            member = archive.extractfile(license_entries[0])
            assert member is not None
            text = member.read().decode("utf-8")
            self.assertIn("Apache License", text)


if __name__ == "__main__":
    unittest.main()
