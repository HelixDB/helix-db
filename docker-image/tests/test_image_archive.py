import io
import json
import sys
import tarfile
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock


TESTS_DIR = Path(__file__).resolve().parent
DOCKER_IMAGE_DIR = TESTS_DIR.parent
sys.path.insert(0, str(DOCKER_IMAGE_DIR))

import image_archive  # noqa: E402


UNSET = object()


class ArchiveFixture:
    def __init__(self, root: Path) -> None:
        self.path = root / "image.tar"

    @staticmethod
    def _add_bytes(archive: tarfile.TarFile, name: str, payload: bytes) -> None:
        info = tarfile.TarInfo(name)
        info.size = len(payload)
        info.mode = 0o644
        archive.addfile(info, io.BytesIO(payload))

    @staticmethod
    def _layer(
        architecture: str,
        include_binary: bool,
        tmp_mode: int,
        data_owner: tuple,
        cache_owner: tuple,
        binary_suffix: bytes,
        binary_payload,
        extra_files: dict,
    ) -> bytes:
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as layer:
            tmp = tarfile.TarInfo("tmp/")
            tmp.type = tarfile.DIRTYPE
            tmp.mode = tmp_mode
            layer.addfile(tmp)

            data = tarfile.TarInfo("var/lib/helix/")
            data.type = tarfile.DIRTYPE
            data.mode = 0o755
            data.uid, data.gid = data_owner
            layer.addfile(data)

            cache = tarfile.TarInfo("var/cache/helix/")
            cache.type = tarfile.DIRTYPE
            cache.mode = 0o755
            cache.uid, cache.gid = cache_owner
            layer.addfile(cache)

            if include_binary:
                loader = {
                    "amd64": b"/lib64/ld-linux-x86-64.so.2",
                    "arm64": b"/lib/ld-linux-aarch64.so.1",
                }.get(architecture, b"unknown-loader")
                payload = binary_payload or (
                    b"\x7fELF\x00" + loader + b"\x00/v2/query\x00" + binary_suffix
                )
                ArchiveFixture._add_bytes(
                    layer,
                    "bin/helix-server",
                    payload,
                )

            for path, payload in extra_files.items():
                ArchiveFixture._add_bytes(layer, path, payload)
        return buffer.getvalue()

    def write(
        self,
        *,
        architecture="amd64",
        expected_tag="ghcr.io/helixdb/helixdb:test",
        config_updates=None,
        environment=None,
        include_binary=True,
        tmp_mode=0o1777,
        data_owner=(65532, 65532),
        cache_owner=(65532, 65532),
        binary_suffix=b"",
        binary_payload=None,
        extra_files=None,
        manifest_override=UNSET,
        config_override=UNSET,
        include_layer=True,
    ) -> Path:
        runtime_config = {
            "Entrypoint": list(image_archive.EXPECTED_ENTRYPOINT),
            "WorkingDir": "/home/nonroot",
            "User": "65532:65532",
            "StopSignal": "SIGTERM",
            "ExposedPorts": {"8080/tcp": {}},
            "Env": ["{}={}".format(key, value) for key, value in image_archive.EXPECTED_ENV.items()],
            "Labels": dict(image_archive.EXPECTED_LABELS),
        }
        if environment is not None:
            runtime_config["Env"] = environment
        if config_updates:
            runtime_config.update(config_updates)

        config = {"architecture": architecture, "config": runtime_config}
        manifest = [
            {
                "Config": "config.json",
                "RepoTags": [expected_tag],
                "Layers": ["layer.tar"],
            }
        ]
        if config_override is not UNSET:
            config = config_override
        if manifest_override is not UNSET:
            manifest = manifest_override
        layer = self._layer(
            architecture,
            include_binary,
            tmp_mode,
            data_owner,
            cache_owner,
            binary_suffix,
            binary_payload,
            extra_files or {},
        )

        with tarfile.open(self.path, mode="w") as archive:
            self._add_bytes(archive, "manifest.json", json.dumps(manifest).encode())
            self._add_bytes(archive, "config.json", json.dumps(config).encode())
            if include_layer:
                self._add_bytes(archive, "layer.tar", layer)
        return self.path


class ImageInspectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fixture = ArchiveFixture(Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def assert_inspection_error(self, expected: str, path: Path) -> None:
        with self.assertRaisesRegex(image_archive.ImageArchiveError, expected):
            image_archive.inspect_archive(path)

    def test_valid_amd64_and_arm64_archives_pass(self) -> None:
        amd64 = self.fixture.write()
        image_archive.inspect_archive(amd64, "ghcr.io/helixdb/helixdb:test")
        image_archive.scan_archive_for_secrets(amd64)

        arm64 = self.fixture.write(architecture="arm64")
        image_archive.inspect_archive(arm64)

    def test_expected_reference_must_be_present(self) -> None:
        path = self.fixture.write()
        with self.assertRaisesRegex(image_archive.ImageArchiveError, "expected image reference"):
            image_archive.inspect_archive(path, "ghcr.io/helixdb/helixdb:missing")

    def test_runtime_metadata_is_exact(self) -> None:
        cases = [
            ({"Entrypoint": ["/bin/wrong"]}, "unexpected entrypoint"),
            ({"WorkingDir": "/workspace"}, "unexpected working directory"),
            ({"User": "0"}, "unexpected runtime user"),
            ({"StopSignal": "SIGKILL"}, "unexpected stop signal"),
            ({"ExposedPorts": {"8081/tcp": {}}}, "unexpected exposed ports"),
            ({"Labels": {}}, "unexpected label"),
        ]
        for updates, expected in cases:
            with self.subTest(expected=expected):
                path = self.fixture.write(config_updates=updates)
                self.assert_inspection_error(expected, path)

    def test_malformed_manifest_and_config_are_rejected(self) -> None:
        cases = [
            ({}, UNSET, "expected one image manifest entry"),
            ([], UNSET, "expected one image manifest entry"),
            (["invalid"], UNSET, "manifest entry is not an object"),
            ([{}], UNSET, "manifest has no config path"),
            (UNSET, [], "image config is not an object"),
            (UNSET, {"architecture": "amd64"}, "no runtime config object"),
        ]
        for manifest, config, expected in cases:
            with self.subTest(expected=expected):
                path = self.fixture.write(
                    manifest_override=manifest,
                    config_override=config,
                )
                self.assert_inspection_error(expected, path)

        with tarfile.open(self.fixture.path, mode="w") as archive:
            ArchiveFixture._add_bytes(archive, "manifest.json", b"not-json")
        self.assert_inspection_error("invalid JSON", self.fixture.path)

        with tarfile.open(self.fixture.path, mode="w"):
            pass
        self.assert_inspection_error("missing archive member", self.fixture.path)

    def test_environment_rejects_missing_unexpected_and_changed_values(self) -> None:
        expected_entries = [
            "{}={}".format(key, value) for key, value in image_archive.EXPECTED_ENV.items()
        ]
        cases = [
            (expected_entries[1:], "missing expected environment"),
            (expected_entries + ["S3_BUCKET=bucket"], "unexpected baked environment"),
            ("not-a-list", "image environment is not a list"),
            (
                [entry for entry in expected_entries if not entry.startswith("DB_PATH=")]
                + ["DB_PATH=wrong/"],
                "unexpected environment DB_PATH",
            ),
            (expected_entries + ["malformed"], "malformed environment entry"),
        ]
        for environment, expected in cases:
            with self.subTest(expected=expected):
                path = self.fixture.write(environment=environment)
                self.assert_inspection_error(expected, path)

    def test_filesystem_contract_rejects_missing_or_legacy_paths(self) -> None:
        missing = self.fixture.write(include_binary=False)
        self.assert_inspection_error("missing required image paths", missing)

        legacy = self.fixture.write(extra_files={"bin/gateway": b"legacy"})
        self.assert_inspection_error("legacy runtime paths", legacy)

        malformed_layers = self.fixture.write(
            manifest_override=[
                {
                    "Config": "config.json",
                    "RepoTags": ["ghcr.io/helixdb/helixdb:test"],
                    "Layers": "layer.tar",
                }
            ]
        )
        self.assert_inspection_error("layers are malformed", malformed_layers)

        missing_layer = self.fixture.write(include_layer=False)
        self.assert_inspection_error("missing image layer", missing_layer)

    def test_writable_directory_contract_is_enforced(self) -> None:
        bad_tmp = self.fixture.write(tmp_mode=0o755)
        self.assert_inspection_error("mode-1777", bad_tmp)

        bad_owner = self.fixture.write(data_owner=(0, 0))
        self.assert_inspection_error("/var/lib/helix is not owned by the non-root", bad_owner)

        bad_cache_owner = self.fixture.write(cache_owner=(0, 0))
        self.assert_inspection_error(
            "/var/cache/helix is not owned by the non-root", bad_cache_owner
        )

    def test_binary_contract_rejects_wrong_linkage_and_contents(self) -> None:
        unsupported = self.fixture.write(architecture="ppc64le")
        self.assert_inspection_error("unsupported image architecture", unsupported)

        musl = self.fixture.write(binary_suffix=b"musl")
        self.assert_inspection_error("musl linkage", musl)

        wrong_loader = self.fixture.write(binary_payload=b"\x7fELF\x00wrong\x00/v2/query")
        self.assert_inspection_error("expected GNU libc loader", wrong_loader)

        no_endpoint = self.fixture.write(
            binary_payload=b"\x7fELF\x00/lib64/ld-linux-x86-64.so.2"
        )
        self.assert_inspection_error("does not contain the /v2/query endpoint", no_endpoint)

    def test_non_mapping_labels_are_rejected(self) -> None:
        path = self.fixture.write(config_updates={"Labels": "invalid"})
        self.assert_inspection_error("labels are not an object", path)

    def test_cli_reports_success_and_contract_failures(self) -> None:
        valid = self.fixture.write()
        stdout = io.StringIO()
        with mock.patch.object(
            sys,
            "argv",
            ["image_archive.py", "inspect", str(valid), "--expected-image", "ghcr.io/helixdb/helixdb:test"],
        ), redirect_stdout(stdout):
            self.assertEqual(image_archive.main(), 0)
        self.assertIn("Image inspection passed", stdout.getvalue())

        base_environment = [
            "{}={}".format(key, value) for key, value in image_archive.EXPECTED_ENV.items()
        ]
        invalid = self.fixture.write(environment=base_environment + ["API_TOKEN=abcdefgh"])
        stderr = io.StringIO()
        with mock.patch.object(sys, "argv", ["image_archive.py", "scan", str(invalid)]), redirect_stderr(stderr):
            self.assertEqual(image_archive.main(), 1)
        self.assertIn("forbidden environment variable", stderr.getvalue())

        valid = self.fixture.write()
        stdout = io.StringIO()
        with mock.patch.object(sys, "argv", ["image_archive.py", "scan", str(valid)]), redirect_stdout(stdout):
            self.assertEqual(image_archive.main(), 0)
        self.assertIn("Secret scan passed", stdout.getvalue())


class SecretScanningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fixture = ArchiveFixture(Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def assert_secret_error(self, expected: str, path: Path) -> None:
        with self.assertRaisesRegex(image_archive.ImageArchiveError, expected):
            image_archive.scan_archive_for_secrets(path)

    def test_secret_environment_names_and_values_are_rejected(self) -> None:
        base = ["{}={}".format(key, value) for key, value in image_archive.EXPECTED_ENV.items()]
        forbidden_name = self.fixture.write(environment=base + ["API_TOKEN=abcdefgh"])
        self.assert_secret_error("forbidden environment variable", forbidden_name)

        forbidden_value = self.fixture.write(environment=base + ["CONFIG=AKIAABCDEFGHIJKLMNOP"])
        self.assert_secret_error("potential secret environment value", forbidden_value)

    def test_secret_paths_and_text_are_rejected(self) -> None:
        secret_path = self.fixture.write(extra_files={"root/.aws/credentials": b"clean"})
        self.assert_secret_error("forbidden path pattern", secret_path)

        secret_text = self.fixture.write(extra_files={"app/config.txt": b"password=abcdefgh"})
        self.assert_secret_error("potential secret pattern", secret_text)

    def test_binary_and_large_payloads_are_not_treated_as_text(self) -> None:
        binary = self.fixture.write(extra_files={"app/blob.bin": b"\x00password=abcdefgh"})
        image_archive.scan_archive_for_secrets(binary)

        large = self.fixture.write(
            extra_files={"app/large.txt": b"password=abcdefgh" * 100000}
        )
        image_archive.scan_archive_for_secrets(large)

        clean_text = self.fixture.write(extra_files={"app/NOTICE": b"plain text"})
        image_archive.scan_archive_for_secrets(clean_text)

        image_archive._scan_text_payload("app/invalid.txt", b"\xff")

    def test_secret_scan_rejects_malformed_runtime_and_missing_layers(self) -> None:
        missing_runtime = self.fixture.write(config_override={"architecture": "amd64"})
        self.assert_secret_error("no runtime config object", missing_runtime)

        missing_layer = self.fixture.write(include_layer=False)
        self.assert_secret_error("missing image layer", missing_layer)

    def test_missing_archive_is_rejected_by_both_operations(self) -> None:
        missing = Path(self.temp_dir.name) / "missing.tar"
        with self.assertRaisesRegex(image_archive.ImageArchiveError, "does not exist"):
            image_archive.inspect_archive(missing)
        with self.assertRaisesRegex(image_archive.ImageArchiveError, "does not exist"):
            image_archive.scan_archive_for_secrets(missing)


if __name__ == "__main__":
    unittest.main()
