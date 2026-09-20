"""A cached image must not hide an unavailable fixture registry."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class ComposeDependencyTests(unittest.TestCase):
    def test_pull_failure_stops_before_starting_cached_images(self):
        script = Path(__file__).with_name("compose-smoke.sh")
        for platform in ("linux/amd64", "linux/arm64"):
            with self.subTest(platform=platform), tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                docker = directory / "docker"
                log = directory / "docker.log"
                docker.write_text(
                    '#!/bin/sh\n'
                    'printf "%s|%s\\n" "${HELIX_IMAGE_PLATFORM:-}" "$*" >> "$DOCKER_LOG"\n'
                    'case "$*" in\n'
                    '  *" pull minio minio-init") exit 42 ;;\n'
                    'esac\n'
                    'exit 0\n'
                )
                docker.chmod(0o755)
                result = subprocess.run(
                    ["bash", str(script), "--platform", platform, "--image", "cached:test"],
                    env={
                        **os.environ,
                        "PATH": f"{directory}{os.pathsep}{os.environ['PATH']}",
                        "DOCKER_LOG": str(log),
                    },
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(result.returncode, 42, result.stderr)
                calls = log.read_text().splitlines()
                self.assertIn("|image inspect cached:test", calls)
                self.assertTrue(
                    any(
                        call.startswith(f"{platform}|compose ")
                        and call.endswith(" pull minio minio-init")
                        for call in calls
                    ),
                    calls,
                )
                self.assertFalse(any(" up " in call or "|run " in call for call in calls), calls)
                self.assertTrue(any(call.endswith(" down -v") for call in calls), calls)


if __name__ == "__main__":
    unittest.main()
