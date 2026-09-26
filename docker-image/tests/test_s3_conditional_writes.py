"""The conditional-write probe must fail loudly on a store that ignores them."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

PROBE = Path(__file__).with_name("s3_conditional_writes.py")

# Emulates `docker exec <container> curl ...` against an in-memory S3 bucket
# whose conditional-write behavior is selected by FAKE_S3_MODE.
FAKE_DOCKER = """
import hashlib
import json
import os
from pathlib import Path
import sys

mode = os.environ["FAKE_S3_MODE"]
if mode == "exec-fails":
    sys.stderr.write("Error response from daemon: container is not running\\n")
    sys.exit(1)
args = sys.argv[1:]
assert args[:3] == ["exec", "seaweedfs-test", "curl"], args
options = {}
index = 3
while index < len(args) - 1:
    if args[index] in ("--max-time", "--aws-sigv4", "--user", "-X", "-o", "-w", "--data-binary"):
        options[args[index]] = args[index + 1]
        index += 2
    elif args[index] == "-H":
        if not args[index + 1].startswith("Content-Type:"):
            options["condition"] = args[index + 1]
        index += 2
    else:
        index += 1
assert options["--user"] == "helix:secret", options
key = args[-1]
state_path = Path(os.environ["FAKE_S3_STATE"])
state = json.loads(state_path.read_text()) if state_path.exists() else {}
current = state.get(key)
condition = options.get("condition", "")
method = options["-X"]
status, etag, payload = 200, "", ""
if mode == "garbled":
    sys.stdout.write("not an HTTP trailer")
    sys.exit(0)
if mode == "forbidden":
    status, payload = 403, "<Error><Code>AccessDenied</Code></Error>"
elif method == "PUT":
    create_conflict = condition == "If-None-Match: *" and current is not None
    match_conflict = condition.startswith("If-Match: ") and (
        current is None or current["etag"] != condition[len("If-Match: "):]
    )
    if mode == "ignore-if-none-match":
        create_conflict = False
    if mode == "ignore-if-match":
        match_conflict = False
    if mode == "reject-if-match" and condition.startswith("If-Match: "):
        match_conflict = True
    if create_conflict or match_conflict:
        status, payload = 412, "<Error><Code>PreconditionFailed</Code></Error>"
    if status == 200 or mode == "412-but-writes":
        body = options["--data-binary"]
        etag = '"' + hashlib.md5(body.encode()).hexdigest() + '"'
        state[key] = {"body": body, "etag": etag}
    if mode == "no-etag" or status != 200:
        etag = ""
elif method == "GET":
    if current is None:
        status = 404
    else:
        etag, payload = current["etag"], current["body"]
elif method == "DELETE":
    state.pop(key, None)
    status = 204
state_path.write_text(json.dumps(state))
sys.stdout.write(payload + "\\n" + str(status) + " " + etag)
"""


class ConditionalWriteProbeTests(unittest.TestCase):
    def run_probe(self, mode):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            docker = directory / "docker"
            docker.write_text(f"#!{sys.executable}\n{FAKE_DOCKER}")
            docker.chmod(0o755)
            return subprocess.run(
                [
                    sys.executable, str(PROBE),
                    "--container", "seaweedfs-test",
                    "--endpoint", "http://127.0.0.1:8333",
                    "--endpoint", "http://s3-trace:8333",
                    "--bucket", "helix-db",
                    "--access-key", "helix",
                    "--secret-key", "secret",
                ],
                env={
                    **os.environ,
                    "PATH": f"{directory}{os.pathsep}{os.environ['PATH']}",
                    "FAKE_S3_MODE": mode,
                    "FAKE_S3_STATE": str(directory / "state.json"),
                },
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )

    def assert_fails_loudly(self, mode, *fragments):
        result = self.run_probe(mode)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("CONDITIONAL WRITE PROBE FAILED", result.stderr)
        self.assertIn("silently lose data", result.stderr)
        for fragment in fragments:
            self.assertIn(fragment, result.stderr)
        self.assertNotIn("probe passed", result.stdout)

    def test_enforcing_store_passes_every_endpoint(self):
        result = self.run_probe("enforcing")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for endpoint in ("http://127.0.0.1:8333", "http://s3-trace:8333"):
            self.assertIn(f"Conditional write probe via {endpoint}", result.stdout)
        self.assertEqual(
            result.stdout.count(
                "(a) create with If-None-Match: * on an existing key: HTTP 412 (expected 412)"
            ),
            2,
        )
        self.assertEqual(
            result.stdout.count(
                "(b) replace with If-Match: <wrong etag>: HTTP 412 (expected 412)"
            ),
            2,
        )
        self.assertEqual(
            result.stdout.count("replace with If-Match: <current etag>: HTTP 200"), 2
        )
        self.assertIn("Conditional write probe passed on 2 endpoint(s)", result.stdout)

    def test_ignored_if_none_match_fails(self):
        self.assert_fails_loudly(
            "ignore-if-none-match",
            "(a) create with If-None-Match: * on an existing key returned HTTP 200, expected 412",
        )

    def test_ignored_if_match_fails(self):
        self.assert_fails_loudly(
            "ignore-if-match",
            "(b) replace with If-Match: <wrong etag> returned HTTP 200, expected 412",
        )

    def test_rejection_that_still_writes_fails(self):
        self.assert_fails_loudly(
            "412-but-writes", "object after rejected writes read 'stale', expected 'created'"
        )

    def test_rejected_matching_replace_fails(self):
        self.assert_fails_loudly(
            "reject-if-match",
            "replace with If-Match: <current etag> returned HTTP 412, expected 200",
        )

    def test_other_error_statuses_are_not_accepted(self):
        self.assert_fails_loudly("forbidden", "returned HTTP 403, expected 200", "AccessDenied")

    def test_create_without_etag_fails(self):
        self.assert_fails_loudly("no-etag", "returned no ETag")

    def test_unreachable_container_fails(self):
        self.assert_fails_loudly("exec-fails", "did not complete", "container is not running")

    def test_unparseable_curl_output_fails(self):
        self.assert_fails_loudly("garbled", "printed no status")


if __name__ == "__main__":
    unittest.main()
