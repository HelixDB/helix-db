"""The conditional-write probe must fail loudly on a store that ignores them."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

PROBE = Path(__file__).with_name("s3_conditional_writes.py")

# Emulates `docker exec <container> curl ...` against an in-memory S3 bucket
# whose conditional-write behavior is selected by FAKE_S3_MODE. A curl call
# with several `--next` transfers is a race: an atomic store settles racers
# one at a time, last racer first, so the probe cannot assume the first wins,
# while a "racy-*" store checks every racer's condition before any writes.
FAKE_DOCKER = """
import hashlib
import json
import os
from pathlib import Path
import sys

VALUE_OPTIONS = (
    "--parallel-max", "--max-time", "--aws-sigv4", "--user", "-X", "-o", "-w", "--data-binary"
)

mode = os.environ["FAKE_S3_MODE"]
if mode == "exec-fails":
    sys.stderr.write("Error response from daemon: container is not running\\n")
    sys.exit(1)
args = sys.argv[1:]
assert args[:3] == ["exec", "seaweedfs-test", "curl"], args
flags, transfers = set(), [{"headers": []}]
index = 3
while index < len(args):
    arg = args[index]
    if arg == "--next":
        transfers.append({"headers": []})
    elif arg == "-H":
        index += 1
        transfers[-1]["headers"].append(args[index])
    elif arg in VALUE_OPTIONS:
        index += 1
        transfers[-1][arg] = args[index]
    elif arg.startswith("-"):
        flags.add(arg)
    else:
        transfers[-1]["url"] = arg
    index += 1
race = len(transfers) > 1
if race:
    # Racers must reach the store together, each on its own connection.
    assert {"--parallel", "--parallel-immediate"} <= flags, flags
    assert transfers[0]["--parallel-max"] == str(len(transfers)), transfers[0]
    assert len({transfer["url"] for transfer in transfers}) == 1, transfers
if mode == "garbled":
    sys.stdout.write("not an HTTP trailer")
    sys.exit(0)
state_path = Path(os.environ["FAKE_S3_STATE"])
state = json.loads(state_path.read_text()) if state_path.exists() else {}
before = dict(state)
results = []
for transfer in reversed(transfers):
    assert transfer["--user"] == "helix:secret", transfer
    key = transfer["url"]
    method = transfer["-X"]
    condition = next(
        (header for header in transfer["headers"] if not header.startswith("Content-Type:")), ""
    )
    kind = (
        "if-none-match" if condition == "If-None-Match: *"
        else "if-match" if condition.startswith("If-Match: ")
        else "unconditional"
    )
    current = (before if race and mode == "racy-" + kind else state).get(key)
    status, etag, payload = 200, "", ""
    if mode == "forbidden":
        status, payload = 403, "<Error><Code>AccessDenied</Code></Error>"
    elif method == "PUT":
        create_conflict = kind == "if-none-match" and current is not None
        match_conflict = kind == "if-match" and (
            current is None or current["etag"] != condition[len("If-Match: "):]
        )
        if mode == "ignore-if-none-match":
            create_conflict = False
        if mode == "ignore-if-match":
            match_conflict = False
        if mode == "reject-if-match" and kind == "if-match":
            match_conflict = True
        if create_conflict or match_conflict:
            status, payload = 412, "<Error><Code>PreconditionFailed</Code></Error>"
            if race and mode == "race-409":
                status, payload = 409, "<Error><Code>ConditionalRequestConflict</Code></Error>"
        if status == 200 or mode == "412-but-writes" or (race and mode == "race-412-but-writes"):
            body = transfer["--data-binary"]
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
    if not (race and mode == "race-drops-a-racer" and transfer is transfers[0]):
        results.append((transfer, status, etag, payload))
state_path.write_text(json.dumps(state))
for transfer, status, etag, payload in results:
    if transfer["-o"] == "-":
        sys.stdout.write(payload)
    write_out = transfer["-w"].replace("%{http_code}", str(status))
    sys.stdout.write(write_out.replace("%header{etag}", etag))
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
        for race in (
            "(c) 8 concurrent creates with If-None-Match: * on a new key",
            "(d) 8 concurrent replaces with If-Match: <current etag>",
        ):
            self.assertEqual(
                result.stdout.count(
                    f"{race}: 1 x HTTP 200, 7 x HTTP 412 (expected 1 x HTTP 200, 7 x HTTP 412)"
                ),
                2,
                result.stdout,
            )
        # The fake store lets the last racer win, so the probe follows the
        # actual winner rather than assuming the first racer's body.
        for race, winner in (("creates", "create-7"), ("replaces", "replace-7")):
            self.assertEqual(
                result.stdout.count(
                    f"object after concurrent {race}: '{winner}' (expected '{winner}')"
                ),
                2,
                result.stdout,
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

    def test_racing_creates_that_all_succeed_fail(self):
        self.assert_fails_loudly(
            "racy-if-none-match",
            "(c) 8 concurrent creates with If-None-Match: * on a new key returned 8 x HTTP 200,"
            " expected exactly one HTTP 200 and HTTP 412 for the rest",
        )

    def test_racing_replaces_that_all_succeed_fail(self):
        self.assert_fails_loudly(
            "racy-if-match",
            "(d) 8 concurrent replaces with If-Match: <current etag> returned 8 x HTTP 200,"
            " expected exactly one HTTP 200 and HTTP 412 for the rest",
        )

    def test_race_losers_that_still_write_fail(self):
        self.assert_fails_loudly(
            "race-412-but-writes",
            "object after concurrent creates read 'create-0', expected 'create-7'",
        )

    def test_race_losers_must_get_412(self):
        self.assert_fails_loudly(
            "race-409",
            "returned 1 x HTTP 200, 7 x HTTP 409, expected exactly one HTTP 200",
        )

    def test_race_missing_a_racer_fails(self):
        self.assert_fails_loudly("race-drops-a-racer", "reported 7 of 8 racers")


if __name__ == "__main__":
    unittest.main()
