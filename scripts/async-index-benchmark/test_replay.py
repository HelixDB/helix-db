"""Replay outcome classification against a real local HTTP server."""

import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import replay

REPLIES = {
    "ok": (200, {}),
    "conflict": (409, {"error": "transaction_conflict"}),
    "backpressure": (429, {"error": "index_backpressure", "retryable": True}),
    "invalid": (400, {"error": "invalid_request"}),
    "fenced": (
        503,
        {"error": "writer_fenced_commit_outcome_unknown", "retryable": False},
    ),
    "internal": (500, {"error": "internal_error"}),
}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 - http.server API
        request = json.loads(self.rfile.read(int(self.headers["content-length"])))
        self.server.headers.append(dict(self.headers))
        name = request["query_name"]
        if name == "slow":
            time.sleep(1.5)
            name = "ok"
        status, body = REPLIES[name]
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


class Classification(unittest.TestCase):
    def test_every_outcome_is_distinguished(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.headers = []
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        ms = 1_000_000
        plan = [
            ("write", 0, "ok", "acknowledged"),
            ("write", 100 * ms, "conflict", "conflict"),
            ("write", 200 * ms, "backpressure", "backpressure"),
            ("write", 300 * ms, "invalid", "rejected"),
            ("write", 400 * ms, "fenced", "unknown_write"),
            ("write", 500 * ms, "slow", "timeout"),
            ("write", 500 * ms, "ok", "client_overload"),
            ("strong", 1_300 * ms, "internal", "http_error"),
            ("eventual", 1_300 * ms, "ok", "search_ok"),
        ]
        with tempfile.TemporaryDirectory() as root:
            trace = Path(root) / "trace.jsonl"
            trace.write_text(
                "".join(
                    json.dumps(
                        {
                            "id": i,
                            "at_ns": at,
                            "kind": kind,
                            "payload": {
                                "request_type": "write" if kind == "write" else "read",
                                "query_name": name,
                            },
                        }
                    )
                    + "\n"
                    for i, (kind, at, name, _) in enumerate(plan)
                )
            )
            url = f"http://127.0.0.1:{server.server_address[1]}"
            footer = replay.run(
                trace,
                Path(root) / "out",
                dict.fromkeys(replay.Kind, url),
                dict.fromkeys(replay.Kind, 1),
                start_delay_s=0,
                timeout_s=0.5,
            )
            records = [
                json.loads(line)
                for line in (Path(root) / "out" / "requests.jsonl")
                .read_text()
                .splitlines()
            ]
            with self.assertRaises(FileExistsError):
                replay.run(
                    trace,
                    Path(root) / "out",
                    dict.fromkeys(replay.Kind, url),
                    dict.fromkeys(replay.Kind, 1),
                )
        outcomes = {r["id"]: r["outcome"] for r in records if r["event"] == "result"}
        self.assertEqual(
            outcomes, {i: expected for i, (*_, expected) in enumerate(plan)}
        )
        self.assertEqual(footer["offered"], len(plan))
        self.assertEqual(sum(footer["counts"].values()), len(plan))
        by_id = {r["id"]: r for r in records if r["event"] == "result"}
        self.assertEqual(by_id[4]["error"], "writer_fenced_commit_outcome_unknown")
        self.assertEqual(by_id[5]["error_type"], "TimeoutError")
        self.assertNotIn("latency_ns", by_id[6])
        writes = [h for h in server.headers if h.get("x-helix-await-durable") == "true"]
        self.assertEqual(len(writes), 6)


if __name__ == "__main__":
    unittest.main()
