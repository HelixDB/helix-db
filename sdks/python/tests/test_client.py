from __future__ import annotations

import json
from io import BytesIO
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from helixdb import Client, HelixError, DynamicQueryRequest, g, read_batch


class FakeResponse:
    def __init__(self, body: bytes = b'{"ok":true}', status: int = 200, reason: str = "OK") -> None:
        self.body = body
        self.status = status
        self.reason = reason

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def getcode(self) -> int:
        return self.status

    def read(self) -> bytes:
        return self.body


class ClientTests(unittest.TestCase):
    def test_dynamic_request_posts_query_with_headers(self) -> None:
        request = DynamicQueryRequest.read(
            read_batch().var_as("count", g().n_with_label("User").count()).returning(["count"])
        )

        calls = []

        def fake_urlopen(req):
            calls.append(req)
            return FakeResponse()

        with patch("helixdb.client.urlopen", fake_urlopen):
            result = (
                Client("http://127.0.0.1:6969", api_key="hx_secret")
                .query()
                .writer_only()
                .warm_only()
                .should_await_durability(False)
                .dynamic(request)
                .send()
            )

        self.assertEqual(result, {"ok": True})
        req = calls[0]
        self.assertEqual(req.full_url, "http://127.0.0.1:6969/v1/query")
        self.assertEqual(req.headers["Authorization"], "Bearer hx_secret")
        self.assertEqual(req.headers["X-helix-require-writer"], "true")
        self.assertEqual(req.headers["X-helix-warm"], "true")
        self.assertEqual(req.headers["X-helix-await-durable"], "false")
        self.assertEqual(json.loads(req.data.decode("utf-8"))["request_type"], "read")

    def test_stored_request_posts_to_named_route(self) -> None:
        calls = []

        def fake_urlopen(req):
            calls.append(req)
            return FakeResponse()

        with patch("helixdb.client.urlopen", fake_urlopen):
            result = Client("http://127.0.0.1:6969").query().body({"name": "Alice"}).stored("add_user").send()

        self.assertEqual(result, {"ok": True})
        req = calls[0]
        self.assertEqual(req.full_url, "http://127.0.0.1:6969/v1/query/add_user")
        self.assertEqual(json.loads(req.data.decode("utf-8")), {"name": "Alice"})

    def test_remote_error_includes_status_and_details(self) -> None:
        def fake_urlopen(req):
            raise HTTPError(req.full_url, 409, "Conflict", hdrs={}, fp=BytesIO(b"conflict"))

        with patch("helixdb.client.urlopen", fake_urlopen):
            with self.assertRaises(HelixError) as ctx:
                Client("http://127.0.0.1:6969").query().body({}).stored("conflict").send()

        self.assertEqual(ctx.exception.kind, "Remote")
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.details, "conflict")


if __name__ == "__main__":
    unittest.main()
