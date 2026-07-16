from __future__ import annotations

import json
from io import BytesIO
import sys
import types
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from helixdb import Client, Disk, QueryRequest, HelixError, InMemory, g, read_batch, write_batch


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


class FakeNativeHandle:
    def __init__(self) -> None:
        self.requests: list[bytes] = []
        self.closed = False

    async def query_json(self, body: bytes) -> bytes:
        self.requests.append(bytes(body))
        return b'{"users":0}'

    async def close(self) -> None:
        self.closed = True


class FakeNativeDB:
    opened: list[object] = []
    opened_readers: list[object] = []
    handle = FakeNativeHandle()

    @classmethod
    async def open(cls, source: object) -> FakeNativeHandle:
        cls.opened.append(source)
        cls.handle = FakeNativeHandle()
        return cls.handle

    @classmethod
    async def open_reader(cls, source: object) -> FakeNativeHandle:
        cls.opened_readers.append(source)
        cls.handle = FakeNativeHandle()
        return cls.handle


class FakeNativeSource:
    @staticmethod
    def IN_MEMORY(**kwargs: object) -> tuple[str, dict[str, object]]:
        return ("IN_MEMORY", kwargs)

    @staticmethod
    def DISK(**kwargs: object) -> tuple[str, dict[str, object]]:
        return ("DISK", kwargs)

    @staticmethod
    def OBJECT_STORAGE(**kwargs: object) -> tuple[str, dict[str, object]]:
        return ("OBJECT_STORAGE", kwargs)


def fake_native_module() -> types.SimpleNamespace:
    FakeNativeDB.opened = []
    FakeNativeDB.opened_readers = []
    FakeNativeDB.handle = FakeNativeHandle()
    return types.SimpleNamespace(HelixDb=FakeNativeDB, HelixDbSource=FakeNativeSource)


class ClientTests(unittest.TestCase):
    def test_query_posts_query_with_headers(self) -> None:
        request = QueryRequest.read(
            read_batch().var_as("count", g().n_with_label("User").count()).returning(["count"])
        )

        calls = []

        def fake_urlopen(req):
            calls.append(req)
            return FakeResponse()

        with patch("helixdb.client.urlopen", fake_urlopen):
            result = (
                Client("http://127.0.0.1:6969", api_key="hx_secret")
                .request_builder()
                .writer_only()
                .warm_only()
                .should_await_durability(False)
                .query(request)
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

    def test_remote_error_includes_status_and_details(self) -> None:
        request = QueryRequest.read(read_batch())

        def fake_urlopen(req):
            raise HTTPError(req.full_url, 409, "Conflict", hdrs={}, fp=BytesIO(b"conflict"))

        with patch("helixdb.client.urlopen", fake_urlopen):
            with self.assertRaises(HelixError) as ctx:
                Client("http://127.0.0.1:6969").query(request)

        self.assertEqual(ctx.exception.kind, "Remote")
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.details, "conflict")

    def test_embedded_client_query_uses_native_handle(self) -> None:
        request = QueryRequest.read(
            read_batch().var_as("users", g().n_with_label("Missing").count()).returning(["users"])
        )

        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            client = Client.embedded(InMemory("py-sdk-embedded"))
            result = client.query(request)
            client.close()

        self.assertEqual(result, {"users": 0})
        self.assertEqual(FakeNativeDB.opened, [("IN_MEMORY", {"database": "py-sdk-embedded"})])
        self.assertEqual(json.loads(FakeNativeDB.handle.requests[0].decode("utf-8"))["request_type"], "read")
        self.assertTrue(FakeNativeDB.handle.closed)

    def test_embedded_reader_uses_native_open_reader(self) -> None:
        request = QueryRequest.read(
            read_batch().var_as("users", g().n_with_label("Missing").count()).returning(["users"])
        )

        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            client = Client.embedded_reader(Disk("/tmp/helix", "py-sdk-reader"))
            result = client.query(request)

        self.assertEqual(result, {"users": 0})
        self.assertEqual(
            FakeNativeDB.opened_readers,
            [("DISK", {"root": "/tmp/helix", "database": "py-sdk-reader"})],
        )

    def test_embedded_unavailable_without_native_bindings(self) -> None:
        with patch.dict(sys.modules, {"helixdb_uniffi": None}):
            with self.assertRaises(HelixError) as ctx:
                Client.embedded(InMemory("missing-native"))

        self.assertEqual(ctx.exception.kind, "EmbeddedUnavailable")

    def test_embedded_execute_rejects_server_options(self) -> None:
        request = QueryRequest.write(
            write_batch().var_as("created", g().add_n("User", {"name": "Ada"})).returning(["created"])
        )

        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            client = Client.embedded(InMemory("py-sdk-options"))
            with self.assertRaises(HelixError) as ctx:
                client.execute(request, writer_only=True)

        self.assertEqual(ctx.exception.kind, "InvalidRequest")
        self.assertIn("embedded mode does not support execute option(s): writer_only", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
