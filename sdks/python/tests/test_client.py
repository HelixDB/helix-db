from __future__ import annotations

import asyncio
import json
import os
import sys
import types
import unittest
from io import BytesIO
from unittest.mock import patch
from urllib.error import HTTPError

from helixdb import (
    Client,
    Disk,
    EmbeddedCacheConfig,
    HelixError,
    HybridCache,
    InMemory,
    MemoryCache,
    QueryBuilder,
    QueryExecutionRequest,
    QueryRequest,
    g,
    read_batch,
    write_batch,
)
from helixdb._client_common import remote_error


def public_api_members(type_: type) -> set[str]:
    """Return methods and properties that form a class's named public API."""

    return {
        name
        for name, member in vars(type_).items()
        if not name.startswith("_")
        and (callable(member) or isinstance(member, (classmethod, staticmethod, property)))
    }


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
        self.gate: asyncio.Event | None = None
        self.active = 0
        self.max_active = 0
        self.error: Exception | None = None

    async def query_json(self, body: bytes) -> bytes:
        self.requests.append(bytes(body))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.gate is not None:
                await self.gate.wait()
            if self.error is not None:
                raise self.error
            return b'{"users":0}'
        finally:
            self.active -= 1

    async def close(self) -> None:
        self.closed = True


class FakeNativeError(Exception):
    def __init__(self, error: str, msg: str) -> None:
        super().__init__(msg)
        self.error = error
        self.msg = msg


class FakeNativeDB:
    opened: list[object] = []
    opened_readers: list[object] = []
    configured: list[tuple[object, object]] = []
    configured_readers: list[tuple[object, object]] = []
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

    @classmethod
    async def open_with_config(cls, source: object, cache: object) -> FakeNativeHandle:
        cls.configured.append((source, cache))
        return cls.handle

    @classmethod
    async def open_reader_with_config(cls, source: object, cache: object) -> FakeNativeHandle:
        cls.configured_readers.append((source, cache))
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


class FakeNativeCacheMode:
    @staticmethod
    def VECTOR_MEMORY_ONLY() -> str:
        return "VECTOR_MEMORY_ONLY"

    @staticmethod
    def MEMORY() -> str:
        return "MEMORY"

    @staticmethod
    def HYBRID(**kwargs: object) -> tuple[str, dict[str, object]]:
        return ("HYBRID", kwargs)


class FakeNativeCacheConfig:
    def __new__(cls, **kwargs: object) -> tuple[str, dict[str, object]]:
        return ("CACHE", kwargs)


def fake_native_module() -> types.SimpleNamespace:
    FakeNativeDB.opened = []
    FakeNativeDB.opened_readers = []
    FakeNativeDB.configured = []
    FakeNativeDB.configured_readers = []
    FakeNativeDB.handle = FakeNativeHandle()
    return types.SimpleNamespace(
        HelixDb=FakeNativeDB,
        HelixDbSource=FakeNativeSource,
        EmbeddedCacheConfig=FakeNativeCacheConfig,
        EmbeddedCacheMode=FakeNativeCacheMode,
    )


class ClientTests(unittest.TestCase):
    def test_lost_response_gateway_probe(self) -> None:
        base_url = os.environ.get("HELIX_LOST_RESPONSE_GATEWAY_URL")
        if base_url is None:
            self.skipTest("real lost-response gateway fixture is not configured")
        probe_id = os.environ["HELIX_LOST_RESPONSE_PROBE_ID"]
        request = QueryRequest.write(
            write_batch()
            .var_as(
                "created",
                g().add_n("LostResponseProbe", {"business_id": probe_id}),
            )
            .returning(["created"])
        )

        with self.assertRaises(HelixError) as ctx:
            Client(
                base_url,
                api_key=os.environ.get("HELIX_LOST_RESPONSE_API_KEY"),
            ).query(request)

        self.assertEqual(ctx.exception.status_code, 503)
        self.assertEqual(ctx.exception.code, "WRITE_OUTCOME_UNKNOWN")
        self.assertIs(ctx.exception.retryable, False)
        self.assertFalse(ctx.exception.is_retryable())

    def test_public_client_api_is_explicitly_accounted_for(self) -> None:
        self.assertEqual(
            public_api_members(Client),
            {
                "server",
                "embedded",
                "embedded_reader",
                "with_api_key",
                "request_builder",
                "query",
                "base_url",
                "execute",
                "graph",
                "close",
            },
        )
        self.assertEqual(
            public_api_members(QueryBuilder),
            {"writer_only", "warm_only", "should_await_durability", "query"},
        )
        self.assertEqual(public_api_members(QueryExecutionRequest), {"send_bytes", "send"})

    def test_server_convenience_methods_and_raw_response(self) -> None:
        request = QueryRequest.read(read_batch())
        calls = []

        def fake_urlopen(req):
            calls.append(req)
            return FakeResponse()

        client = Client.server("http://127.0.0.1:6969/base", api_key="first")
        self.assertEqual(client.base_url, "http://127.0.0.1:6969/base")
        first_request = client.query().query(request)
        self.assertIs(client.with_api_key("second"), client)

        with patch("helixdb.client.urlopen", fake_urlopen):
            self.assertEqual(first_request.send_bytes(), b'{"ok":true}')
            self.assertEqual(
                client.execute(
                    request,
                    writer_only=True,
                    warm_only=True,
                    await_durability=True,
                ),
                {"ok": True},
            )
        client.close()

        self.assertEqual(calls[0].headers["Authorization"], "Bearer first")
        self.assertEqual(calls[0].full_url, "http://127.0.0.1:6969/v2/query")
        self.assertEqual(calls[1].headers["Authorization"], "Bearer second")
        self.assertEqual(calls[1].headers["X-helix-require-writer"], "true")
        self.assertEqual(calls[1].headers["X-helix-warm"], "true")
        self.assertEqual(calls[1].headers["X-helix-await-durable"], "true")

    def test_graph_delegates_to_graph_loader(self) -> None:
        client = Client.server()
        selection = object()
        loaded = object()

        with patch("helixdb.graph.load_graph", return_value=loaded) as load_graph:
            self.assertIs(client.graph(selection), loaded)

        load_graph.assert_called_once_with(client, selection)

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
        self.assertEqual(req.full_url, "http://127.0.0.1:6969/v2/query")
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
        self.assertIsNone(ctx.exception.code)
        self.assertIsNone(ctx.exception.retryable)
        self.assertFalse(ctx.exception.is_retryable())

    def test_unknown_write_outcome_is_terminal_and_sent_once(self) -> None:
        request = QueryRequest.write(
            write_batch()
            .var_as("created", g().add_n("User", {"name": "Ada"}))
            .returning(["created"])
        )
        cases = [
            (
                b'{"error":"writer_fenced_commit_outcome_unknown",'
                b'"msg":"write outcome is unknown","retryable":false}',
                "writer_fenced_commit_outcome_unknown",
            ),
            (
                b'{"code":"WRITE_OUTCOME_UNKNOWN",'
                b'"error":"write outcome is unknown","retryable":false}',
                "WRITE_OUTCOME_UNKNOWN",
            ),
        ]
        for body, expected_code in cases:
            with self.subTest(expected_code=expected_code):
                calls = 0

                def fake_urlopen(req):
                    nonlocal calls
                    calls += 1
                    raise HTTPError(
                        req.full_url,
                        503,
                        "Service Unavailable",
                        hdrs={},
                        fp=BytesIO(body),
                    )

                with patch("helixdb.client.urlopen", fake_urlopen):
                    with self.assertRaises(HelixError) as ctx:
                        Client("http://127.0.0.1:6969").query(request)

                self.assertEqual(calls, 1)
                self.assertEqual(ctx.exception.code, expected_code)
                self.assertIs(ctx.exception.retryable, False)
                self.assertFalse(ctx.exception.is_retryable())

    def test_retryability_requires_explicit_boolean_true(self) -> None:
        cases = [
            (b'{"error":"busy","retryable":true}', True),
            (b'{"error":"unknown","retryable":false}', False),
            (b'{"error":"malformed","retryable":"true"}', False),
            (b'{"error":"missing"}', False),
        ]
        for body, expected in cases:
            with self.subTest(body=body):
                self.assertIs(remote_error(body, "fallback").is_retryable(), expected)

    def test_remote_error_parses_new_legacy_future_missing_and_malformed_bodies(self) -> None:
        request = QueryRequest.read(read_batch())
        cases = [
            (
                b'{"error":"index_not_found","msg":"missing index"}',
                "index_not_found",
                "missing index",
            ),
            (
                b'{"error":"legacy message","code":"index_not_found"}',
                "index_not_found",
                "legacy message",
            ),
            (b'{"error":"future_code","msg":"future message"}', "future_code", "future message"),
            (b'{"error":"message without code"}', None, "message without code"),
            (b"not JSON", None, "not JSON"),
        ]

        for body, expected_code, expected_details in cases:
            with self.subTest(body=body):

                def fake_urlopen(req, response_body=body):
                    raise HTTPError(
                        req.full_url,
                        500,
                        "Internal Server Error",
                        hdrs={},
                        fp=BytesIO(response_body),
                    )

                with patch("helixdb.client.urlopen", fake_urlopen):
                    with self.assertRaises(HelixError) as ctx:
                        Client("http://127.0.0.1:6969").query(request)

                self.assertEqual(ctx.exception.code, expected_code)
                self.assertEqual(ctx.exception.details, expected_details)

    def test_embedded_error_preserves_uniffi_code_and_message_fields(self) -> None:
        request = QueryRequest.read(read_batch())

        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            client = Client.embedded(InMemory("py-sdk-embedded-error"))
            FakeNativeDB.handle.error = FakeNativeError(
                "index_not_found",
                "missing text index",
            )
            with self.assertRaises(HelixError) as ctx:
                client.query(request)

        self.assertEqual(ctx.exception.kind, "Embedded")
        self.assertEqual(ctx.exception.code, "index_not_found")
        self.assertEqual(ctx.exception.details, "missing text index")

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
        self.assertEqual(
            json.loads(FakeNativeDB.handle.requests[0].decode("utf-8"))["request_type"],
            "read",
        )
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

    def test_embedded_cache_config_maps_hybrid_and_memory_profiles(self) -> None:
        hybrid = EmbeddedCacheConfig(
            vector_memory_bytes=1024,
            mode=HybridCache(2048, "/tmp/slate", 4096, "/tmp/object", 8192),
        )
        memory = EmbeddedCacheConfig(vector_memory_bytes=512, mode=MemoryCache())

        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            Client.embedded(InMemory("configured-writer"), cache=hybrid)
            Client.embedded_reader(InMemory("configured-reader"), cache=memory)

        self.assertEqual(
            FakeNativeDB.configured[0][1],
            (
                "CACHE",
                {
                    "vector_memory_bytes": 1024,
                    "mode": (
                        "HYBRID",
                        {
                            "slate_memory_bytes": 2048,
                            "slate_disk_path": "/tmp/slate",
                            "slate_disk_bytes": 4096,
                            "object_store_disk_path": "/tmp/object",
                            "object_store_disk_bytes": 8192,
                        },
                    ),
                },
            ),
        )
        self.assertEqual(
            FakeNativeDB.configured_readers[0][1],
            ("CACHE", {"vector_memory_bytes": 512, "mode": "MEMORY"}),
        )

    def test_embedded_unavailable_without_native_bindings(self) -> None:
        with patch.dict(sys.modules, {"helixdb_uniffi": None}):
            with self.assertRaises(HelixError) as ctx:
                Client.embedded(InMemory("missing-native"))

        self.assertEqual(ctx.exception.kind, "EmbeddedUnavailable")

    def test_embedded_execute_rejects_server_options(self) -> None:
        request = QueryRequest.write(
            write_batch()
            .var_as("created", g().add_n("User", {"name": "Ada"}))
            .returning(["created"])
        )

        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            client = Client.embedded(InMemory("py-sdk-options"))
            with self.assertRaises(HelixError) as ctx:
                client.execute(request, writer_only=True)

        self.assertEqual(ctx.exception.kind, "InvalidRequest")
        self.assertIn(
            "embedded mode does not support execute option(s): writer_only",
            str(ctx.exception),
        )


if __name__ == "__main__":
    unittest.main()
