from __future__ import annotations

import asyncio
import json
import sys
import unittest
from unittest.mock import patch

import httpx
from test_client import FakeNativeDB, fake_native_module, public_api_members

from helix_db import AsyncQueryBuilder, AsyncQueryExecutionRequest
from helixdb import (
    AsyncClient,
    Disk,
    EmbeddedCacheConfig,
    HelixError,
    HybridCache,
    InMemory,
    QueryRequest,
    g,
    read_batch,
)


def read_request() -> QueryRequest:
    return QueryRequest.read(
        read_batch().var_as("users", g().n_with_label("User").count()).returning(["users"])
    )


class ControlledStream(httpx.AsyncByteStream):
    def __init__(self, body: bytes, *, block: bool = False, fail: bool = False) -> None:
        self.body = body
        self.block = block
        self.fail = fail
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = False
        self.request: httpx.Request | None = None

    async def __aiter__(self):
        self.started.set()
        if self.fail:
            assert self.request is not None
            raise httpx.ReadTimeout("timed out", request=self.request)
        if self.block:
            await self.release.wait()
        yield self.body

    async def aclose(self) -> None:
        self.closed = True


class SequencedTransport(httpx.AsyncBaseTransport):
    def __init__(self, first: ControlledStream, *, first_status: int = 200) -> None:
        self.first = first
        self.first_status = first_status
        self.calls = 0
        self.closed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if self.calls == 1:
            self.first.request = request
            return httpx.Response(self.first_status, stream=self.first)
        return httpx.Response(200, json={"reused": True})

    async def aclose(self) -> None:
        self.closed = True


class AsyncClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_async_client_api_is_explicitly_accounted_for(self) -> None:
        self.assertEqual(
            public_api_members(AsyncClient),
            {
                "server",
                "embedded",
                "embedded_reader",
                "with_api_key",
                "request_builder",
                "query",
                "execute",
                "base_url",
                "close",
            },
        )
        self.assertEqual(
            public_api_members(AsyncQueryBuilder),
            {"writer_only", "warm_only", "should_await_durability", "query"},
        )
        self.assertEqual(
            public_api_members(AsyncQueryExecutionRequest),
            {"send_bytes", "send"},
        )

    async def test_server_constructor_and_raw_response(self) -> None:
        calls: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, content=b'{"raw":true}')

        client = AsyncClient.server(
            "http://127.0.0.1:6969/base",
            api_key="hx_secret",
            transport=httpx.MockTransport(handler),
        )
        self.assertEqual(client.base_url, "http://127.0.0.1:6969/base")
        response = await client.request_builder().query(read_request()).send_bytes()
        await client.close()

        self.assertEqual(response, b'{"raw":true}')
        self.assertEqual(str(calls[0].url), "http://127.0.0.1:6969/v2/query")
        self.assertEqual(calls[0].headers["authorization"], "Bearer hx_secret")

    async def test_query_posts_query_with_headers_and_timeout(self) -> None:
        calls: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True})

        async with AsyncClient(
            "http://127.0.0.1:6969/base",
            api_key="hx_secret",
            timeout=5.0,
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
            transport=httpx.MockTransport(handler),
        ) as client:
            result = await (
                client.request_builder()
                .writer_only()
                .warm_only()
                .should_await_durability(False)
                .query(read_request())
                .send(timeout=0.25)
            )

        self.assertEqual(result, {"ok": True})
        request = calls[0]
        self.assertEqual(str(request.url), "http://127.0.0.1:6969/v2/query")
        self.assertEqual(request.headers["authorization"], "Bearer hx_secret")
        self.assertEqual(request.headers["x-helix-require-writer"], "true")
        self.assertEqual(request.headers["x-helix-warm"], "true")
        self.assertEqual(request.headers["x-helix-await-durable"], "false")
        self.assertEqual(request.extensions["timeout"]["read"], 0.25)
        self.assertEqual(json.loads(request.content)["request_type"], "read")

    async def test_default_timeout_is_disabled(self) -> None:
        calls: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, content=b"")

        async with AsyncClient(transport=httpx.MockTransport(handler)) as client:
            self.assertIsNone(await client.query(read_request()))

        self.assertTrue(all(value is None for value in calls[0].extensions["timeout"].values()))

    async def test_execute_validates_options_and_updates_api_key(self) -> None:
        calls: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True})

        client = AsyncClient(transport=httpx.MockTransport(handler)).with_api_key("first")
        builder = client.request_builder()
        client.with_api_key("second")

        await builder.query(read_request()).send()
        await client.execute(
            read_request(),
            writer_only=True,
            warm_only=True,
            await_durability=True,
        )
        with self.assertRaisesRegex(TypeError, "unknown execute option.*unknown"):
            await client.execute(read_request(), unknown=True)
        await client.close()

        self.assertEqual(calls[0].headers["authorization"], "Bearer first")
        self.assertEqual(calls[1].headers["authorization"], "Bearer second")
        self.assertEqual(calls[1].headers["x-helix-require-writer"], "true")
        self.assertEqual(calls[1].headers["x-helix-warm"], "true")
        self.assertEqual(calls[1].headers["x-helix-await-durable"], "true")

    async def test_empty_invalid_remote_and_network_responses_match_sync_contract(
        self,
    ) -> None:
        responses = [
            httpx.Response(200, content=b""),
            httpx.Response(200, content=b"not-json"),
            httpx.Response(409, content=b"conflict"),
            httpx.Response(503, content=b""),
        ]

        async def handler(request: httpx.Request) -> httpx.Response:
            return responses.pop(0)

        async with AsyncClient(transport=httpx.MockTransport(handler)) as client:
            self.assertIsNone(await client.query(read_request()))
            with self.assertRaises(HelixError) as invalid:
                await client.query(read_request())
            with self.assertRaises(HelixError) as conflict:
                await client.query(read_request())
            with self.assertRaises(HelixError) as unavailable:
                await client.query(read_request())

        self.assertEqual(invalid.exception.kind, "Serialization")
        self.assertEqual(conflict.exception.kind, "Remote")
        self.assertEqual(conflict.exception.status_code, 409)
        self.assertEqual(conflict.exception.details, "conflict")
        self.assertEqual(unavailable.exception.details, "Service Unavailable")

        async def network_error(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        async with AsyncClient(transport=httpx.MockTransport(network_error)) as client:
            with self.assertRaises(HelixError) as network:
                await client.query(read_request())
        self.assertEqual(network.exception.kind, "Network")
        self.assertIsInstance(network.exception.__cause__, httpx.ConnectError)

    async def test_timeout_closes_response_and_client_is_reusable(self) -> None:
        stream = ControlledStream(b"", fail=True)
        transport = SequencedTransport(stream)

        async with AsyncClient(transport=transport) as client:
            with self.assertRaises(HelixError) as timeout:
                await client.query(read_request())
            self.assertTrue(stream.closed)
            self.assertEqual(await client.query(read_request()), {"reused": True})

        self.assertEqual(timeout.exception.kind, "Network")
        self.assertIsInstance(timeout.exception.__cause__, httpx.ReadTimeout)
        self.assertTrue(transport.closed)

    async def test_success_and_remote_error_close_response_streams(self) -> None:
        success_stream = ControlledStream(b'{"ok":true}')
        success_transport = SequencedTransport(success_stream)
        async with AsyncClient(transport=success_transport) as client:
            self.assertEqual(await client.query(read_request()), {"ok": True})
        self.assertTrue(success_stream.closed)

        error_stream = ControlledStream(b"busy")
        error_transport = SequencedTransport(error_stream, first_status=503)
        async with AsyncClient(transport=error_transport) as client:
            with self.assertRaises(HelixError) as remote:
                await client.query(read_request())
            self.assertEqual(await client.query(read_request()), {"reused": True})
        self.assertEqual(remote.exception.kind, "Remote")
        self.assertEqual(remote.exception.status_code, 503)
        self.assertTrue(error_stream.closed)

    async def test_warm_no_content_is_success(self) -> None:
        calls: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(204)

        async with AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await client.execute(read_request(), warm_only=True)

        self.assertIsNone(result)
        self.assertEqual(calls[0].headers["x-helix-warm"], "true")

    async def test_cancellation_closes_response_and_client_is_reusable(self) -> None:
        stream = ControlledStream(b'{"blocked":true}', block=True)
        transport = SequencedTransport(stream)

        async with AsyncClient(transport=transport) as client:
            pending = asyncio.create_task(client.query(read_request()))
            await stream.started.wait()
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
            self.assertTrue(stream.closed)
            self.assertEqual(await client.query(read_request()), {"reused": True})

    async def test_context_close_is_idempotent_and_closed_requests_fail(self) -> None:
        transport = SequencedTransport(ControlledStream(b'{"ok":true}'))
        client = AsyncClient(transport=transport)
        pending = client.request_builder().query(read_request())

        async with client as entered:
            self.assertIs(entered, client)
            self.assertEqual(client.base_url, "http://localhost:6969")

        await client.close()
        self.assertTrue(transport.closed)
        for operation in (
            lambda: client.request_builder(),
            lambda: client.with_api_key("closed"),
            lambda: client.base_url,
        ):
            with self.assertRaises(HelixError) as closed:
                operation()
            self.assertEqual(closed.exception.kind, "InvalidRequest")
        with self.assertRaises(HelixError) as closed_request:
            await pending.send()
        self.assertEqual(closed_request.exception.kind, "InvalidRequest")

    async def test_invalid_url_fails_before_transport_creation(self) -> None:
        for url in (
            "localhost:6969",
            "ftp://localhost/query",
            "http://[::1",
            "http://bad host/query",
        ):
            with self.subTest(url=url):
                with self.assertRaises(HelixError) as invalid:
                    AsyncClient(url)
                self.assertEqual(invalid.exception.kind, "InvalidUrl")

    async def test_embedded_writer_queries_overlap_and_close_in_active_loop(
        self,
    ) -> None:
        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            client = await AsyncClient.embedded(InMemory("async-embedded"))
            handle = FakeNativeDB.handle
            handle.gate = asyncio.Event()
            tasks = [asyncio.create_task(client.query(read_request())) for _ in range(3)]
            for _ in range(100):
                if handle.active == 3:
                    break
                await asyncio.sleep(0)
            self.assertEqual(handle.active, 3)
            self.assertEqual(handle.max_active, 3)
            handle.gate.set()
            self.assertEqual(
                await asyncio.gather(*tasks),
                [{"users": 0}, {"users": 0}, {"users": 0}],
            )
            await client.close()
            await client.close()

        self.assertEqual(FakeNativeDB.opened, [("IN_MEMORY", {"database": "async-embedded"})])
        self.assertTrue(handle.closed)

    async def test_embedded_cancellation_propagates_and_client_is_reusable(self) -> None:
        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            client = await AsyncClient.embedded(InMemory("async-cancellation"))
            handle = FakeNativeDB.handle
            handle.gate = asyncio.Event()
            pending = asyncio.create_task(client.query(read_request()))
            while handle.active == 0:
                await asyncio.sleep(0)
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
            self.assertEqual(handle.active, 0)
            handle.gate.set()
            self.assertEqual(await client.query(read_request()), {"users": 0})
            await client.close()

    async def test_serialization_errors_match_in_server_and_embedded_modes(self) -> None:
        class InvalidRequest:
            def to_json_bytes(self) -> bytes:
                raise ValueError("cannot encode")

        async with AsyncClient(transport=httpx.MockTransport(lambda request: None)) as client:
            with self.assertRaises(HelixError) as server:
                await client.query(InvalidRequest())  # type: ignore[arg-type]

        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            client = await AsyncClient.embedded(InMemory("async-serialization"))
            with self.assertRaises(HelixError) as embedded:
                await client.query(InvalidRequest())  # type: ignore[arg-type]
            await client.close()

        self.assertEqual(server.exception.kind, "Serialization")
        self.assertEqual(embedded.exception.kind, "Serialization")

    async def test_embedded_reader_cache_errors_and_server_options(self) -> None:
        cache = EmbeddedCacheConfig(
            vector_memory_bytes=1024,
            mode=HybridCache(2048, "/tmp/slate", 4096, "/tmp/object", 8192),
        )
        with patch.dict(sys.modules, {"helixdb_uniffi": fake_native_module()}):
            client = await AsyncClient.embedded_reader(
                Disk("/tmp/helix", "async-reader"), cache=cache
            )
            self.assertEqual(client.base_url, "embedded://helixdb")
            with self.assertRaises(HelixError) as execute_option:
                await client.execute(read_request(), writer_only=True)
            with self.assertRaises(HelixError) as builder_option:
                await client.request_builder().warm_only().query(read_request()).send()
            with self.assertRaises(HelixError) as timeout:
                await client.query(read_request(), timeout=1.0)
            FakeNativeDB.handle.error = RuntimeError("native failure")
            with self.assertRaises(HelixError) as embedded:
                await client.query(read_request())
            await client.close()

        self.assertEqual(execute_option.exception.kind, "InvalidRequest")
        self.assertEqual(builder_option.exception.kind, "InvalidRequest")
        self.assertEqual(timeout.exception.kind, "InvalidRequest")
        self.assertEqual(embedded.exception.kind, "Embedded")
        self.assertEqual(FakeNativeDB.opened_readers, [])
        self.assertEqual(
            FakeNativeDB.configured_readers[0][0],
            ("DISK", {"root": "/tmp/helix", "database": "async-reader"}),
        )
        self.assertEqual(FakeNativeDB.configured_readers[0][1][0], "CACHE")

    async def test_embedded_unavailable_and_public_exports(self) -> None:
        with patch.dict(sys.modules, {"helixdb_uniffi": None}):
            with self.assertRaises(HelixError) as unavailable:
                await AsyncClient.embedded(InMemory("missing"))

        self.assertEqual(unavailable.exception.kind, "EmbeddedUnavailable")
        self.assertIsNotNone(AsyncQueryBuilder)
        self.assertIsNotNone(AsyncQueryExecutionRequest)


if __name__ == "__main__":
    unittest.main()
