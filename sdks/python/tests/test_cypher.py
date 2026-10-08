from __future__ import annotations

import asyncio
import doctest
import json
import sys
import types
import unittest
import warnings
from io import BytesIO
from typing import Any
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import httpx
from test_client import FakeNativeError, FakeNativeSource, FakeResponse

from helixdb import AsyncClient, Client, HelixError, InMemory, _client_common
from helixdb._client_common import serialize_cypher

ROWS = {"columns": ["x"], "rows": [[1]]}
PLAN = {"plan": {"operator": "Return"}, "warnings": []}


def load_tests(
    loader: unittest.TestLoader, tests: unittest.TestSuite, pattern: str | None
) -> unittest.TestSuite:
    """Run the shared request-contract doc examples with ``unittest discover``."""

    tests.addTests(doctest.DocTestSuite(_client_common))
    return tests


class CypherHandle:
    """Native handle from a build with Cypher execution but no explain support."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.error: Exception | None = None

    async def cypher_json(self, body: bytes) -> bytes:
        self.calls.append(("cypher_json", json.loads(body)))
        if self.error is not None:
            raise self.error
        return json.dumps(ROWS).encode()

    async def close(self) -> None:
        return None


class ExplainCypherHandle(CypherHandle):
    """Native handle from a build with Cypher execution and explain support."""

    async def explain_cypher_json(self, body: bytes) -> bytes:
        self.calls.append(("explain_cypher_json", json.loads(body)))
        if self.error is not None:
            raise self.error
        return json.dumps(PLAN).encode()


def native_module(handle: CypherHandle) -> types.SimpleNamespace:
    async def open_handle(source: object) -> CypherHandle:
        return handle

    return types.SimpleNamespace(
        HelixDb=types.SimpleNamespace(open=open_handle),
        HelixDbSource=FakeNativeSource,
    )


class CypherContractTests(unittest.TestCase):
    def test_sync_route_and_lossless_response(self) -> None:
        response = {
            "columns": ["x"],
            "rows": [[{"$type": "integer", "value": "9223372036854775807"}]],
        }
        with patch(
            "helixdb.client.urlopen", return_value=FakeResponse(json.dumps(response).encode())
        ) as send:
            self.assertEqual(
                Client(api_key="local-test").cypher("RETURN $x", {"x": 2**63 - 1}), response
            )
        request = send.call_args.args[0]
        self.assertTrue(request.full_url.endswith("/v2/cypher"))
        self.assertEqual(request.get_header("Authorization"), "Bearer local-test")
        self.assertEqual(json.loads(request.data)["parameters"]["x"], 2**63 - 1)
        send.assert_called_once()

    def test_invalid_parameters_fail_before_io(self) -> None:
        for parameters in ([], "text", 7):
            with self.assertRaises(HelixError):
                serialize_cypher("RETURN 1", parameters, None)
        with self.assertRaises(HelixError):
            serialize_cypher("RETURN $x", {"x": float("nan")}, None)
        for query, query_name in ((7, None), ("RETURN 1", 7)):
            with self.assertRaises(HelixError) as invalid:
                serialize_cypher(query, None, query_name)  # type: ignore[arg-type]
            self.assertEqual(invalid.exception.kind, "InvalidRequest")
        self.assertEqual(json.loads(serialize_cypher("RETURN 1", None, None))["parameters"], {})

    def test_sync_options_and_database_id_headers(self) -> None:
        calls = []

        def fake_urlopen(request, timeout):
            calls.append((request, timeout))
            return FakeResponse(json.dumps(ROWS).encode())

        client = Client("http://127.0.0.1:6969/base", api_key="hx_secret", database_id="db-1")
        with patch("helixdb.client.urlopen", fake_urlopen):
            self.assertEqual(
                client.cypher("CREATE (n)", writer_only=True, await_durability=False), ROWS
            )
            client.cypher("RETURN 1", warm_only=True, timeout=2)
            client.with_database_id().with_api_key().cypher("RETURN 1")

        writer, warm, cleared = (request for request, _ in calls)
        self.assertEqual(writer.full_url, "http://127.0.0.1:6969/v2/cypher")
        self.assertEqual(writer.get_header("Authorization"), "Bearer hx_secret")
        self.assertEqual(writer.get_header("X-helix-database-id"), "db-1")
        self.assertEqual(writer.get_header("X-helix-require-writer"), "true")
        self.assertEqual(writer.get_header("X-helix-await-durable"), "false")
        self.assertIsNone(writer.get_header("X-helix-warm"))
        self.assertEqual(warm.get_header("X-helix-warm"), "true")
        self.assertIsNone(warm.get_header("X-helix-require-writer"))
        self.assertEqual([timeout for _, timeout in calls], [30, 2, 30])
        self.assertIsNone(cleared.get_header("X-helix-database-id"))
        self.assertIsNone(cleared.get_header("Authorization"))

    def test_sync_explain_route_body_and_response(self) -> None:
        calls = []

        def fake_urlopen(request, timeout):
            calls.append(request)
            return FakeResponse(json.dumps(PLAN).encode())

        client = Client(api_key="hx_secret", database_id="db-1")
        with patch("helixdb.client.urlopen", fake_urlopen):
            self.assertEqual(
                client.explain_cypher(
                    "MATCH (n) WHERE n.x = $x RETURN n",
                    {"x": 1},
                    query_name="plan",
                    writer_only=True,
                ),
                PLAN,
            )

        request = calls[0]
        self.assertEqual(request.full_url, "http://localhost:6969/v2/cypher/explain")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(
            request.data,
            serialize_cypher("MATCH (n) WHERE n.x = $x RETURN n", {"x": 1}, "plan"),
        )
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertEqual(request.get_header("Authorization"), "Bearer hx_secret")
        self.assertEqual(request.get_header("X-helix-database-id"), "db-1")
        self.assertEqual(request.get_header("X-helix-require-writer"), "true")

    def test_sync_warm_no_content_is_success(self) -> None:
        calls = []

        def fake_urlopen(request, timeout):
            calls.append(request)
            return FakeResponse(b"", status=204, reason="No Content")

        client = Client(database_id="db-1")
        with patch("helixdb.client.urlopen", fake_urlopen):
            self.assertEqual(
                client.cypher("MATCH (n) RETURN n", warm_only=True), {"columns": [], "rows": []}
            )
            self.assertIsNone(client.explain_cypher("MATCH (n) RETURN n", warm_only=True))

        self.assertEqual(
            [request.full_url for request in calls],
            ["http://localhost:6969/v2/cypher", "http://localhost:6969/v2/cypher/explain"],
        )
        self.assertTrue(all(request.get_header("X-helix-warm") == "true" for request in calls))
        self.assertTrue(
            all(request.get_header("X-helix-database-id") == "db-1" for request in calls)
        )

    def test_sync_explain_remote_error_and_unknown_option(self) -> None:
        def fake_urlopen(request, timeout):
            raise HTTPError(
                request.full_url,
                400,
                "Bad Request",
                hdrs={},
                fp=BytesIO(b'{"error":"tenant_id_required","msg":"database id required"}'),
            )

        with patch("helixdb.client.urlopen", fake_urlopen):
            with self.assertRaises(HelixError) as remote:
                Client().explain_cypher("RETURN 1")
        self.assertEqual(remote.exception.code, "tenant_id_required")
        self.assertEqual(remote.exception.status_code, 400)

        with patch("helixdb.client.urlopen", side_effect=URLError("connection refused")):
            with self.assertRaises(HelixError) as network:
                Client().explain_cypher("RETURN 1")
        self.assertEqual(network.exception.kind, "Network")

        with patch("helixdb.client.urlopen") as send:
            for method in (Client().cypher, Client().explain_cypher):
                with self.subTest(method=method.__name__):
                    with self.assertRaisesRegex(TypeError, "unknown execute option.*retries"):
                        method("RETURN 1", retries=3)
        send.assert_not_called()

    def test_sync_embedded_rejects_options_and_routes_to_native_methods(self) -> None:
        handle = ExplainCypherHandle()
        with patch.dict(sys.modules, {"helixdb_uniffi": native_module(handle)}):
            client = Client.embedded(InMemory("cypher-embedded"))
            self.assertIs(client.with_database_id("db-1"), client)
            for method in (client.cypher, client.explain_cypher):
                with self.subTest(method=method.__name__):
                    with self.assertRaises(HelixError) as rejected:
                        method("RETURN 1", warm_only=True)
                    self.assertEqual(rejected.exception.kind, "InvalidRequest")
                    self.assertIn(
                        "embedded mode does not support execute option(s): warm_only",
                        str(rejected.exception),
                    )
            self.assertEqual(client.cypher("RETURN $x", {"x": 1}), ROWS)
            self.assertEqual(client.explain_cypher("RETURN $x", {"x": 1}, query_name="q"), PLAN)
            handle.error = FakeNativeError("SyntaxError", "invalid syntax")
            with self.assertRaises(HelixError) as native_error:
                client.explain_cypher("RETURN")
            client.close()

        self.assertEqual(
            handle.calls[:2],
            [
                ("cypher_json", {"query": "RETURN $x", "parameters": {"x": 1}, "query_name": None}),
                (
                    "explain_cypher_json",
                    {"query": "RETURN $x", "parameters": {"x": 1}, "query_name": "q"},
                ),
            ],
        )
        self.assertEqual(native_error.exception.kind, "Embedded")
        self.assertEqual(native_error.exception.code, "SyntaxError")

    def test_sync_embedded_explain_inside_event_loop_keeps_runtime_error(self) -> None:
        with patch.dict(sys.modules, {"helixdb_uniffi": native_module(ExplainCypherHandle())}):
            client = Client.embedded(InMemory("cypher-active-loop"))

        async def explain_in_loop() -> str:
            try:
                client.explain_cypher("RETURN 1")
            except HelixError as error:
                return error.kind
            return "no error"

        with warnings.catch_warnings():
            # The rejected native coroutine is never awaited by design.
            warnings.simplefilter("ignore", RuntimeWarning)
            self.assertEqual(asyncio.run(explain_in_loop()), "EmbeddedRuntime")

    def test_sync_embedded_explain_requires_rebuilt_bindings(self) -> None:
        handle = CypherHandle()
        with patch.dict(sys.modules, {"helixdb_uniffi": native_module(handle)}):
            client = Client.embedded(InMemory("cypher-old-bindings"))
            self.assertEqual(client.cypher("RETURN 1"), ROWS)
            with self.assertRaises(HelixError) as unavailable:
                client.explain_cypher("RETURN 1")

        self.assertEqual(unavailable.exception.kind, "EmbeddedUnavailable")
        self.assertEqual(
            str(unavailable.exception), "rebuild native bindings with Cypher explain support"
        )
        self.assertEqual([name for name, _ in handle.calls], ["cypher_json"])


class AsyncCypherContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_route_and_error_detail(self) -> None:
        calls = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(
                400,
                json={
                    "error": "SyntaxError",
                    "msg": "invalid syntax",
                    "details": {"detail": "UndefinedVariable", "phase": "compile"},
                },
            )

        async with AsyncClient(
            api_key="local-test", transport=httpx.MockTransport(handler)
        ) as client:
            with self.assertRaises(HelixError) as error:
                await client.cypher("RETURN missing")
        self.assertEqual(error.exception.code, "SyntaxError")
        self.assertEqual(
            error.exception.server_details, {"detail": "UndefinedVariable", "phase": "compile"}
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].url.path, "/v2/cypher")
        self.assertEqual(calls[0].headers["authorization"], "Bearer local-test")

    async def test_async_options_database_id_and_explain(self) -> None:
        calls: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            if request.headers.get("x-helix-warm") == "true":
                return httpx.Response(204)
            if request.url.path == "/v2/cypher/explain":
                return httpx.Response(200, json=PLAN)
            return httpx.Response(200, json=ROWS)

        async with AsyncClient(
            "http://127.0.0.1:6969/base",
            api_key="hx_secret",
            database_id="db-1",
            timeout=5.0,
            transport=httpx.MockTransport(handler),
        ) as client:
            self.assertEqual(
                await client.cypher("CREATE (n)", writer_only=True, await_durability=True), ROWS
            )
            self.assertEqual(
                await client.cypher("MATCH (n) RETURN n", warm_only=True),
                {"columns": [], "rows": []},
            )
            self.assertIsNone(await client.explain_cypher("MATCH (n) RETURN n", warm_only=True))
            self.assertEqual(
                await client.explain_cypher("RETURN $x", {"x": 1}, query_name="q", timeout=0.5),
                PLAN,
            )
            client.with_database_id()
            await client.explain_cypher("RETURN 1")
            with self.assertRaisesRegex(TypeError, "unknown execute option.*retries"):
                await client.explain_cypher("RETURN 1", retries=3)

        writer, warm, warm_explain, explain, cleared = calls
        self.assertEqual(str(writer.url), "http://127.0.0.1:6969/v2/cypher")
        self.assertEqual(writer.headers["authorization"], "Bearer hx_secret")
        self.assertEqual(writer.headers["x-helix-database-id"], "db-1")
        self.assertEqual(writer.headers["x-helix-require-writer"], "true")
        self.assertEqual(writer.headers["x-helix-await-durable"], "true")
        self.assertNotIn("x-helix-warm", writer.headers)
        self.assertEqual(writer.extensions["timeout"]["read"], 5.0)
        self.assertEqual(warm.headers["x-helix-warm"], "true")
        self.assertEqual(warm_explain.url.path, "/v2/cypher/explain")
        self.assertEqual(warm_explain.headers["x-helix-warm"], "true")
        self.assertEqual(str(explain.url), "http://127.0.0.1:6969/v2/cypher/explain")
        self.assertEqual(explain.content, serialize_cypher("RETURN $x", {"x": 1}, "q"))
        self.assertEqual(explain.headers["content-type"], "application/json")
        self.assertEqual(explain.extensions["timeout"]["read"], 0.5)
        self.assertNotIn("x-helix-database-id", cleared.headers)
        self.assertEqual(cleared.url.path, "/v2/cypher/explain")

    async def test_async_explain_network_error(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        async with AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(HelixError) as network:
                await client.explain_cypher("RETURN 1")
        self.assertEqual(network.exception.kind, "Network")

    async def test_async_embedded_rejects_options_and_routes_to_native_methods(self) -> None:
        handle = ExplainCypherHandle()
        with patch.dict(sys.modules, {"helixdb_uniffi": native_module(handle)}):
            client = await AsyncClient.embedded(InMemory("async-cypher-embedded"))
            self.assertIs(client.with_database_id("db-1"), client)
            for method in (client.cypher, client.explain_cypher):
                with self.subTest(method=method.__name__):
                    with self.assertRaises(HelixError) as rejected:
                        await method("RETURN 1", writer_only=True)
                    self.assertEqual(rejected.exception.kind, "InvalidRequest")
            self.assertEqual(await client.cypher("RETURN 1"), ROWS)
            self.assertEqual(await client.explain_cypher("RETURN 1", query_name="q"), PLAN)
            handle.error = FakeNativeError("SyntaxError", "invalid syntax")
            with self.assertRaises(HelixError) as native_error:
                await client.explain_cypher("RETURN")
            await client.close()

        self.assertEqual(
            [name for name, _ in handle.calls],
            ["cypher_json", "explain_cypher_json", "explain_cypher_json"],
        )
        self.assertEqual(handle.calls[1][1]["query_name"], "q")
        self.assertEqual(native_error.exception.kind, "Embedded")
        self.assertEqual(native_error.exception.code, "SyntaxError")

    async def test_async_embedded_explain_requires_rebuilt_bindings(self) -> None:
        handle = CypherHandle()
        with patch.dict(sys.modules, {"helixdb_uniffi": native_module(handle)}):
            client = await AsyncClient.embedded(InMemory("async-cypher-old-bindings"))
            self.assertEqual(await client.cypher("RETURN 1"), ROWS)
            with self.assertRaises(HelixError) as unavailable:
                await client.explain_cypher("RETURN 1")
            await client.close()

        self.assertEqual(unavailable.exception.kind, "EmbeddedUnavailable")
        self.assertEqual(
            str(unavailable.exception), "rebuild native bindings with Cypher explain support"
        )
