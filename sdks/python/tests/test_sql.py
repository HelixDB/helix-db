from __future__ import annotations

import json
import sys
import types
import unittest
from typing import Any
from unittest.mock import patch

import httpx
from test_client import FakeNativeError, FakeNativeSource, FakeResponse

from helixdb import AsyncClient, Client, HelixError, InMemory
from helixdb._client_common import serialize_sql

ROWS = {"columns": ["name"], "rows": [["Ada"]]}
PLAN = {"effect": "Read", "operators": []}
MAPPINGS = {"tables": {"people": "Person"}, "join_columns": {"people.team_id": "MEMBER_OF"}}


class SqlHandle:
    """Native handle from a build with SQL support; ``legacy`` predates it."""

    def __init__(self, *, legacy: bool = False) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.error: Exception | None = None
        if not legacy:
            self.sql_json = self._respond("sql_json", ROWS)
            self.explain_sql_json = self._respond("explain_sql_json", PLAN)

    def _respond(self, name: str, result: dict[str, Any]) -> Any:
        async def method(body: bytes) -> bytes:
            self.calls.append((name, json.loads(body)))
            if self.error is not None:
                raise self.error
            return json.dumps(result).encode()

        return method

    async def close(self) -> None:
        return None


def native_module(handle: SqlHandle) -> types.SimpleNamespace:
    async def open_handle(source: object) -> SqlHandle:
        return handle

    return types.SimpleNamespace(
        HelixDb=types.SimpleNamespace(open=open_handle),
        HelixDbSource=FakeNativeSource,
    )


class SqlContractTests(unittest.TestCase):
    def test_serialization_validates_before_io(self) -> None:
        self.assertEqual(
            json.loads(serialize_sql("SELECT ?", None, None, None)),
            {"query": "SELECT ?", "parameters": [], "query_name": None, "mappings": {}},
        )
        self.assertEqual(
            json.loads(serialize_sql("SELECT :a", {"a": 2**63 - 1}, MAPPINGS, "q")),
            {
                "query": "SELECT :a",
                "parameters": {"a": 2**63 - 1},
                "query_name": "q",
                "mappings": MAPPINGS,
            },
        )
        for query, parameters, mappings, query_name in (
            (7, None, None, None),
            ("SELECT 1", "text", None, None),
            ("SELECT 1", 7, None, None),
            ("SELECT 1", None, ["tables"], None),
            ("SELECT 1", None, None, 7),
        ):
            with self.assertRaises(HelixError) as invalid:
                serialize_sql(query, parameters, mappings, query_name)  # type: ignore[arg-type]
            self.assertEqual(invalid.exception.kind, "InvalidRequest")
        with self.assertRaises(HelixError) as unencodable:
            serialize_sql("SELECT ?", [float("nan")], None, None)
        self.assertEqual(unencodable.exception.kind, "Serialization")

    def test_sync_routes_bodies_options_and_warm_success(self) -> None:
        calls = []

        def fake_urlopen(request, timeout):
            calls.append(request)
            if request.get_header("X-helix-warm") == "true":
                return FakeResponse(b"", status=204, reason="No Content")
            body = PLAN if request.full_url.endswith("/explain") else ROWS
            return FakeResponse(json.dumps(body).encode())

        client = Client(api_key="hx_secret", database_id="db-1")
        with patch("helixdb.client.urlopen", fake_urlopen):
            self.assertEqual(
                client.sql(
                    "INSERT INTO people (name) VALUES (?)",
                    ("Ada",),
                    mappings=MAPPINGS,
                    query_name="insert",
                    writer_only=True,
                ),
                ROWS,
            )
            self.assertEqual(client.explain_sql("SELECT p.name FROM people p"), PLAN)
            self.assertEqual(client.sql("SELECT 1", warm_only=True), {"columns": [], "rows": []})
            self.assertIsNone(client.explain_sql("SELECT 1", warm_only=True))
            with self.assertRaisesRegex(TypeError, "unknown execute option.*retries"):
                client.sql("SELECT 1", retries=3)

        insert, explain, warm, warm_explain = calls
        self.assertEqual(insert.full_url, "http://localhost:6969/v2/sql")
        self.assertEqual(
            json.loads(insert.data),
            {
                "query": "INSERT INTO people (name) VALUES (?)",
                "parameters": ["Ada"],
                "query_name": "insert",
                "mappings": MAPPINGS,
            },
        )
        self.assertEqual(insert.get_header("Authorization"), "Bearer hx_secret")
        self.assertEqual(insert.get_header("X-helix-database-id"), "db-1")
        self.assertEqual(insert.get_header("X-helix-require-writer"), "true")
        self.assertEqual(explain.full_url, "http://localhost:6969/v2/sql/explain")
        self.assertEqual(warm.full_url, "http://localhost:6969/v2/sql")
        self.assertEqual(warm_explain.full_url, "http://localhost:6969/v2/sql/explain")

    def test_sync_embedded_routes_to_native_methods(self) -> None:
        handle = SqlHandle()
        with patch.dict(sys.modules, {"helixdb_uniffi": native_module(handle)}):
            client = Client.embedded(InMemory("sql-embedded"))
            with self.assertRaises(HelixError) as rejected:
                client.sql("SELECT 1", warm_only=True)
            self.assertEqual(rejected.exception.kind, "InvalidRequest")
            self.assertEqual(client.sql("SELECT ?", [1], mappings=MAPPINGS), ROWS)
            self.assertEqual(client.explain_sql("SELECT :x", {"x": 1}, query_name="q"), PLAN)
            handle.error = FakeNativeError(
                "unsupported_feature:compile:procedures_and_subqueries", "no subqueries"
            )
            with self.assertRaises(HelixError) as native_error:
                client.sql("SELECT (SELECT 1)")
            client.close()

        self.assertEqual(
            handle.calls[:2],
            [
                (
                    "sql_json",
                    {
                        "query": "SELECT ?",
                        "parameters": [1],
                        "query_name": None,
                        "mappings": MAPPINGS,
                    },
                ),
                (
                    "explain_sql_json",
                    {
                        "query": "SELECT :x",
                        "parameters": {"x": 1},
                        "query_name": "q",
                        "mappings": {},
                    },
                ),
            ],
        )
        self.assertEqual(native_error.exception.kind, "Embedded")
        self.assertEqual(
            native_error.exception.code, "unsupported_feature:compile:procedures_and_subqueries"
        )

    def test_sync_embedded_requires_rebuilt_bindings(self) -> None:
        with patch.dict(sys.modules, {"helixdb_uniffi": native_module(SqlHandle(legacy=True))}):
            client = Client.embedded(InMemory("sql-old-bindings"))
            for method in (client.sql, client.explain_sql):
                with self.subTest(method=method.__name__):
                    with self.assertRaises(HelixError) as unavailable:
                        method("SELECT 1")
                    self.assertEqual(unavailable.exception.kind, "EmbeddedUnavailable")
                    self.assertEqual(
                        str(unavailable.exception), "rebuild native bindings with SQL support"
                    )


class AsyncSqlContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_routes_bodies_errors_and_warm_success(self) -> None:
        calls: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            if request.headers.get("x-helix-warm") == "true":
                return httpx.Response(204)
            if request.url.path == "/v2/sql/explain":
                return httpx.Response(200, json=PLAN)
            if json.loads(request.content)["query"] == "SELECT (SELECT 1)":
                return httpx.Response(
                    400,
                    json={
                        "error": "unsupported_feature",
                        "msg": "no subqueries",
                        "details": {"detail": "procedures_and_subqueries", "phase": "compile"},
                    },
                )
            return httpx.Response(200, json=ROWS)

        async with AsyncClient(
            "http://127.0.0.1:6969/base",
            database_id="db-1",
            transport=httpx.MockTransport(handler),
        ) as client:
            self.assertEqual(
                await client.sql("SELECT ?", [1], mappings=MAPPINGS, writer_only=True), ROWS
            )
            self.assertEqual(await client.explain_sql("SELECT 1", timeout=0.5), PLAN)
            self.assertIsNone(await client.explain_sql("SELECT 1", warm_only=True))
            with self.assertRaises(HelixError) as remote:
                await client.sql("SELECT (SELECT 1)")

        execute, explain, warm, rejected = calls
        self.assertEqual(str(execute.url), "http://127.0.0.1:6969/v2/sql")
        self.assertEqual(
            json.loads(execute.content),
            {"query": "SELECT ?", "parameters": [1], "query_name": None, "mappings": MAPPINGS},
        )
        self.assertEqual(execute.headers["x-helix-database-id"], "db-1")
        self.assertEqual(execute.headers["x-helix-require-writer"], "true")
        self.assertEqual(explain.url.path, "/v2/sql/explain")
        self.assertEqual(explain.extensions["timeout"]["read"], 0.5)
        self.assertEqual(warm.url.path, "/v2/sql/explain")
        self.assertEqual(rejected.url.path, "/v2/sql")
        self.assertEqual(remote.exception.code, "unsupported_feature")
        self.assertEqual(remote.exception.status_code, 400)

    async def test_async_embedded_routes_and_requires_rebuilt_bindings(self) -> None:
        handle = SqlHandle()
        with patch.dict(sys.modules, {"helixdb_uniffi": native_module(handle)}):
            client = await AsyncClient.embedded(InMemory("async-sql-embedded"))
            with self.assertRaises(HelixError) as rejected:
                await client.explain_sql("SELECT 1", writer_only=True)
            self.assertEqual(rejected.exception.kind, "InvalidRequest")
            self.assertEqual(await client.sql("SELECT 1"), ROWS)
            self.assertEqual(await client.explain_sql("SELECT 1", query_name="q"), PLAN)
            await client.close()
        self.assertEqual([name for name, _ in handle.calls], ["sql_json", "explain_sql_json"])

        with patch.dict(sys.modules, {"helixdb_uniffi": native_module(SqlHandle(legacy=True))}):
            client = await AsyncClient.embedded(InMemory("async-sql-old-bindings"))
            with self.assertRaises(HelixError) as unavailable:
                await client.sql("SELECT 1")
            await client.close()
        self.assertEqual(unavailable.exception.kind, "EmbeddedUnavailable")


if __name__ == "__main__":
    unittest.main()
