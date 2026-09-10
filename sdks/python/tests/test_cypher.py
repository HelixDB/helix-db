from __future__ import annotations

import json
import unittest
from io import BytesIO
from unittest.mock import patch

import httpx

from helixdb import AsyncClient, Client, HelixError
from helixdb._client_common import serialize_cypher


class CypherContractTests(unittest.TestCase):
    def test_sync_route_and_lossless_response(self) -> None:
        response = {
            "columns": ["x"],
            "rows": [[{"$type": "integer", "value": "9223372036854775807"}]],
        }
        with patch(
            "helixdb.client.urlopen", return_value=BytesIO(json.dumps(response).encode())
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
        self.assertEqual(json.loads(serialize_cypher("RETURN 1", None, None))["parameters"], {})


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
