"""Exercise real MCP dispatch and embedded storage without external web requests."""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

import httpx
from mcp import types

import research

SOURCE = {"url": "https://example.com/source", "title": "Source", "excerpts": ["Search excerpt"]}


class MCPFixture:
    def __init__(self, *, empty=False, tool_error=False, fetch_error=False, missing_tool=False):
        self.empty = empty
        self.tool_error = tool_error
        self.fetch_error = fetch_error
        self.missing_tool = missing_tool
        self.requests = []
        self.calls = []

    def __call__(self, request):
        self.requests.append(request)
        if request.method != "POST":
            return httpx.Response(405)
        message = json.loads(request.content)
        if "id" not in message:
            return httpx.Response(202)
        method = message["method"]
        if method == "initialize":
            result = {
                "protocolVersion": message["params"]["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "parallel-fixture", "version": "1"},
            }
        elif method == "tools/list":
            names = ["web_search"] if self.missing_tool else ["web_search", "web_fetch"]
            result = {
                "tools": [{"name": name, "inputSchema": {"type": "object"}} for name in names]
            }
        elif method == "tools/call":
            params = message["params"]
            self.calls.append(params)
            if self.tool_error:
                result = {"isError": True, "content": [{"type": "text", "text": "Unavailable"}]}
            else:
                source = dict(SOURCE)
                errors = []
                if params["name"] == "web_fetch":
                    source["excerpts"] = ["Fetched detail"]
                    if self.fetch_error:
                        errors = [{"url": SOURCE["url"], "error_type": "not_found"}]
                data = {"results": [] if self.empty or errors else [source], "errors": errors}
                result = {
                    "content": [{"type": "text", "text": json.dumps(data)}],
                    "structuredContent": data,
                }
        else:
            raise AssertionError(f"Unexpected MCP method: {method}")
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": message["id"], "result": result})


class ResearchTests(unittest.TestCase):
    def search(self, fixture, *, fetch_first=False):
        return asyncio.run(
            research.search_sources(
                "graph vector search",
                "test-run",
                1,
                fetch_first,
                transport=httpx.MockTransport(fixture),
            )
        )

    def test_search_fetch_store_and_reopen(self):
        fixture = MCPFixture()
        sources = self.search(fixture, fetch_first=True)
        self.assertEqual(sources[0]["excerpts"], ["Fetched detail"])
        self.assertEqual([call["name"] for call in fixture.calls], ["web_search", "web_fetch"])
        for call in fixture.calls:
            self.assertEqual(call["arguments"]["session_id"], "test-run")
        self.assertEqual(fixture.calls[1]["arguments"]["urls"], [SOURCE["url"]])
        for request in fixture.requests:
            self.assertEqual(str(request.url), research.MCP_URL)
            self.assertEqual(request.headers["user-agent"], research.USER_AGENT)
            self.assertNotIn("authorization", request.headers)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "new" / "research-data"
            first = research.store_sources(path, "graph vector search", "test-run", sources)
            self.assertEqual(len(first["sources"]), 1)
            self.assertEqual(first["sources"][0]["excerpt"], "Fetched detail")
            second = research.store_sources(path, "another question", "second-run", [SOURCE])
            self.assertEqual(len(second["sources"]), 1)
            self.assertEqual(second["sources"][0]["run_id"], "second-run")
            # Reopen and independently read both persisted runs through the SDK.
            client = research.Client.embedded(research.Disk(str(path), "parallel-research"))
            try:
                query = (
                    research.read_batch()
                    .var_as(
                        "sources",
                        research.g()
                        .n_with_label("ResearchSource")
                        .value_map(research.SOURCE_FIELDS),
                    )
                    .returning(["sources"])
                )
                rows = client.query(query.to_query_request())["sources"]
                self.assertEqual({row["run_id"] for row in rows}, {"test-run", "second-run"})
                self.assertTrue(all(row["url"] == SOURCE["url"] for row in rows))
            finally:
                client.close()

    def test_search_only(self):
        fixture = MCPFixture()
        self.assertEqual(self.search(fixture), [SOURCE])
        self.assertEqual(len(fixture.calls), 1)

    def test_no_results(self):
        with self.assertRaises(ExceptionGroup) as raised:
            self.search(MCPFixture(empty=True))
        self.assertIn("Search returned no sources", repr(raised.exception))

    def test_tool_error(self):
        with self.assertRaises(ExceptionGroup) as raised:
            self.search(MCPFixture(tool_error=True))
        self.assertIn("Parallel tool failed", repr(raised.exception))

    def test_fetch_error(self):
        with self.assertRaises(ExceptionGroup) as raised:
            self.search(MCPFixture(fetch_error=True), fetch_first=True)
        self.assertIn("Could not fetch the first source", repr(raised.exception))

    def test_missing_tool(self):
        with self.assertRaises(ExceptionGroup) as raised:
            self.search(MCPFixture(missing_tool=True), fetch_first=True)
        self.assertIn("Parallel MCP is missing tools", repr(raised.exception))

    def test_text_result_fallback_and_invalid_shape(self):
        data = {"results": [SOURCE]}
        result = types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(data))]
        )
        self.assertEqual(research.tool_data(result), data)
        with self.assertRaisesRegex(RuntimeError, "unexpected result shape"):
            research.tool_data(types.CallToolResult(content=[], structuredContent={}))


if __name__ == "__main__":
    unittest.main()
