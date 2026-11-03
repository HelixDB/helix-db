"""Exercise real MCP dispatch and embedded graph storage without external web requests."""

import asyncio
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from mcp import types

import research

SOURCES = [
    {
        "url": "https://example.com/graph",
        "title": "Graph storage",
        "excerpts": ["HelixDB keeps nodes and edges in one graph engine"],
    },
    {
        "url": "https://example.com/vector",
        "title": "Vector search",
        "excerpts": ["Vector search ranks embeddings instead of keywords"],
    },
]
FETCHED = {
    "url": SOURCES[0]["url"],
    "title": SOURCES[0]["title"],
    "excerpts": ["A fetched page about nodes and edges in a graph engine"],
}
MALFORMED = [{"title": "No URL"}, {"url": "https://example.com/empty", "excerpts": []}, "junk"]
OBJECTIVE = "how do graph databases rank text"
STORED = [
    {"url": source["url"], "title": source["title"], "excerpt": source["excerpts"][0]}
    for source in SOURCES
]


class MCPFixture:
    """Answer Streamable HTTP MCP calls in-process so the example never leaves the machine."""

    def __init__(
        self,
        *,
        empty=False,
        tool_error=False,
        fetch_error=False,
        missing_tool=False,
        malformed=False,
        malformed_fetch=False,
        warnings=None,
    ):
        self.empty = empty
        self.tool_error = tool_error
        self.fetch_error = fetch_error
        self.missing_tool = missing_tool
        self.malformed = malformed
        self.malformed_fetch = malformed_fetch
        self.warnings = warnings
        self.requests = []
        self.calls = []

    def _results(self, name):
        if self.empty:
            return []
        if name == "web_fetch":
            if self.fetch_error:
                return []
            return MALFORMED if self.malformed_fetch else [FETCHED]
        return MALFORMED if self.malformed else SOURCES

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
                data = {"results": self._results(params["name"])}
                if params["name"] == "web_fetch" and self.fetch_error:
                    data["errors"] = [{"url": FETCHED["url"], "error_type": "not_found"}]
                if self.warnings is not None:
                    data["warnings"] = self.warnings
                result = {
                    "content": [{"type": "text", "text": json.dumps(data)}],
                    "structuredContent": data,
                }
        else:
            raise AssertionError(f"Unexpected MCP method: {method}")
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": message["id"], "result": result})


@contextlib.contextmanager
def mocked_parallel(fixture):
    """Route the HTTPX client that the CLI builds for itself at the in-process fixture."""
    original = httpx.AsyncClient
    with patch.object(
        research.httpx,
        "AsyncClient",
        side_effect=lambda **kwargs: original(
            **{**kwargs, "transport": httpx.MockTransport(fixture)}
        ),
    ):
        yield


def run_cli(argv):
    """Run main() with mocked argv and return its stdout, stderr, and exit code."""
    stdout, stderr = io.StringIO(), io.StringIO()
    code = 0
    with (
        patch("sys.argv", ["research.py", *argv]),
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        try:
            research.main()
        except SystemExit as exit_request:
            code = exit_request.code
    return stdout.getvalue(), stderr.getvalue(), code


def search(fixture, *, objective=OBJECTIVE, fetch_first=False, max_results=2):
    return asyncio.run(
        research.search_sources(
            objective,
            "test-run",
            max_results,
            fetch_first,
            transport=httpx.MockTransport(fixture),
        )
    )


def failure_messages(error):
    return "; ".join(research.error_messages(error))


class SearchTests(unittest.TestCase):
    def test_search_normalizes_sources(self):
        fixture = MCPFixture()
        self.assertEqual(search(fixture), STORED)
        self.assertEqual([call["name"] for call in fixture.calls], ["web_search"])

    def test_max_results_limits_what_is_stored(self):
        self.assertEqual(search(MCPFixture(), max_results=1), STORED[:1])

    def test_fetch_replaces_the_first_source(self):
        fixture = MCPFixture()
        sources = search(fixture, fetch_first=True)
        self.assertEqual(sources[0]["excerpt"], FETCHED["excerpts"][0])
        self.assertEqual(sources[1], STORED[1])
        self.assertEqual([call["name"] for call in fixture.calls], ["web_search", "web_fetch"])
        self.assertEqual(fixture.calls[1]["arguments"]["urls"], [SOURCES[0]["url"]])
        for call in fixture.calls:
            self.assertEqual(call["arguments"]["session_id"], "test-run")

    def test_long_objectives_become_short_search_queries(self):
        fixture = MCPFixture()
        long_objective = " ".join(f"word{index}" for index in range(20))
        search(fixture, objective=long_objective)
        arguments = fixture.calls[0]["arguments"]
        self.assertEqual(len(arguments["search_queries"][0].split()), research.SEARCH_QUERY_WORDS)
        self.assertEqual(arguments["objective"], long_objective)

    def test_requests_carry_no_helixdb_branding(self):
        fixture = MCPFixture()
        search(fixture)
        for request in fixture.requests:
            self.assertEqual(str(request.url), research.MCP_URL)
            user_agent = request.headers["user-agent"]
            self.assertTrue(user_agent.startswith("python-httpx/"), user_agent)
            self.assertNotIn("helix", user_agent.lower())
            self.assertNotIn("authorization", request.headers)

    def test_malformed_search_results_are_skipped(self):
        with self.assertRaises(Exception) as raised:
            search(MCPFixture(malformed=True))
        self.assertIn("no usable sources", failure_messages(raised.exception))

    def test_malformed_fetch_results_are_skipped(self):
        with self.assertRaises(Exception) as raised:
            search(MCPFixture(malformed_fetch=True), fetch_first=True)
        self.assertIn("no readable content", failure_messages(raised.exception))

    def test_no_results(self):
        with self.assertRaises(Exception) as raised:
            search(MCPFixture(empty=True))
        self.assertIn("no usable sources", failure_messages(raised.exception))

    def test_tool_error(self):
        with self.assertRaises(Exception) as raised:
            search(MCPFixture(tool_error=True))
        self.assertIn("Parallel tool failed", failure_messages(raised.exception))

    def test_fetch_error(self):
        with self.assertRaises(Exception) as raised:
            search(MCPFixture(fetch_error=True), fetch_first=True)
        self.assertIn("Could not fetch the first source", failure_messages(raised.exception))

    def test_missing_tool(self):
        with self.assertRaises(Exception) as raised:
            search(MCPFixture(missing_tool=True), fetch_first=True)
        self.assertIn("Parallel MCP is missing tools", failure_messages(raised.exception))

    def test_text_result_fallback_and_invalid_shape(self):
        data = {"results": SOURCES}
        result = types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(data))]
        )
        self.assertEqual(research.tool_data(result), data)
        with self.assertRaisesRegex(RuntimeError, "unexpected result shape"):
            research.tool_data(types.CallToolResult(content=[], structuredContent={}))

    def test_warnings_are_visible_in_any_shape(self):
        result = types.CallToolResult(
            content=[],
            structuredContent={
                "results": SOURCES,
                "warnings": [{"message": "Partial coverage"}, "bare warning"],
            },
        )
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            self.assertEqual(research.tool_data(result)["results"], SOURCES)
        self.assertIn("Partial coverage", stderr.getvalue())
        self.assertIn("bare warning", stderr.getvalue())


class GraphTests(unittest.TestCase):
    def test_runs_cite_sources_and_excerpts_stay_searchable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "new" / "research-data"
            with research.open_database(path) as client:
                research.await_excerpt_index(client)
                written = research.store_run(client, OBJECTIVE, "first-run", STORED)
                self.assertEqual(written["run"][0]["run_id"], "first-run")
                self.assertEqual(written["run"][0]["objective"], OBJECTIVE)
                self.assertEqual(
                    [row["url"] for row in written["sources"]],
                    [source["url"] for source in STORED],
                )
                self.assertEqual(written["sources"][0]["excerpt"], STORED[0]["excerpt"])
                research.store_run(client, "second question", "second-run", STORED[:1])

            # Reopen so the assertions below read persisted state through a fresh handle.
            with research.open_database(path) as client:
                research.await_excerpt_index(client)
                hits = research.search_excerpts(client, "nodes and edges", 5)["hits"]
                self.assertEqual({hit["url"] for hit in hits}, {SOURCES[0]["url"]})
                self.assertEqual({hit["run_id"] for hit in hits}, {"first-run", "second-run"})
                self.assertTrue(all(hit["score"] > 0 for hit in hits))
                self.assertEqual({hit["objective"] for hit in hits}, {OBJECTIVE, "second question"})

                embeddings = research.search_excerpts(client, "embeddings keywords", 5)["hits"]
                self.assertEqual({hit["url"] for hit in embeddings}, {SOURCES[1]["url"]})
                self.assertEqual({hit["run_id"] for hit in embeddings}, {"first-run"})

                self.assertEqual(research.search_excerpts(client, "kubernetes", 5)["hits"], [])

    def test_search_on_an_empty_database_returns_no_hits(self):
        with tempfile.TemporaryDirectory() as directory:
            with research.open_database(Path(directory) / "empty") as client:
                research.await_excerpt_index(client)
                self.assertEqual(research.search_excerpts(client, "anything", 5)["hits"], [])

    def test_a_close_failure_does_not_mask_the_original_error(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "original failure"):
                with research.open_database(Path(directory) / "db") as client:
                    with patch.object(client, "close", side_effect=RuntimeError("close failure")):
                        raise RuntimeError("original failure")


class CLITests(unittest.TestCase):
    def test_store_then_search_from_the_command_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "research-data"
            with mocked_parallel(MCPFixture()):
                stdout, _, code = run_cli([OBJECTIVE, "--database-dir", str(path)])
            self.assertEqual(code, 0)
            stored = json.loads(stdout)
            self.assertEqual(stored["run"][0]["objective"], OBJECTIVE)
            self.assertEqual(
                [row["url"] for row in stored["sources"]], [source["url"] for source in SOURCES]
            )

            stdout, _, code = run_cli(["--search", "nodes and edges", "--database-dir", str(path)])
            self.assertEqual(code, 0)
            hits = json.loads(stdout)["hits"]
            self.assertEqual([hit["url"] for hit in hits], [SOURCES[0]["url"]])
            self.assertEqual(hits[0]["run_id"], stored["run"][0]["run_id"])

    def test_a_failed_search_reports_one_line_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "research-data"
            with mocked_parallel(MCPFixture(tool_error=True)):
                stdout, stderr, code = run_cli([OBJECTIVE, "--database-dir", str(path)])
            self.assertEqual(code, 1)
            self.assertEqual(stdout, "")
            self.assertEqual(len(stderr.strip().splitlines()), 1)
            self.assertIn("Parallel tool failed", stderr)
            self.assertFalse(path.exists())

    def test_an_objective_longer_than_the_fetch_limit_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "research-data"
            _, stderr, code = run_cli(
                ["x" * (research.OBJECTIVE_LIMIT + 1), "--database-dir", str(path)]
            )
            self.assertEqual(code, 2)
            self.assertIn(str(research.OBJECTIVE_LIMIT), stderr)
            self.assertFalse(path.exists())

    def test_exactly_one_mode_is_required(self):
        for argv in ([], [OBJECTIVE, "--search", "a query"]):
            with self.subTest(argv=argv):
                _, stderr, code = run_cli(argv)
                self.assertEqual(code, 2)
                self.assertIn("either an objective", stderr)


if __name__ == "__main__":
    unittest.main()
