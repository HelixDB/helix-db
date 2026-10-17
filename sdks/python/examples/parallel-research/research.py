"""Search public sources through MCP, then store and search their excerpts as a HelixDB graph."""

import argparse
import asyncio
import contextlib
import json
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import httpx
from helixdb import BindingProjection, Client, Disk, NodeRef, g, read_batch, write_batch
from mcp import ClientSession, types
from mcp.client.streamable_http import streamable_http_client

MCP_URL = "https://search.parallel.ai/mcp"
DATABASE_NAME = "parallel-research"
# web_fetch rejects a longer objective, so the CLI refuses one before opening a session.
OBJECTIVE_LIMIT = 200
# Parallel ranks short keyword queries; the full question stays in the `objective` argument.
SEARCH_QUERY_WORDS = 6
SEARCH_TIMEOUT_SECONDS = 120
INDEX_BUILD_TIMEOUT_SECONDS = 30
RUN_FIELDS = ["$id", "run_id", "objective"]
SOURCE_FIELDS = ["$id", "url", "title", "excerpt"]


def tool_data(result: types.CallToolResult) -> dict:
    """Read an MCP JSON result; tool errors must never become stored source content."""
    if result.isError:
        raise RuntimeError(f"Parallel tool failed: {result.content}")
    data = result.structuredContent
    if data is None:
        text = "\n".join(
            item.text for item in result.content if isinstance(item, types.TextContent)
        )
        data = json.loads(text)
    if not isinstance(data, dict) or not isinstance(data.get("results"), list):
        raise RuntimeError("Parallel returned an unexpected result shape")
    for warning in data.get("warnings") or []:
        message = warning.get("message") if isinstance(warning, dict) else warning
        print(f"Parallel warning: {message}", file=sys.stderr)
    return data


def usable_sources(results: list) -> list[dict]:
    """Drop results that carry no URL or no text; a partial row must not reach the graph."""
    sources = []
    for result in results:
        if not isinstance(result, dict):
            continue
        url = result.get("url")
        excerpts = [text for text in result.get("excerpts") or [] if isinstance(text, str)]
        if not isinstance(url, str) or not url or not excerpts:
            continue
        title = result.get("title")
        sources.append(
            {
                "url": url,
                "title": title if isinstance(title, str) else "",
                "excerpt": "\n\n".join(excerpts),
            }
        )
    return sources


async def search_sources(
    objective: str,
    run_id: str,
    max_results: int,
    fetch_first: bool,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[dict]:
    """Use anonymous Streamable HTTP; a single session ID covers search and fetch."""
    async with httpx.AsyncClient(timeout=60, transport=transport) as http:
        async with streamable_http_client(MCP_URL, http_client=http) as (read, write, _):
            async with ClientSession(
                read, write, read_timeout_seconds=timedelta(seconds=60)
            ) as session:
                await session.initialize()
                tools = {tool.name for tool in (await session.list_tools()).tools}
                required = {"web_search", "web_fetch"} if fetch_first else {"web_search"}
                if not required <= tools:
                    raise RuntimeError(f"Parallel MCP is missing tools: {required - tools}")
                search = tool_data(
                    await session.call_tool(
                        "web_search",
                        {
                            "objective": objective,
                            "search_queries": [" ".join(objective.split()[:SEARCH_QUERY_WORDS])],
                            "session_id": run_id,
                        },
                    )
                )
                sources = usable_sources(search.get("results") or [])[:max_results]
                if not sources:
                    raise RuntimeError("Search returned no usable sources; nothing was stored")
                if fetch_first:
                    fetched = tool_data(
                        await session.call_tool(
                            "web_fetch",
                            {
                                "urls": [sources[0]["url"]],
                                "objective": objective,
                                "session_id": run_id,
                            },
                        )
                    )
                    detailed = usable_sources(fetched.get("results") or [])
                    if not detailed:
                        raise RuntimeError(
                            "Could not fetch the first source: "
                            f"{fetched.get('errors') or 'no readable content'}"
                        )
                    sources[0] = detailed[0]
                return sources


@contextlib.contextmanager
def open_database(directory: Path) -> Iterator[Client]:
    """Close the handle on the way out without letting a close failure mask the real error."""
    directory.mkdir(parents=True, exist_ok=True)
    client = Client.embedded(Disk(str(directory.resolve()), DATABASE_NAME))
    try:
        yield client
    except BaseException:
        with contextlib.suppress(Exception):
            client.close()
        raise
    else:
        client.close()


def await_excerpt_index(client: Client) -> None:
    """Create the BM25 index over `Source.excerpt`, then wait for its build to activate."""
    receipt = client.query(
        write_batch()
        .var_as("index", g().create_text_index_nodes("Source", "excerpt"))
        .returning(["index"])
        .to_query_request()
    )["index"]
    operation_id = receipt.get("operation_id")
    if operation_id is None:
        return  # An already-active index reports no build operation to wait for.
    deadline = time.monotonic() + INDEX_BUILD_TIMEOUT_SECONDS
    while True:
        operation = client.query(
            read_batch()
            .var_as("operation", g().get_index_operation(operation_id))
            .returning(["operation"])
            .to_query_request()
        )["operation"]
        status = operation.get("status")
        if status == "succeeded":
            return
        if status not in ("queued", "running"):
            raise RuntimeError(f"Excerpt index build stopped early: {operation}")
        if time.monotonic() > deadline:
            raise RuntimeError("Timed out waiting for the excerpt index to activate")
        time.sleep(0.05)


def store_run(client: Client, objective: str, run_id: str, sources: list[dict]) -> dict:
    """Write `Run -[CITES]-> Source` and read the rows back through the variables just written."""
    batch = write_batch().var_as(
        "run_node", g().add_n("Run", {"run_id": run_id, "objective": objective})
    )
    for rank, source in enumerate(sources):
        batch = batch.var_as(f"source_{rank}", g().add_n("Source", source))
        batch = batch.var_as(
            f"cites_{rank}",
            g()
            .n(NodeRef.var("run_node"))
            .add_e("CITES", NodeRef.var(f"source_{rank}"), {"rank": rank}),
        )
    batch = batch.var_as("run", g().n(NodeRef.var("run_node")).value_map(RUN_FIELDS))
    batch = batch.var_as(
        "sources", g().n(NodeRef.var("run_node")).out("CITES").value_map(SOURCE_FIELDS)
    )
    return client.query(batch.returning(["run", "sources"]).to_query_request())


def search_excerpts(client: Client, query: str, limit: int) -> dict:
    """Rank stored excerpts with BM25, then hop each hit back to the run that cited it."""
    hits = (
        read_batch()
        .var_as(
            "hits",
            g()
            .text_search_nodes("Source", "excerpt", query, limit)
            .bind("source")
            .in_("CITES")
            .bind("run")
            .project_distinct_bindings(
                [
                    BindingProjection.binding("source", "$score", "score"),
                    BindingProjection.binding("source", "url", "url"),
                    BindingProjection.binding("source", "title", "title"),
                    BindingProjection.binding("source", "excerpt", "excerpt"),
                    BindingProjection.binding("run", "run_id", "run_id"),
                    BindingProjection.binding("run", "objective", "objective"),
                ]
            ),
        )
        .returning(["hits"])
    )
    return client.query(hits.to_query_request())


def error_messages(error: BaseException) -> Iterator[str]:
    """Flatten the exception groups that MCP's task groups raise into their leaf messages."""
    nested = getattr(error, "exceptions", None)
    if nested is None:
        yield f"{type(error).__name__}: {error}"
        return
    for inner in nested:
        yield from error_messages(inner)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "objective", nargs="?", help="A web research question to send to Parallel and store"
    )
    parser.add_argument(
        "--search",
        metavar="QUERY",
        help="Rank excerpts stored by past runs instead of calling Parallel",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        choices=range(1, 11),
        default=3,
        help="Sources to store, or ranked sources to return for --search",
    )
    parser.add_argument(
        "--fetch-first", action="store_true", help="Fetch more detail for the first URL"
    )
    parser.add_argument("--database-dir", type=Path, default=Path("research-data"))
    args = parser.parse_args()
    if (args.objective is None) == (args.search is None):
        parser.error("pass either an objective to research or --search to rank stored excerpts")
    if args.objective is not None and len(args.objective) > OBJECTIVE_LIMIT:
        parser.error(f"objective must be at most {OBJECTIVE_LIMIT} characters for web_fetch")
    try:
        if args.search is not None:
            with open_database(args.database_dir) as client:
                await_excerpt_index(client)
                result = search_excerpts(client, args.search, args.max_results)
        else:
            run_id = str(uuid.uuid4())
            sources = asyncio.run(
                asyncio.wait_for(
                    search_sources(args.objective, run_id, args.max_results, args.fetch_first),
                    SEARCH_TIMEOUT_SECONDS,
                )
            )
            with open_database(args.database_dir) as client:
                await_excerpt_index(client)
                result = store_run(client, args.objective, run_id, sources)
    except Exception as error:
        print(f"research failed: {'; '.join(error_messages(error))}", file=sys.stderr)
        raise SystemExit(1) from error
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
