"""Search public sources through MCP, then store and read their provenance in HelixDB."""

import argparse
import asyncio
import json
import sys
import uuid
from datetime import timedelta
from pathlib import Path

import httpx
from helixdb import Client, Disk, Predicate, g, read_batch, write_batch
from mcp import ClientSession, types
from mcp.client.streamable_http import streamable_http_client

MCP_URL = "https://search.parallel.ai/mcp"
USER_AGENT = "HelixDB-ParallelResearch/0.1 (+https://github.com/HelixDB/helix-db)"
SOURCE_FIELDS = ["run_id", "objective", "url", "title", "excerpt"]


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
        print(f"Parallel warning: {warning['message']}", file=sys.stderr)
    return data


async def search_sources(
    objective: str,
    run_id: str,
    max_results: int,
    fetch_first: bool,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[dict]:
    """Use anonymous Streamable HTTP; a single session ID covers search and fetch."""
    async with asyncio.timeout(120):
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT}, timeout=60, transport=transport
        ) as http:
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
                                "search_queries": [objective],
                                "session_id": run_id,
                            },
                        )
                    )
                    sources = search["results"][:max_results]
                    if not sources:
                        raise RuntimeError("Search returned no sources; nothing was stored")
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
                        if fetched.get("errors") or not fetched["results"]:
                            raise RuntimeError(f"Could not fetch the first source: {fetched}")
                        sources[0] = fetched["results"][0]
                    return sources


def store_sources(directory: Path, objective: str, run_id: str, sources: list[dict]) -> dict:
    """Append one research run, then read only that run back from the embedded database."""
    client = Client.embedded(Disk(str(directory.resolve()), "parallel-research"))
    try:
        batch = write_batch()
        names = []
        for index, source in enumerate(sources):
            name = f"source_{index}"
            names.append(name)
            batch = batch.var_as(
                name,
                g().add_n(
                    "ResearchSource",
                    {
                        "run_id": run_id,
                        "objective": objective,
                        "url": source["url"],
                        "title": source.get("title") or "",
                        "excerpt": "\n\n".join(source["excerpts"]),
                    },
                ),
            )
        client.query(batch.returning(names).to_query_request())
        query = (
            read_batch()
            .var_as(
                "sources",
                g()
                .n_with_label("ResearchSource")
                .where_(Predicate.eq("run_id", run_id))
                .value_map(SOURCE_FIELDS),
            )
            .returning(["sources"])
        )
        return client.query(query.to_query_request())
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("objective", help="A short web research question or search phrase")
    parser.add_argument("--max-results", type=int, choices=range(1, 11), default=3)
    parser.add_argument(
        "--fetch-first", action="store_true", help="Fetch more detail for the first URL"
    )
    parser.add_argument("--database-dir", type=Path, default=Path("research-data"))
    args = parser.parse_args()
    run_id = str(uuid.uuid4())
    sources = asyncio.run(
        search_sources(args.objective, run_id, args.max_results, args.fetch_first)
    )
    print(json.dumps(store_sources(args.database_dir, args.objective, run_id, sources), indent=2))


if __name__ == "__main__":
    main()
