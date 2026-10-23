# Store web research as a HelixDB graph

Search the web with [Parallel Search MCP](https://docs.parallel.ai/integrations/mcp/search-mcp),
store each run and its sources as a graph in an embedded HelixDB database, then rank
the excerpts of past runs with HelixDB's BM25 text index. Search uses Fast mode on the
anonymous endpoint, with no Parallel account or API key needed. The free endpoint is
intended for light use and has rate limits.

## The graph

Every invocation writes one `Run` node and one `Source` node per result, connected by
a `CITES` edge that records the result's rank:

```
(Run {run_id, objective}) -[CITES {rank}]-> (Source {url, title, excerpt})
```

`Source.excerpt` carries a BM25 text index, so `--search` ranks stored excerpts and
then walks `CITES` backwards to report which run found each one. Nothing is looked up
by scanning properties: a stored run is read back through the variables the write
batch just created, and a search enters the graph through the text index.

## Run

Requires Python 3.10 or later and a platform supported by `helix-db-embedded`.
From this directory, install the example's dependencies in a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python research.py "HelixDB graph vector search" --max-results 3
```

On Windows, activate with `.venv\Scripts\activate` instead.
No running database, Cloud login, or model API key is required. The script calls
MCP tools directly over Streamable HTTP; it does not run an LLM agent.

Search excerpts are usually enough. When you need more detail from the first URL:

```bash
python research.py "HelixDB graph vector search" --fetch-first --max-results 3
```

The script prints the `Run` row and the `Source` rows it wrote, in rank order. The
objective is sent as the tools' `objective` argument and must be at most 200
characters, the limit `web_fetch` enforces; the `web_search` query itself is the
first six words, because Parallel ranks short keyword queries rather than whole
questions. `--max-results` limits how many sources are stored locally, not the
server's search settings.

## Search past runs

```bash
python research.py --search "graph text ranking"
```

Each hit reports the BM25 `score`, the `Source` fields, and the `run_id` and
`objective` of every run that cited it, highest score first. `--max-results` caps how
many sources the index returns, so a source cited by two runs contributes two rows.
Searching reads only the local database; it does not call Parallel.

Each invocation adds a separate run; the example does not deduplicate sources across
runs, so two runs that find the same URL produce two `Source` nodes. Pass
`--database-dir /path/to/research-data` to choose a different disk directory. Delete
that directory to discard the example's data after closing the process.

Web content is untrusted data. This example stores it as text and never executes it.
Results without a URL or without text are skipped rather than stored. Search, fetch,
or transport errors stop the invocation before any database write, print one line to
stderr, and exit non-zero; warnings are printed to stderr. MCP calls have a 60-second
response timeout and the network phase has a 120-second overall timeout.

## Check the example

```bash
python -m unittest -v test_research
```

The tests use the real MCP client with a local HTTP transport fixture and a real
embedded HelixDB database. They make no live web requests, and the Python and
JavaScript packages workflow runs them on every change to the SDK.
