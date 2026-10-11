# Store web research in HelixDB

Search the web with [Parallel Search MCP](https://docs.parallel.ai/integrations/mcp/search-mcp),
then store excerpts and their source URLs as `ResearchSource` nodes in an embedded
HelixDB database. Search uses Fast mode on the anonymous endpoint, with no Parallel
account or API key needed. The free endpoint is intended for light use and has rate limits.

## Run

Requires Python 3.11 or later and a platform supported by `helix-db-embedded`.
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

The script prints the nodes it reads back from HelixDB, including `url`, `title`,
`excerpt`, `objective`, and `run_id`. `--max-results` limits the number stored
locally, not the server's search settings. Each invocation appends a separate run
to `research-data/`; it does not deduplicate earlier research. Pass
`--database-dir /path/to/research-data` to choose a different disk directory.
Delete that directory to discard the example's data after closing the process.

Web content is untrusted data. This example stores it as text and never executes
it. Search, fetch, or transport errors stop the invocation before database writes;
warnings are printed to stderr. MCP calls have a 60-second response timeout and
the network phase has a 120-second overall timeout.

## Check the example

```bash
python -m unittest -v test_research
```

The tests use the real MCP client with a local HTTP transport fixture and a real
embedded HelixDB database. They make no live web requests.
