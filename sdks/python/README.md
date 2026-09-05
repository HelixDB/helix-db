# HelixDB Python SDK

The Python SDK pairs an idiomatic query-builder DSL with synchronous and
asynchronous clients for sending HelixDB queries to `POST /v2/query` or
executing them against an embedded database. The async client uses HTTPX; the
sync API remains unchanged.

## License

The HelixDB Python SDK is licensed under the Apache License 2.0. See
[`LICENSE`](LICENSE) for the complete terms.

```python
from helixdb import Client, Predicate, g, read_batch

query = (
    read_batch()
    .var_as(
        "users",
        g()
        .n_with_label("User")
        .where(Predicate.eq("status", "active"))
        .limit(25)
        .value_map(["$id", "name", "status"]),
    )
    .returning(["users"])
)

request = query.to_query_request()
result = Client("http://localhost:6969").query(request)
```

For Helix Cloud, provide the dashboard database ID with the API key. The client
sends it in the canonical `x-helix-database-id` header:

```python
client = Client.managed(
    "https://cluster.helix-db.com",
    database_id="db_your_database_id",
    api_key="hx_your_api_key",
)
result = client.query(request)
```

## Async Client

Reuse one `AsyncClient` so its HTTPX connection pool can serve many requests.
There is no timeout unless you configure one on the client or request.

```python
import asyncio

import httpx

from helixdb import AsyncClient

async def main():
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
    async with AsyncClient(timeout=10.0, limits=limits) as client:
        first, second = await asyncio.gather(
            client.query(request),
            client.execute(request, writer_only=True, timeout=2.0),
        )
        return first, second

results = asyncio.run(main())
```

`AsyncClient`, `AsyncQueryBuilder`, and `AsyncQueryExecutionRequest` are also
exported from the compatibility import path `helix_db`. Builder methods preserve
the synchronous writer, warm, durability, authorization, and response contracts.

HTTP cancellation propagates as `asyncio.CancelledError`; the response stream is
closed and the client remains reusable. For embedded operations, use
`asyncio.timeout(...)` when a cancellation boundary is required. HTTPX
`AsyncBaseTransport` instances can be injected for custom networking or tests;
the client owns and closes an injected transport. Calling `await client.close()`
more than once is safe.

`HelixError.code` preserves the static server or embedded code separately from
the readable `details`. See the canonical
[query error-code reference](../../docs/database/helix-db/query-guides/error-handling.mdx)
for the complete catalog and migration contract.

Warm a read with `client.execute(request, warm_only=True)`. Helix Cloud fans the
ordinary read out to every eligible backend, discards the results, and returns
an empty successful response after at least one target succeeds. Pass
`writer_only=True` as well to warm only the authoritative writer. Warm writes
return an error before backend execution. A standalone local warm read can
return its normal query payload instead.

The DSL emits the same query JSON AST as the Rust, TypeScript, and Go
SDKs. Python methods use `snake_case`; compatibility aliases such as
`nWithLabel` and `valueMap` are also available for users translating TypeScript
examples directly.

## Query Parameters

```python
from helixdb import Predicate, define_params, g, param, read_batch

params = define_params({
    "tenant_id": param.string(),
    "limit": param.i64(),
})

query = (
    read_batch()
    .var_as(
        "users",
        g()
        .n_with_label("User")
        .where(Predicate.eq("tenantId", params.tenant_id))
        .limit(params.limit)
        .value_map(["$id", "name", "tenantId"]),
    )
    .returning(["users"])
)

body = query.to_query_json(
    params,
    {"tenant_id": "acme", "limit": 10},
    query_name="find_users",
)
```

## Traversal-scoped text search

Use `text_search(...)` after a node or edge traversal to rank only the IDs in
that current stream. This is an exact BM25 prefilter: results equal an
exhaustive search of the selected tenant partition, intersected with the unique
input IDs, followed by deterministic top-`k` selection.

BM25 statistics still come from the full tenant partition.

```python
from helixdb import (
    Predicate,
    PropertyProjection,
    define_params,
    g,
    param,
    read_batch,
)

search_params = define_params({
    "tenant_id": param.string(),
    "query": param.string(),
    "limit": param.i64(),
})

query = (
    read_batch()
    .var_as(
        "documents",
        g()
        .n_with_label("Document")
        .where(Predicate.eq("tenantId", search_params.tenant_id))
        .text_search_with(
            "Document",
            "body",
            search_params.query,
            search_params.limit,
            search_params.tenant_id,
        )
        .project([
            PropertyProjection.renamed("$id", "id"),
            PropertyProjection.renamed("$score", "score"),
            PropertyProjection.new("title"),
        ]),
    )
    .returning(["documents"])
)
```

Use the literal form as
`.text_search("Document", "body", "graph databases", 10, "acme")`.
The same methods work on edge streams. Source-level
`text_search_nodes[_with]` and `text_search_edges[_with]` remain
whole-partition searches.

Restricted results contain unique input IDs, return at most `k`, and order by
`$score` descending then entity ID ascending. The selected input row keeps its
bindings, path, and sack. An empty input returns without opening the text
index. A non-node/edge stream or more than 1,000,000 unique candidates is a
query error. For a tenant-scoped index, pass the same tenant partition used to
build the candidate stream.

## Row Bindings

Use `bind(...)` when a multi-hop traversal needs to keep earlier elements
correlated with later results. `project_distinct_bindings(...)` emits one row
per projected tuple.

```python
from helixdb import BindingProjection, g, read_batch, sub

query = (
    read_batch()
    .var_as(
        "workloads",
        g()
        .n_with_label("Service")
        .bind("service")
        .optional(sub().in_("CREATES").bind("deployment"))
        .union([sub().in_("MANAGES").bind("owner"), sub().out("ROUTES_TO").bind("workload")])
        .project_distinct_bindings([
            BindingProjection.binding("service", "$id", "service_id"),
            BindingProjection.coalesce(
                [
                    BindingProjection.binding_ref("deployment", "$id"),
                    BindingProjection.binding_ref("owner", "$id"),
                    BindingProjection.binding_ref("workload", "$id"),
                ],
                "workload_id",
            ),
        ]),
    )
    .returning(["workloads"])
)
```

## Embedded Client

Install the SDK and matching native runtime:

```sh
python -m pip install helix-db helix-db-embedded
```

```python
from helixdb import Client, InMemory

client = Client.embedded(InMemory("app"))
try:
    response = client.query(request)
finally:
    client.close()
```

Cache profiles are fixed when the handle opens. Vector-memory-only mode
disables SlateDB and object-store caches, not canonical persistence.

```python
from helixdb import EmbeddedCacheConfig, VectorMemoryOnly

client = Client.embedded(
    InMemory("app"),
    cache=EmbeddedCacheConfig(256 * 1024 * 1024, VectorMemoryOnly()),
)
```

`Client.embedded_reader(...)` opens an existing disk or object-storage database
read-only. Stored routes and query bundles are not supported.

The async client awaits native UniFFI operations directly and supports the same
writer, reader, memory, disk, object-storage, and cache configurations.

```python
from helixdb import AsyncClient, Disk, InMemory

async def embedded_query(request):
    writer = await AsyncClient.embedded(InMemory("app"))
    async with writer:
        return await writer.query(request)

async def read_checkpoint(request):
    reader = await AsyncClient.embedded_reader(Disk("./data", "app"))
    async with reader:
        return await reader.query(request)
```

Concurrent `asyncio.gather(...)` calls are passed to the native runtime without
a synchronous wrapper. Native graph loading remains available only on `Client`.

## Native graph algorithms

```python
from helixdb import Client, SourcePredicate, g
from helixdb.graph import BetweennessOptions, GraphSelection

client = Client()
selection = GraphSelection(
    node_traversal=g().n_where(SourcePredicate.has_key("$id")),
    edge_traversal=g().e_where(SourcePredicate.has_key("$id")),
    direction="directed",
    allow_full_scan=True,
)
graph = client.graph(selection)
scores = graph.betweenness_centrality(BetweennessOptions.graphify_default())
```

The returned object retains the immutable Rust topology. Every accessor and
algorithm runs locally without another Helix read. Native wheels are required
for this graph API and embedded mode; server clients do not require the native
runtime.

Run the SDK tests from the repository root:

```sh
python -m pip install -e './sdks/python[dev]'
python -c 'import doctest, helixdb.async_client as module; raise SystemExit(doctest.testmod(module).failed)'
python -m unittest discover -s sdks/python/tests
```
