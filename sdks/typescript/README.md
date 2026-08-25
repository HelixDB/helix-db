# @helix-db/helix-db

TypeScript query DSL and client for HelixDB. The SDK emits the same query JSON
AST as the Rust, Go, and Python SDKs and can execute it over HTTP or against an
embedded database.

## Quick Start

```ts
import { Client, Predicate, defineParams, g, param, readBatch } from "@helix-db/helix-db";

const params = defineParams({
  tenantId: param.string(),
  limit: param.i64(),
});

function findUsers(p = params) {
  return readBatch()
    .varAs("users", g().nWithLabel("User").where(Predicate.eq("tenantId", p.tenantId)).limit(p.limit).valueMap(["$id", "name"]))
    .returning(["users"]);
}

const request = findUsers().toQueryRequest(params, {
  tenantId: "acme",
  limit: 25n,
});
const rows = await new Client("http://localhost:6969").query(request).send();
```

Query builders are normal functions returning `ReadBatch` or `WriteBatch`.
The request helpers are:

```ts
findUsers().toQueryRequest(params, { tenantId: "acme", limit: 25n });
findUsers().toQueryJson(params, { tenantId: "acme", limit: 25n });
findUsers().toQueryBytes(params, { tenantId: "acme", limit: 25n });
```

All requests execute through `POST /v2/query`. The SDK does not expose stored
routes, registration, or query-bundle APIs.

## Clients

Server mode uses the built-in global `fetch`:

```ts
const client = Client.server("https://cluster.helix-db.com").withApiKey("hx_secret");
const result = await client.query<MyResponse>(request).send();
```

Advanced server-only headers are available from `requestBuilder<R>()`:

```ts
await client.requestBuilder<MyResponse>().writerOnly().query(request).send();
await client.requestBuilder<void>().warmOnly().query(request).send();
await client.requestBuilder<MyResponse>().shouldAwaitDurability(false).query(request).send();
```

`warmOnly()` is read-only. It sends the ordinary read with
`X-Helix-Warm: true`; Helix Cloud fans it out to every eligible backend and
returns `204 No Content` with no query payload after at least one target
succeeds. Chain `writerOnly()` before `warmOnly()` to warm only the
authoritative writer. Warm writes return `400 Bad Request` before execution.
A standalone local warm read can return its normal query payload instead.

Embedded mode uses `@helix-db/helix-db-embedded`:

```sh
npm install @helix-db/helix-db @helix-db/helix-db-embedded
```

```ts
const client = await Client.embedded({ kind: "inMemory", database: "app" });
try {
  const result = await client.query<MyResponse>(request).send();
} finally {
  await client.close();
}
```

Pass a cache profile as the second argument when the default memory cache is
not appropriate. Vector-memory-only mode disables SlateDB and object-store
caches; it does not disable canonical persistence.

```ts
const client = await Client.embedded(
  { kind: "disk", root: "/data/helix", database: "app" },
  { vectorMemoryBytes: 256 * 1024 * 1024, mode: { kind: "vectorMemoryOnly" } },
);
```

`Client.embeddedReader(...)` opens an existing disk or object-storage database
read-only. Server request options are rejected in embedded mode.

Set `HELIXDB_EMBEDDED_NODE_PACKAGE` to load a compatible native package from a
different module specifier. The former `HELIXDB_UNIFFI_NODE_PACKAGE` name
remains supported as a deprecated compatibility alias.

`HelixError.kind` distinguishes `Network`, `Remote`, `Serialization`,
`InvalidUrl`, `InvalidRequest`, `EmbeddedUnavailable`, and `Embedded` failures.
`HelixError.code` preserves the static server or embedded code separately from
`details`. See the canonical
[query error-code reference](../../docs/database/helix-db/query-guides/error-handling.mdx).

Remote errors expose `statusCode`, `code`, `serverMessage`, `serverDetails`, and
the decoded response `rawBody`. Retry idempotent reads after rate limits or
temporary server failures with backoff:

```ts
import { Client, HelixError, QueryRequest } from "@helix-db/helix-db";

const sleep = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function sendReadWithRetry<T>(client: Client, readRequest: QueryRequest): Promise<T> {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      return await client.query<T>(readRequest).send();
    } catch (error) {
      const retryable =
        error instanceof HelixError &&
        (error.isRateLimited() || (error.statusCode !== undefined && error.statusCode >= 500 && error.statusCode < 600));
      if (!retryable || attempt === 2) throw error;
      await sleep(100 * (attempt + 1));
    }
  }
  throw new Error("unreachable");
}
```

For write conflicts, reload the current state before rebuilding the mutation.
Do not blindly retry writes after server errors because the commit may have
succeeded before the response failed:

```ts
try {
  await client.query(writeRequest).send();
} catch (error) {
  if (error instanceof HelixError && error.isConflict()) {
    console.error("write conflict", error.serverMessage);
  }
}
```

Validation and authentication failures should be handled directly rather than
retried. `rawBody` retains the decoded response text before message fallbacks;
`details` preserves the previous fallback behavior.

## Parameter Schemas

Supported schemas are `param.bool()`, `param.i64()`, `param.f64()`,
`param.f32()`, `param.string()`, `param.dateTime()`, `param.bytes()`,
`param.value()`, `param.object()`, `param.object(inner)`, and
`param.array(inner)`.

Parameter values and types are inserted into the request together. Unknown
parameters and invalid values are rejected before execution. JSON requests
cannot represent bytes parameters, so `param.bytes()` conversion returns
`QueryError.UnsupportedBytesParameter`.

Unnamed requests serialize `query_name: null`. Pass an optional name for logs
and diagnostics:

```ts
findUsers().toQueryJson(params, { tenantId: "acme", limit: 25n }, { queryName: "find_users" });
```

## Predicates

`Predicate.eq`, `neq`, `gt`, `gte`, `lt`, `lte`, and `between` accept literal
values or expressions. Use parameter helpers for request-specific values and
`Predicate.compare(...)` for arbitrary expression comparisons.

```ts
g().nWithLabel("User").where(Predicate.eqParam("email", "email"));
g()
  .nWithLabel("User")
  .where(Predicate.eq("email", Expr.param("email")));
```

## Traversal-scoped text search

Use `textSearch(...)` after a node or edge traversal to rank only the IDs in
that current stream. This is an exact BM25 prefilter, not a post-search limit:
the result is the same as searching the selected tenant partition exhaustively,
intersecting with the unique input IDs, and taking the deterministic top `k`.

BM25 statistics still come from the full tenant partition.

```ts
import { Expr, Predicate, PropertyInput, PropertyProjection, defineParams, g, param, readBatch } from "@helix-db/helix-db";

const searchParams = defineParams({
  tenantId: param.string(),
  query: param.string(),
  limit: param.i64(),
});

function searchVisibleDocuments(p = searchParams) {
  return readBatch()
    .varAs(
      "documents",
      g()
        .nWithLabel("Document")
        .where(Predicate.eq("tenantId", p.tenantId))
        .textSearchWith("Document", "body", PropertyInput.param("query"), Expr.param("limit"), PropertyInput.param("tenantId"))
        .project([PropertyProjection.renamed("$id", "id"), PropertyProjection.renamed("$score", "score"), PropertyProjection.new("title")]),
    )
    .returning(["documents"]);
}
```

Use the literal form as
`.textSearch("Document", "body", "graph databases", 10, "acme")`.
The same `textSearch` and `textSearchWith` methods work on edge streams and
emit `TextSearchEdgesWithin`. Source-level `textSearchNodes[With]` and
`textSearchEdges[With]` remain whole-partition searches.

Restricted results contain unique input IDs, return at most `k`, and order by
`$score` descending then entity ID ascending. The selected input row keeps its
bindings, path, and sack. An empty input returns without opening the text
index. More than 1,000,000 unique candidates is a query error. For a
tenant-scoped index, pass the same tenant partition used to build the candidate
stream.

## Row Bindings

Bindings retain correlated values across multi-hop traversals:

```ts
const query = readBatch()
  .varAs(
    "dependencies",
    g()
      .nWithLabel("Service")
      .bind("service")
      .out("ROUTES_TO")
      .bind("pod")
      .optional(sub().in("CREATES").bind("deployment"))
      .projectDistinctBindings([
        BindingProjection.binding("service", "$id", "service_id"),
        BindingProjection.coalesce(
          [BindingProjection.bindingRef("deployment", "$id"), BindingProjection.bindingRef("pod", "$id")],
          "workload_id",
        ),
      ]),
  )
  .returning(["dependencies"]);
```

Binding projections support stored properties and virtual fields such as
`$id`, `$label`, `$from`, `$to`, `$distance`, and `$score`.

## Numbers and Datetimes

Use `bigint` or `i64(...)` for integers outside JavaScript's safe range.
Query responses preserve those integers as `bigint`.

```ts
g().n(9223372036854775807n);
PropertyValue.i64(9223372036854775807n);
```

Use `stringifyJson` or request `toJsonString()` when values may contain
`bigint`. `DateTime` stores epoch milliseconds and renders parameters as UTC
RFC3339 strings with millisecond precision.

```ts
DateTime.fromMillis(-1).toRfc3339(); // 1969-12-31T23:59:59.999Z
```

## Native graph algorithms

```ts
const selection = new GraphSelection({
  nodeTraversal: g().nWhere(SourcePredicate.hasKey("$id")),
  edgeTraversal: g().eWhere(SourcePredicate.hasKey("$id")),
  direction: "directed",
  allowFullScan: true,
});
const graph = await client.graph(selection);
const scores = await graph.betweennessCentrality({ mode: "auto", exactThrough: 1000, sampleCount: 100, seed: 42 });
```

The load is one normal query. The returned immutable native object runs all
accessors, algorithms, subgraphs, and transforms in Rust without additional
reads. CPU-heavy work therefore runs outside the JavaScript event loop.

## API Reference

The public entry point exports scalar helpers, AST types, traversal builders,
batch builders, query request helpers, graph helpers, and the server/embedded
client. The wire contract follows Rust serde names while TypeScript builders use
camel case.
