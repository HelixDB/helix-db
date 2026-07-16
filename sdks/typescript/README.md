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

All requests execute through `POST /v1/query`. The SDK does not expose stored
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
await client.requestBuilder<MyResponse>().warmOnly().query(request).send();
await client.requestBuilder<MyResponse>().shouldAwaitDurability(false).query(request).send();
```

Embedded mode uses the generated `@helix-db/uniffi` package:

```ts
const client = await Client.embedded({ kind: "inMemory", database: "app" });
try {
  const result = await client.query<MyResponse>(request).send();
} finally {
  await client.close();
}
```

`Client.embeddedReader(...)` opens an existing disk or object-storage database
read-only. Server request options are rejected in embedded mode.

`HelixError.kind` distinguishes `Network`, `Remote`, `Serialization`,
`InvalidUrl`, `InvalidRequest`, `EmbeddedUnavailable`, and `Embedded` failures.

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
