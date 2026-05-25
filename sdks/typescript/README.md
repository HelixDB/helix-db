# @helix-db/helix-db

TypeScript query DSL for HelixDB. This package builds the same JSON AST shape as the Rust `helix-db` crate.

The compatibility target is structural JSON equality with the Rust DSL. Object formatting and key order are not part of the contract, but enum tags, field names, omitted fields, explicit `null` fields, bundle metadata, and dynamic request payloads are intended to match Rust serde output.

## Quick Start

```ts
import { defineParams, g, param, readBatch } from "@helix-db/helix-db";

const params = defineParams({
  tenantId: param.string(),
  limit: param.i64(),
});

function findUsers(p = params) {
  return readBatch()
    .varAs("users", g().nWithLabel("User").limit(p.limit).valueMap(["$id", "name"]))
    .returning(["users"]);
}

const body = findUsers().toDynamicJson(params, {
  tenantId: "acme",
  limit: 25n,
});
```

Query builders are plain functions. Calling the function returns a `ReadBatch` or `WriteBatch` that can serialize itself.

```ts
findUsers().toJsonString(); // raw batch JSON
findUsers().toDynamicJson(params, { tenantId: "acme", limit: 25n }); // full /v1/query request JSON
findUsers().toDynamicRequest(params, { tenantId: "acme", limit: 25n }); // request object
```

## Registration Model

Registration is only needed when generating predefined/stored query bundles. Rust registration macros are represented explicitly with `defineParams`, `registerRead`, `registerWrite`, and `defineQueries`.

```ts
const addUserParams = defineParams({
  name: param.string(),
  tenantId: param.string(),
});

function addUser(p = addUserParams) {
  return writeBatch()
    .varAs("user", g().addN("User", { name: p.name, tenantId: p.tenantId }))
    .returning(["user"]);
}

addUser().toDynamicJson(addUserParams, {
  name: "Alice",
  tenantId: "acme",
});

export const queries = defineQueries({
  write: {
    add_user: registerWrite(addUser, addUserParams),
  },
});
```

Route names must be unique across read and write routes. Duplicate names throw `GenerateError`.

## Parameter Schemas

Supported schemas are `param.bool()`, `param.i64()`, `param.f64()`, `param.f32()`, `param.string()`, `param.dateTime()`, `param.bytes()`, `param.value()`, `param.object()`, `param.object(inner)`, and `param.array(inner)`.

Dynamic request helpers and registered route helpers are typed from the schema:

```ts
const params = defineParams({
  ids: param.array(param.i64()),
  labels: param.object(param.string()),
});

queries.call.some_route({
  ids: [1n, 2n],
  labels: { status: "active" },
});
```

Dynamic JSON requests cannot represent bytes parameters, so schema conversion rejects `param.bytes()` with `DynamicQueryError.UnsupportedBytesParameter`.

## Dynamic Requests

For dynamic `/v1/query`, call your plain query function and serialize the returned batch as a request.

```ts
const body = findUsers().toDynamicJson(params, {
  tenantId: "acme",
  limit: 25n,
});
```

Use `toDynamicRequest(...)` when you need the request object instead of a string.

```ts
const request = findUsers().toDynamicRequest(params, {
  tenantId: "acme",
  limit: 25n,
});
```

No-parameter queries do not need a schema argument.

```ts
function countUsers() {
  return readBatch().varAs("count", g().nWithLabel("User").count()).returning(["count"]);
}

countUsers().toDynamicJson();
```

Registered routes still get callable helpers under `queries.call` for compatibility and stored-route workflows.

The request includes `request_type`, the batch query, converted `parameters`, and `parameter_types`, matching the Rust dynamic request shape.

## Bundle Generation

```ts
const bundle = queries.buildQueryBundle();
const json = serializeQueryBundle(bundle);

await queries.generate("queries.json");
```

Bundles use `QUERY_BUNDLE_VERSION = 4` and contain read routes, write routes, and per-route parameter metadata. `deserializeQueryBundle` validates the bundle version for TypeScript consumers.

## Number Handling

JavaScript `number` values are accepted for safe integers only when an integer is required. Use `bigint` or `i64(...)` for full `i64` range values.

```ts
g().n(9223372036854775807n);
PropertyValue.i64(9223372036854775807n);
```

Use `stringifyJson`, `serializeQueryBundle`, or request `toJsonString()` instead of raw `JSON.stringify` when payloads may contain `bigint`.

## Datetime Handling

`DateTime` stores milliseconds since the Unix epoch, supports negative epochs, and renders dynamic request parameters as UTC RFC3339 strings with millisecond precision.

```ts
DateTime.fromMillis(-1).toRfc3339(); // 1969-12-31T23:59:59.999Z
DateTime.parseRfc3339("2026-04-05T12:34:56.789+02:00").toRfc3339();
```

## Rust Migration

Common translations:

- `#[register]` becomes `registerRead(...)` or `registerWrite(...)`
- Rust function parameters become `defineParams(...)`
- Rust parameter expressions become direct `params` properties
- `read_batch()` becomes `readBatch()`
- `write_batch()` becomes `writeBatch()`
- `var_as(...)` becomes `varAs(...)`
- `NodeRef::var(...)` becomes `NodeRef.var(...)`
- `SourcePredicate::eq(...)` becomes `SourcePredicate.eq(...)`

## API Reference

The public entry point exports scalar helpers, AST classes, traversal builders, batch builders, registration helpers, dynamic request helpers, bundle helpers, and a `prelude` object for convenience. The implementation is intentionally close to Rust enum names on the wire while exposing camelCase TypeScript builders.
