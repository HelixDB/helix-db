# Cypher

Helix supports the Cypher profile below alongside the native DSL. Both frontends
use the existing graph storage, transactions, and indexes. Existing databases do
not need a storage migration. This is a supported language profile, not full
openCypher compatibility. Use a server or embedded build containing this implementation;
cloud gateway deployment is separate.

## Execute a statement

Send one statement to a local server's `POST /v2/cypher` endpoint:

```sh
curl http://localhost:6969/v2/cypher \
  -H 'Content-Type: application/json' \
  --data '{"query":"RETURN $name AS name, $age AS age","parameters":{"name":"Ada","age":30}}'
```

```json
{"columns":["name","age"],"rows":[["Ada",30]]}
```

Use the host and port of your configured instance. Existing server authentication
and tenant routing apply. `query` is required; `parameters` and `query_name` are
optional. Each row has one value per column in column order. Duplicates are
preserved unless the query uses `DISTINCT`. Use `ORDER BY` when row order matters.

Create and read a graph:

```cypher
CREATE (a:Person {name:'Ada', age:30})-[:KNOWS]->(b:Person {name:'Bob', age:40})
RETURN a.name AS person, b.name AS friend
```

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.age >= $minimumAge
RETURN a.name AS person, collect(b.name) AS friends
ORDER BY person
```

Parameters are values; labels, relationship types, and variable names are query
syntax. Keep values in parameters rather than interpolating them into query text.
The existing `POST /v2/query` endpoint continues to accept the native DSL contract.

## SDK and embedded entry points

Use an existing configured client and its normal connection lifecycle:

| Client | Method |
|---|---|
| TypeScript | `await client.cypher(query, parameters, queryName)` |
| Rust SDK | `client.cypher(query, parameters, query_name).await` |
| Go | `client.Cypher(ctx, helix.CypherRequest{Query: query, Parameters: parameters})` |
| Python | `client.cypher(query, parameters, query_name="example")` |
| Async Python | `await client.cypher(query, parameters, query_name="example")` |

TypeScript and Python default omitted parameters to an empty map. Rust SDK
parameters are a `BTreeMap<String, serde_json::Value>` and `query_name` is
`Option<&str>`. Go returns `(*CypherResponse, error)`. Each client preserves tagged
lossless values in the response. See the [Rust](../sdks/rust/README.md),
[TypeScript](../sdks/typescript/README.md), [Go](../sdks/go/README.md), and
[Python](../sdks/python/README.md) guides for client construction and embedded
build prerequisites. Use SDK and server builds from this checkout for Cypher.

The database crate also exposes `database.cypher(db::cypher::Request::new(query)).await`.
The lower-level `db::cypher::execute` accepts explicit scope, execution control,
query mode, and resource limits. These use the caller's existing database handle.
For a prepared JSON body, use `database.cypher_json(request).await` or
`db::cypher::execute_json` with explicit scope and limits. JSON preparation runs
before a modifying statement commits, so an encoding resource failure rolls back
the statement. `EncodedResponse::body()` borrows the JSON, `into_bytes()` transfers
it to shared transport ownership, and `into_vec()` moves its allocation to an
embedded caller. None of these methods serializes the result again.
Transport adapters that check read/write policy before execution can consume
`Request::compile()` once, inspect `CompiledRequest::request_type()`, and pass the
owned result to `HelixQueryService::execute_compiled_cypher_json_scoped_controlled`.
HTTP and gRPC use this path. Compilation retains no database or catalog snapshot;
execution still acquires the current scoped catalog, validates parameters, and
applies the attempt's limits and cancellation control.

The additive gRPC `ExecuteCypher(QueryJsonRequest)` method accepts the same JSON
body and existing request options, returning a `QueryJsonResponse`.

From a linked local Helix project:

```sh
helix cypher -e 'RETURN 1 AS value'
helix cypher -e 'RETURN $name AS name' --parameters '{"name":"Ada"}'
helix cypher --file query.cypher --parameters '{"minimumAge":30}'
```

An optional instance name selects a linked local instance. `--host`, `--port`,
and `--compact` control connection and output. The CLI currently requires a local
instance configuration.

## Supported language profile

| Area | Supported |
|---|---|
| Patterns | `MATCH`, `OPTIONAL MATCH`, fixed-length directed and undirected relationships, multiple patterns, repeated variables, named paths |
| Rows | `WHERE`, `WITH`, `RETURN`, aliases, `DISTINCT`, `UNWIND`, `ORDER BY`, `SKIP`, `LIMIT` |
| Expressions | Parameters, scalar/list/map literals, property and index access, arithmetic, comparisons, boolean operators, null tests, `IN`, string predicates, `CASE` |
| Graph/list functions | `id`, `type`, `labels`, `properties`, `keys`, `exists` for properties, `size`, `length`, `nodes`, `relationships`, `head`, `last`, `range`, `reverse` |
| Scalar functions | `coalesce`, `abs`, `toString`, `toInteger`/`toInt`, `toFloat`, `toBoolean`, `trim`, `ltrim`, `rtrim`, `toLower`, `toUpper`, `substring` |
| Aggregation | `count`, `sum`, `avg`, `min`, `max`, `collect`, including distinct arguments |
| Writes | `CREATE`, property and map `SET`, property `REMOVE`, `DELETE`, `DETACH DELETE` |

`MERGE`, variable-length and shortest paths, `UNION`, subqueries, comprehensions,
procedures, schema DDL, temporal/spatial functions, and Bolt are outside this
profile. Unsupported syntax receives a specific error. Use the existing native
index-management API to create indexes; Cypher planning can select existing
compatible indexes.

## Storage and mutation rules

New nodes require exactly one nonempty label; new relationships require exactly
one nonempty type. Unlabeled `MATCH` scans existing nodes. Multiple node labels and
label changes are unsupported. Property names must be nonempty and cannot begin
with `$`; that namespace contains internal metadata and is omitted from Cypher
property maps.

Stored properties support scalars and homogeneous scalar lists. Maps and nested
lists can be expression values but cannot be stored as properties. Native index
value restrictions also apply; an incompatible indexed value fails the statement
atomically.

```cypher
MATCH (n:Person {name:'Ada'})
SET n += {age:31, nickname:null}
RETURN properties(n)
```

`+=` updates the supplied keys and preserves other properties. `=` replaces all
user properties with the supplied map. Either form preserves internal labels and
types. A null property assignment removes that property. Separate SET items execute
in order; later items can read earlier changes. Native DSL null behavior is preserved.

Each modifying statement runs in one write transaction, including index changes.
Plain `DELETE` of a node with attached relationships fails and rolls back the
statement. `DETACH DELETE` removes those relationships and the node. An error in a
later clause also rolls back earlier writes. Do not automatically retry an
uncertain commit outcome; follow the existing transaction error contract.

Once commit starts, dropping the caller does not stop commit finalization. The
engine retains mutation guards, pending property admission, and cache fences
until finalization completes. `HelixDB::close()` stops new commit admission and
waits for started commits before closing shared resources; dropping the close
waiter does not abandon shutdown. Keep the async runtime alive until close
finishes. A cancelled transport request does not prove that its write rolled back.

## Lossless values

Nulls, booleans, strings, lists, maps, and finite floats use ordinary JSON values.
Signed integers in the JavaScript safe-integer range use JSON numbers. Larger
integers and nonfinite floats use tagged values:

```json
{"$type":"integer","value":"9223372036854775807"}
```

```json
{"$type":"float","value":"Infinity"}
```

Float tags also support `NaN` and `-Infinity`. These tags are accepted in parameters.
Wrap a literal map containing a `$type` key as
`{"$type":"map","value":{"$type":"literal"}}` to disambiguate it from a tag.

Graph results use these tags:

| `$type` | Fields |
|---|---|
| `node` | `id`, `labels`, `properties` |
| `relationship` | `id`, `start`, `end`, `type`, `properties` |
| `path` | Ordered `nodes` and `relationships`, containing full tagged graph objects |

Node IDs and relationship IDs are unsigned
decimal strings and occupy separate namespaces. Nodes include `labels` and
`properties`; relationships include `start`, `end`, `type`, and `properties`.
Paths include ordered `nodes` and `relationships`. Relationship endpoints retain
their stored direction even when a path traverses them in reverse. Preserve IDs
as strings; `id()` separately checks conversion to a signed integer and reports
overflow when it cannot be represented.

## Explain and limits

`POST /v2/cypher/explain` accepts the same request body and returns a separate
planning response without executing the statement. It describes selected access
paths, cardinality estimates, blocking work, Cartesian products, and optimizer
budget warnings. The embedded equivalent is `HelixDB::explain_cypher`; the CLI
uses `--explain`.

Successful result JSON contains only `columns` and `rows`. Embedded response
objects expose local diagnostics separately. Resource-limit failures return an
error instead of truncating a successful result. The advanced embedded API can
set query memory, result-size, batch-size, and collection-item limits. The memory
budget is an admission estimate, not a process RSS ceiling. HTTP, gRPC and
embedded SDK bindings prepare their JSON within that budget before commit,
including the overlap between typed values and the encoded buffer. Shared body
clones and slices retain the body's admission until their final owner is dropped.
An embedded caller taking a Vec assumes its memory ownership and accounting.
Transport framing, TLS queues, shared storage caches and caller allocations are
outside this engine estimate. Disk spilling is not implemented.
Modifying statements admit boxed transaction read operations before allocation,
including calls that are dropped without being polled. They also admit the
serializable transaction's retained read
keys and requested scan ranges, including empty scans. Repeated keys share one
retained payload allowance; table growth and commit-time read-state copies are
included. This admission lasts through backend commit or transaction abort, so
a write that streams a small result can still reach its budget through a large
read set. A memory failure before commit rolls back the complete statement.
Frontend/planner allocations and some native storage working buffers still need
memory accounting; the current budget covers the integrated execution buffers.
