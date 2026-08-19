# helix-db

> There is good documentation in the crate doc comments, especially in `src/lib.rs`. AI agents should read the source code and doc comments to get a feel for the query-building patterns and the full API surface.

The `helix-db` crate (imported as `helix_db`) is the Rust SDK for [HelixDB](https://github.com/helix_db/helix-db). It pairs a query-builder DSL with a small async HTTP client ([`helix_db::Client`](#executing-queries-with-helix_dbclient)) for running those queries against a Helix instance.

The DSL is centered on two entry points:

- `read_batch()` for read-only transactions
- `write_batch()` for write-capable transactions

Everything in the DSL is designed to be composed inside those batch chains. You write one or more named traversals with `.var_as(...)` / `.var_as_if(...)`, then choose the final payload with `.returning(...)`.

## Install

Add the crate under `[dependencies]`:

```toml
helix-db = "3.0.0"
```

The crate is published under the name `helix-db` and its library is imported as `helix_db`. For shorter query code, bring the curated builder API into scope:

```rust
use helix_db::dsl::prelude::*;
```

The examples below assume that prelude is in scope.

## Core Shape

Read chain:
`read_batch() -> var_as / var_as_if -> returning`

Write chain:
`write_batch() -> var_as / var_as_if -> returning`

Each `var_as` call accepts a traversal expression, usually starting with `g()`. Traversals can read, traverse, filter, aggregate, or mutate depending on whether they are used in a read or write batch.

## Read Batches

```rust
read_batch()
    .var_as(
        "user",
        g().n_where(SourcePredicate::eq("username", "alice")),
    )
    .var_as(
        "friends",
        g()
            .n(NodeRef::var("user"))
            .out(Some("FOLLOWS"))
            .dedup()
            .limit(100),
    )
    .returning(["user", "friends"]);
```

```rust
read_batch()
    .var_as(
        "active_users",
        g()
            .n_with_label_where("User", SourcePredicate::eq("status", "active"))
            .where_(Predicate::gt("score", 100i64))
            .order_by("score", Order::Desc)
            .limit(25)
            .value_map(Some(vec!["$id", "name", "score"])),
    )
    .returning(["active_users"]);
```

```rust
let statuses = Expr::param("statuses");

read_batch()
    .var_as(
        "matching_users",
        g()
            .n_with_label("User")
            .where_(Predicate::is_in_expr("status", statuses))
            .value_map(Some(vec!["$id", "name", "status"])),
    )
    .returning(["matching_users"]);
```

`Predicate::eq`, `neq`, `gt`, `gte`, `lt`, `lte`, and `between` accept either literal property values or `Expr` parameters. Literal values keep the original literal variants in JSON, while expressions serialize as `EqExpr`, `GteExpr`, `BetweenExpr`, and so on. Use `Predicate::compare(...)` for arbitrary expression-to-expression comparisons.

## Conditional Queries

Use `BatchCondition` with `var_as_if` to run later queries only when earlier variables satisfy runtime conditions.

```rust
read_batch()
    .var_as(
        "user",
        g().n_where(SourcePredicate::eq("username", "alice")),
    )
    .var_as_if(
        "posts",
        BatchCondition::VarNotEmpty("user".to_string()),
        g().n(NodeRef::var("user")).out(Some("POSTED")),
    )
    .returning(["user", "posts"]);
```

## Write Batches

```rust
write_batch()
    .var_as(
        "alice",
        g().add_n("User", vec![("name", "Alice"), ("tier", "pro")]),
    )
    .var_as("bob", g().add_n("User", vec![("name", "Bob")]))
    .var_as(
        "linked",
        g()
            .n(NodeRef::var("alice"))
            .add_e(
                "FOLLOWS",
                NodeRef::var("bob"),
                vec![("since", "2026-01-01")],
            )
            .count(),
    )
    .returning(["alice", "bob", "linked"]);
```

```rust
write_batch()
    .var_as(
        "inactive_users",
        g().n_with_label_where(
            "User",
            SourcePredicate::eq("status", "inactive"),
        ),
    )
    .var_as_if(
        "deactivated_count",
        BatchCondition::VarNotEmpty("inactive_users".to_string()),
        g()
            .n(NodeRef::var("inactive_users"))
            .set_property("deactivated", true)
            .count(),
    )
    .returning(["deactivated_count"]);
```

## Executing Queries with `helix_db::Client`

`helix_db::Client` is a thin async wrapper over `reqwest` for running queries against a Helix
instance. Construct it with an optional base URL, then optionally attach a bearer API key:

```rust
use helix_db::Client;

// Defaults to http://localhost:6969 when `url` is None.
let client = Client::new(None)?;

// Or point at a remote cluster and attach an API key:
let client = Client::new(Some("https://11e2fc88c410fa5eb13e.cluster.helix-db.com"))?
    .with_api_key(Some("hx_your_api_key"));
```

Queries use `client.query(request).send().await`. Advanced server-only requests use
`client.request_builder::<R>()`, optionally toggle request headers, attach a query, and
call `.send().await`:

```rust
// POST a `QueryRequest` (DSL query + parameters) to `/v2/query`.
let response: MyResponse = client
    .query(request)                // `request` is a QueryRequest (see below)
    .send()
    .await?;
```

Optional header toggles can be chained before choosing the query kind:

- `.writer_only()` — require the request to be served by a writer node (`x-helix-require-writer`).
- `.warm_only()` — fan the read out for cache warming (`x-helix-warm`); reads only.
- `.should_await_durability(true)` — block until the write is durable (`x-helix-await-durable`).

`send()` is generic over the deserialized response type `R` and returns `Result<R, HelixError>`.
`HelixError` distinguishes transport errors, non-success responses from the server (`RemoteError`),
serialization failures, and invalid URLs. Use `error.error_code()` to branch on a
static code without parsing the diagnostic. See the canonical
[query error-code reference](../../docs/database/helix-db/query-guides/error-handling.mdx).

A successful warm read returns `204 No Content` with no query payload after at
least one eligible backend succeeds. Chain `.writer_only().warm_only()` to warm
only the authoritative writer. Warm writes return `400 Bad Request` before
backend execution. A standalone local warm read can return its normal query
payload instead.

### Query functions

Annotate a query builder with `#[query]` to get a callable helper that builds a
`QueryRequest` directly from typed arguments. The generated function returns the request
value itself (not a `Result`) — parameter coercion that can fail (e.g. `DateTime`, bytes) panics
with a descriptive message rather than returning an error.

```rust
use helix_db::dsl::prelude::*;
use helix_db::Client;
use serde::Deserialize;

#[query]
pub fn add_user(name: String) -> WriteBatch {
    write_batch()
        .var_as("user_id", g().add_n("user", vec![("name", name)]))
        .returning(vec!["user_id"])
}

#[derive(Deserialize)]
struct AddUserResponse {
    user_id: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(Some("https://11e2fc88c410fa5eb13e.cluster.helix-db.com"))?
        .with_api_key(Some("hx_your_api_key"));

    // Building the request is infallible — no `?` needed here.
    let request = add_user("John".to_string());

    let response: AddUserResponse = client.query(request).send().await?;
    println!("created user {}", response.user_id);
    Ok(())
}
```

Notes:

- A `#[query]` builder remains callable with its declared visibility.
- The serialized payload includes `request_type`, `query_name`, `query`, and optional `parameters` /
  `parameter_types`.
- Requests built directly use `query_name: null`; callable helpers generated by
  `#[query]` set `query_name` to the Rust function name.
- Stored routes, query registration, and bundle generation are not supported.

## Vector Search Operations (End-to-End)

The current Helix interpreter executes vector search as top-k nearest-neighbor lookup with these runtime semantics:

- returns up to `k` hits (top-k behavior)
- hit order is ascending by `$distance` (smaller is closer)
- hit metadata can be read through virtual fields in projections:
  - node hits: `$id`, `$distance`
  - edge hits: `$id`, `$from`, `$to`, `$distance`

### Result field contract

| Field       | Type           | Node hits | Edge hits | Meaning                                            |
| ----------- | -------------- | --------: | --------: | -------------------------------------------------- |
| `$id`       | integer        |       yes |     yes\* | Node ID (for node hits) or edge ID (for edge hits) |
| `$distance` | floating-point |       yes |       yes | Vector distance from query (`lower` = closer)      |
| `$from`     | integer        |        no |       yes | Edge source node ID                                |
| `$to`       | integer        |        no |       yes | Edge target node ID                                |

`*` For edge hits, `$id` is present when an edge ID is available in storage.

Contract scope in the current Helix interpreter:

- available on direct vector-hit streams and projection terminals
- available in `value_map`, `values`, `project`, and (for edges) `edge_properties`
- once a traversal step leaves the hit stream (`out`, `in_`, `both`, etc.), downstream traversers no longer carry distance metadata

### 1) Create indexes and insert vectors

```rust
write_batch()
    .var_as(
        "create_doc_index",
        g().create_vector_index_nodes(
            "Doc",
            "embedding",
            std::num::NonZeroUsize::new(3).expect("vector dimension is non-zero"),
            VectorDistanceMetric::Cosine,
            None::<&str>,
        ),
    )
    .var_as(
        "create_similar_index",
        g().create_vector_index_edges(
            "SIMILAR",
            "embedding",
            std::num::NonZeroUsize::new(3).expect("vector dimension is non-zero"),
            VectorDistanceMetric::Cosine,
            None::<&str>,
        ),
    )
    .var_as(
        "doc_a",
        g().add_n(
            "Doc",
            vec![
                ("title", PropertyValue::from("A")),
                ("embedding", PropertyValue::from(vec![1.0f32, 0.0, 0.0])),
            ],
        ),
    )
    .var_as(
        "doc_b",
        g().add_n(
            "Doc",
            vec![
                ("title", PropertyValue::from("B")),
                ("embedding", PropertyValue::from(vec![0.9f32, 0.1, 0.0])),
            ],
        ),
    )
    .returning(["create_doc_index", "doc_a", "doc_b"]);
```

### 2) Node vector search: get ranked hits and fetch node properties

```rust
read_batch()
    .var_as(
        "doc_hits",
        g().vector_search_nodes("Doc", "embedding", vec![1.0f32, 0.0, 0.0], 5, None)
            .value_map(Some(vec!["$id", "$distance", "title"])),
    )
    .returning(["doc_hits"]);
```

```text
doc_hits rows (example shape):
[
  { "$id": 42, "$distance": 0.0031, "title": "A" },
  { "$id": 77, "$distance": 0.0198, "title": "B" }
]
```

### 3) Use `project(...)` on vector hits (including distance)

```rust
read_batch()
    .var_as(
        "ranked_docs",
        g().vector_search_nodes("Doc", "embedding", vec![1.0f32, 0.0, 0.0], 10, None)
            .project(vec![
                PropertyProjection::renamed("$id", "doc_id"),
                PropertyProjection::renamed("$distance", "score"),
                PropertyProjection::new("title"),
            ]),
    )
    .returning(["ranked_docs"]);
```

### 4) Traverse from hit IDs to related entities

Store hit rows (with `$id` + `$distance`) and then use `NodeRef::var(...)` to continue graph traversal from those hit IDs.

```rust
read_batch()
    .var_as(
        "doc_hit_rows",
        g().vector_search_nodes("Doc", "embedding", vec![1.0f32, 0.0, 0.0], 5, None)
            .value_map(Some(vec!["$id", "$distance", "title"])),
    )
    .var_as(
        "authors",
        g().n(NodeRef::var("doc_hit_rows"))
            .out(Some("AUTHORED_BY"))
            .value_map(Some(vec!["$id", "name"])),
    )
    .returning(["doc_hit_rows", "authors"]);
```

### 5) Edge vector search and endpoint/property extraction

```rust
read_batch()
    .var_as(
        "edge_hits",
        g().vector_search_edges("SIMILAR", "embedding", vec![1.0f32, 0.0, 0.0], 10, None)
            .edge_properties(),
    )
    .var_as(
        "targets",
        g().e(EdgeRef::var("edge_hits"))
            .out_n()
            .value_map(Some(vec!["$id", "title"])),
    )
    .returning(["edge_hits", "targets"]);
```

`edge_hits` rows include `$from`, `$to`, and `$distance` (and `$id` when available), so you can inspect ranking metadata and still traverse from those edges.

### 6) Optional multitenancy

```rust
write_batch()
    .var_as(
        "create_mt_index",
        g().create_vector_index_nodes(
            "Doc",
            "embedding",
            std::num::NonZeroUsize::new(3).expect("vector dimension is non-zero"),
            VectorDistanceMetric::Cosine,
            Some("tenant_id"),
        ),
    )
    .var_as(
        "insert_acme",
        g().add_n(
            "Doc",
            vec![
                ("tenant_id", PropertyValue::from("acme")),
                ("title", PropertyValue::from("Acme doc")),
                ("embedding", PropertyValue::from(vec![1.0f32, 0.0, 0.0])),
            ],
        ),
    )
    .returning(["create_mt_index", "insert_acme"]);
```

```rust
read_batch()
    .var_as(
        "acme_hits",
        g().vector_search_nodes(
            "Doc",
            "embedding",
            vec![1.0f32, 0.0, 0.0],
            5,
            Some(PropertyValue::from("acme")),
        )
        .value_map(Some(vec!["$id", "$distance", "title"])),
    )
    .returning(["acme_hits"]);
```

Multitenant behavior in the current Helix interpreter:

- multitenant index + missing `tenant_value` on search => query error
- multitenant index + unknown tenant => empty result set
- write with vector present but missing tenant property => write error

## Traversal-scoped text search

Use `.text_search(...)` after a node or edge traversal to rank only the IDs in
that current stream. This is an exact BM25 prefilter: results equal an
exhaustive search of the selected tenant partition, intersected with the unique
input IDs, followed by deterministic top-`k` selection.

BM25 statistics still come from the full tenant partition.

```rust
read_batch()
    .var_as(
        "documents",
        g()
            .n_with_label("Document")
            .where_(Predicate::eq("tenant_id", "acme"))
            .text_search(
                "Document",
                "body",
                "graph databases",
                10,
                Some(PropertyValue::from("acme")),
            )
            .project(vec![
                PropertyProjection::renamed("$id", "id"),
                PropertyProjection::renamed("$score", "score"),
                PropertyProjection::new("title"),
            ]),
    )
    .returning(["documents"]);
```

Use `.text_search_with(...)` for runtime `PropertyInput` and `StreamBound`
values. The same method names work on edge streams. Source-level
`text_search_nodes[_with]` and `text_search_edges[_with]` remain
whole-partition searches.

Restricted results contain unique input IDs, return at most `k`, and order by
`$score` descending then entity ID ascending. The selected input row keeps its
bindings, path, and sack. An empty input returns without opening the text
index. A non-node/edge stream or more than 1,000,000 unique candidates is a
query error. For a tenant-scoped index, pass the same tenant partition used to
build the candidate stream.

## Edge-First Reads

```rust
read_batch()
    .var_as(
        "heavy_edges",
        g()
            .e_where(SourcePredicate::gt("weight", 0.8f64))
            .has_label("FOLLOWS")
            .order_by("weight", Order::Desc)
            .limit(50),
    )
    .var_as(
        "targets",
        g()
            .e(EdgeRef::var("heavy_edges"))
            .out_n()
            .dedup(),
    )
    .returning(["heavy_edges", "targets"]);
```

## Row Bindings

Use `.bind(...)` when a multi-hop traversal needs to keep earlier elements correlated with later results. Row bindings are row-local: each path keeps its own named bindings, and `.project_distinct_bindings(...)` can emit one output row per projected tuple.

```rust
read_batch()
    .var_as(
        "dependencies",
        g()
            .n_with_label("Service")
            .where_(Predicate::eq("tenant_id", "acme"))
            .bind("service")
            .out(Some("ROUTES_TO"))
            .where_(Predicate::eq("tenant_id", "acme"))
            .bind("pod")
            .in_(Some("MANAGES"))
            .where_(Predicate::eq("tenant_id", "acme"))
            .bind("owner")
            .union(vec![
                sub()
                    .where_(Predicate::eq("type", "ReplicaSet"))
                    .in_(Some("CREATES"))
                    .where_(Predicate::eq("type", "Deployment"))
                    .where_(Predicate::eq("tenant_id", "acme"))
                    .bind("workload"),
                sub()
                    .where_(Predicate::is_in(
                        "type",
                        vec![
                            "Deployment".to_string(),
                            "StatefulSet".to_string(),
                            "DaemonSet".to_string(),
                        ],
                    ))
                    .bind("workload"),
            ])
            .project_distinct_bindings(vec![
                BindingProjection::binding("service", "$id", "service_id"),
                BindingProjection::binding("workload", "$id", "workload_id"),
            ]),
    )
    .returning(["dependencies"]);
```

Binding projections can read virtual fields such as `$id`, `$label`, `$from`, `$to`, `$distance`, and `$score` from either `BindingTarget::Current` or a named binding. Use `BindingProjection::coalesce(...)` when optional branches may or may not create a binding.

## Branching and Repetition

```rust
read_batch()
    .var_as(
        "recommendations",
        g()
            .n(1u64)
            .store("seed")
            .repeat(RepeatConfig::new(sub().out(Some("FOLLOWS"))).times(2))
            .without("seed")
            .union(vec![sub().out(Some("LIKES"))])
            .dedup()
            .limit(30),
    )
    .returning(["recommendations"]);
```

## Traversal Building Inside `var_as(...)`

Common source steps:

- `n(...)`, `n_where(...)`, `n_with_label(...)`
- `e(...)`, `e_where(...)`, `e_with_label(...)`
- `vector_search_nodes(...)`, `vector_search_edges(...)`
  - current Helix runtime exposes vector hit metadata via virtual fields (`$id`, `$distance`, `$score`, `$from`, `$to`) in terminal projections

Common navigation and filtering:

- `out/in_/both`, `out_e/in_e/both_e`, `out_n/in_n/other_n`
- `has`, `has_label`, `has_key`, `where_`, `within`, `without`, `dedup`
- on edge streams, `has` / `has_label` / `has_key` / `where_` filter stored edge properties and virtual fields; use `edge_has` when the RHS must be a `PropertyInput` expression or parameter
- `limit`, `skip`, `range`, `order_by`, `order_by_multiple`

Common terminal projections:

- `count`, `exists`, `id`, `label`
- `values`, `value_map`, `project`, `edge_properties`

Write-only operations (usable in `write_batch()` traversals):

- `add_n`, `add_e`, `set_property`, `remove_property`, `drop`, `drop_edge`, `drop_edge_by_id`
- `create_index_if_not_exists`, `drop_index`
- `create_vector_index_nodes`, `create_vector_index_edges`, `create_text_index_nodes`, `create_text_index_edges`

For exhaustive catalog-style coverage of every public query-builder function, read the crate docs in `src/lib.rs` and browse the source directly.

## Native graph algorithms

Load a graph with one normal read batch, then reuse it without further database
reads:

```rust,no_run
use helix_db::{Client, SourcePredicate, g};
use helix_db::graph::{BetweennessOptions, GraphDirection, GraphSelection};

# async fn run(client: Client) -> Result<(), Box<dyn std::error::Error>> {
let selection = GraphSelection::new(
    g().n_where(SourcePredicate::has_key("$id")),
    g().e_where(SourcePredicate::has_key("$id")),
    GraphDirection::Directed,
).allow_full_scan();
let graph = client.graph(&selection).await?;
let scores = graph.betweenness_centrality(BetweennessOptions::graphify_default())?;
# let _ = scores;
# Ok(())
# }
```

The immutable, reference-counted graph provides centrality, bounded cycles,
Louvain, BFS/DFS, local shortest paths, degree, subgraphs, transforms, and
spring layout through the storage-independent Rust crate.

## License

Licensed under Apache-2.0.
