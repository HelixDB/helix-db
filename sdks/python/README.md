# HelixDB Python SDK

The Python SDK pairs an idiomatic query-builder DSL with a dependency-free
client for sending HelixDB queries to `POST /v1/query` or executing them against
an embedded database.

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

```python
from helixdb import Client, InMemory

client = Client.embedded(InMemory("app"))
try:
    response = client.query(request)
finally:
    client.close()
```

`Client.embedded_reader(...)` opens an existing disk or object-storage database
read-only. Stored routes and query bundles are not supported.

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
for this graph API and embedded mode; query-only imports remain dependency free.

Run the SDK tests from the repository root:

```sh
PYTHONPATH=sdks/python/src python -m unittest discover sdks/python/tests
```
