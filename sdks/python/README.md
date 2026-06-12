# HelixDB Python SDK

The Python SDK pairs an idiomatic query-builder DSL with a small dependency-free
HTTP client for sending dynamic HelixDB queries to `POST /v1/query`.

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

request = query.to_dynamic_request()
result = Client("http://localhost:6969").query().dynamic(request).send()
```

The DSL emits the same dynamic-query JSON AST as the Rust, TypeScript, and Go
SDKs. Python methods use `snake_case`; compatibility aliases such as
`nWithLabel` and `valueMap` are also available for users translating TypeScript
examples directly.

## Dynamic Parameters

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

body = query.to_dynamic_json(
    params,
    {"tenant_id": "acme", "limit": 10},
    query_name="find_users",
)
```

## Stored Queries

```python
from helixdb import Client

client = Client("https://cluster.helix-db.com", api_key="hx_secret")
response = client.query().body({"tenant_id": "acme"}).stored("find_users").send()
```

Run the SDK tests from the repository root:

```sh
PYTHONPATH=sdks/python/src python -m unittest discover sdks/python/tests
```
