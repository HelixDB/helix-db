# helix-dsl-macros

Procedural macros for the [`helix-db`](../README.md) query DSL.

This crate provides the `#[query]` attribute macro, which transforms a typed
query-building function into a callable function returning
`Result<QueryRequest, QueryError>`.

## Usage

The compile-tested mirror of this example is
[`examples/readme_macros_query.rs`](../examples/readme_macros_query.rs).

```rust
use helix_db::dsl::prelude::*;

#[query]
fn find_user(username: String) -> ReadBatch {
    read_batch()
        .var_as(
            "user",
            g().n_where(SourcePredicate::eq("username", username)),
        )
        .returning(["user"])
}

#[query]
fn create_post(title: String) -> WriteBatch {
    write_batch()
        .var_as("post", g().add_n("Post", vec![("title", title)]))
        .returning(["post"])
}

fn main() -> Result<(), QueryError> {
    let request = find_user("alice".to_string())?;
    Ok(())
}
```

The macro preserves the function's visibility and parameters, builds the query
AST with `Expr::param(...)` references, inserts parameter values and types, sets
`query_name` to the function name, and returns the complete request (or a
`QueryError` if parameter coercion fails). It does not register or persist
queries.

## Supported parameter types

| Rust type | Mapped to |
|---|---|
| `bool` | `Bool` |
| `i64` | `I64` |
| `f32` | `F32` |
| `f64` | `F64` |
| `String` | `String` |
| `Vec<u8>` | `Bytes` |
| `PropertyValue` / `ParamValue` | `Value` |
| `ParamObject` / `HashMap<String, T>` / `BTreeMap<String, T>` | `Object` |
| `Vec<T>` | `Array(T)` (recursive) |

Nested arrays are supported (e.g. `Vec<Vec<f64>>` maps to `Array(Array(F64))`).

## Constraints

The macro rejects functions that are:

- `async`
- Generic (type parameters)
- Methods (have a `self` receiver)
- Using destructuring patterns in parameters
- Returning anything other than `ReadBatch` or `WriteBatch`

## License

Apache-2.0
