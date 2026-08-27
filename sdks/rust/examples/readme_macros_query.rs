//! Compile-tested mirror of the Usage example in
//! [`helix-dsl-macros/README.md`](../helix-dsl-macros/README.md).
//!
//! `#[query]` helpers return `Result<QueryRequest, QueryError>`.

#![recursion_limit = "256"]

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
    assert_eq!(request.query_name(), Some("find_user"));

    let request = create_post("hello".to_string())?;
    assert_eq!(request.query_name(), Some("create_post"));
    Ok(())
}
