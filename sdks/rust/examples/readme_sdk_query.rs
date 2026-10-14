//! Compile-tested mirror of the `### Query functions` example in
//! [`sdks/rust/README.md`](../README.md).
//!
//! `#[query]` helpers return `Result<QueryRequest, QueryError>`.

#![recursion_limit = "256"]

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

    // Handle QueryError from parameter coercion / request construction.
    let request = add_user("John".to_string())?;

    let response: AddUserResponse = client.query(request).send().await?;
    println!("created user {}", response.user_id);
    Ok(())
}
