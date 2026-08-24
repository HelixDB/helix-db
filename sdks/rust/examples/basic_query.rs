//! End-to-end `#[query]` example: build a request and send it through [`Client`].
//!
//! Compile-checked in CI via:
//! `cargo check --locked --manifest-path sdks/rust/Cargo.toml --example basic_query`
//!
//! Matches the public contract in the root README and `sdks/rust/README.md`:
//! `#[query]` helpers return `Result<QueryRequest, QueryError>`.

#![recursion_limit = "256"]

use helix_db::dsl::prelude::*;
use helix_db::Client;

#[query]
fn add_user(name: String) -> WriteBatch {
    write_batch()
        .var_as(
            "user",
            g().add_n("User", vec![("name", name)])
                .value_map(None::<Vec<String>>),
        )
        .returning(["user"])
}

#[query]
fn get_user(name: String) -> ReadBatch {
    read_batch()
        .var_as(
            "user",
            g().n_with_label("User")
                .where_(Predicate::eq("name", name))
                .value_map(None::<Vec<String>>),
        )
        .returning(["user"])
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(None)?; // defaults to http://localhost:6969

    let new_user: sonic_rs::Value = client
        .query(add_user("John Doe".to_string())?)
        .send()
        .await?;
    println!("new user: {:#}", sonic_rs::to_string_pretty(&new_user)?);

    let user: sonic_rs::Value = client
        .query(get_user("John Doe".to_string())?)
        .send()
        .await?;
    println!("user: {:#}", sonic_rs::to_string_pretty(&user)?);
    Ok(())
}
