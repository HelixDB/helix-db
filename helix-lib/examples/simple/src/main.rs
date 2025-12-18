mod queries;

use helix_lib::{HelixDB, ResponseExt};
use serde::Deserialize;
use sonic_rs::json;

#[derive(Deserialize, Debug)]
struct GetUsersResponse {
    users: Vec<User>,
}

#[derive(Deserialize, Debug)]
struct User {
    id: String,
    label: String,
    name: Option<String>,
    email: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load config from generated queries
    let config = queries::config().expect("Failed to load config");

    // Create database
    let db = HelixDB::new("/tmp/memory_example", config)?;

    // Create a user
    let create_response = db.execute(
        "CreateUser",
        json!({
            "name": "Alice",
            "email": "alice@example.com"
        }),
    )?;
    println!("Created user: {}", create_response.json()?);

    // Get all users
    let users: GetUsersResponse = db.execute("GetUsers", json!({}))?.deserialize()?;
    println!("Found {} users", users.users.len());

    for user in users.users {
        println!(
            "  - {}: {}",
            user.name.unwrap_or_default(),
            user.email.unwrap_or_default()
        );
    }

    Ok(())
}
