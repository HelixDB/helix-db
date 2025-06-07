use crate::ingestion_engine::postgres_ingestion::{to_camel_case, PostgresIngestor};
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use pgvector::Vector as PGVector;
use lazy_static::lazy_static;
use tokio_postgres::{
    Config,
    NoTls,
};
use std::{
    fs,
    collections::HashSet,
    path::Path,
    str::FromStr,
};

async fn cleanup_database(client: &mut tokio_postgres::Client) -> Result<(), Box<dyn std::error::Error>> {
    // Drop existing tables if they exist
    let tables = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            &[],
        )
        .await?;

    // Drop all sequences if they exist
    let sequences = client
        .query(
            "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public'",
            &[],
        )
        .await?;

    for row in tables {
        let table_name: String = row.get(0);
        client
            .execute(&format!("DROP TABLE IF EXISTS {} CASCADE", table_name), &[])
            .await?;
    }

    for row in sequences {
        let sequence_name: String = row.get(0);
        client
            .execute(&format!("DROP SEQUENCE IF EXISTS {}", sequence_name), &[])
            .await?;
    }
    Ok(())
}

static PARENTS_DATA: &[(&str, i32, &str)] = &[
    ("Ryan Williams", 61, "Dallas"),
    ("David Christian", 47, "New York"),
    ("Lawrence Dorsey", 66, "Dallas"),
    ("Kayla Mendoza", 63, "New York"),
    ("Aaron Stewart", 60, "San Antonio"),
    ("Victoria Edwards", 50, "Philadelphia"),
    ("James Perry", 50, "San Antonio"),
    ("Taylor Riddle", 55, "San Diego"),
    ("Christopher Garcia", 46, "San Diego"),
    ("Julie Dudley", 44, "Los Angeles"),
    ("Linda Chen", 48, "Dallas"),
    ("Lisa Lee", 45, "Philadelphia"),
    ("Samantha Lewis", 60, "Phoenix"),
    ("Michael Silva", 42, "Los Angeles"),
    ("Sherry Pena", 42, "Chicago"),
    ("Joel Bolton", 46, "New York"),
    ("Samuel Jones", 40, "Phoenix"),
    ("Scott Jones", 49, "Phoenix"),
    ("Kevin Wright", 43, "San Diego"),
    ("Lisa Garza", 62, "Philadelphia"),
];

// Global static data for users
static USERS_DATA: &[(&str, i32, &str, i32)] = &[
    ("Heather Pittman", 28, "Dallas", 8),
    ("Angela Wallace", 19, "San Diego", 19),
    ("Barry Kelly", 19, "Chicago", 5),
    ("Lisa Barnes", 33, "New York", 5),
    ("Stephen Reynolds", 33, "Dallas", 18),
    ("Seth Gomez", 27, "San Diego", 5),
    ("Michelle Vance", 21, "Houston", 11),
    ("Regina Kirby", 27, "Dallas", 20),
    ("Linda Johnson", 36, "Philadelphia", 10),
    ("Virginia Copeland", 39, "San Jose", 9),
    ("Timothy Reed", 30, "Los Angeles", 7),
    ("Ashley Olsen", 28, "Chicago", 4),
    ("Kelly Walter", 21, "Chicago", 8),
    ("Anita Manning", 25, "Philadelphia", 15),
    ("Carl Dillon", 26, "San Diego", 11),
    ("James Keller", 36, "Houston", 11),
    ("Joe Moore", 30, "San Diego", 9),
    ("Brian Silva", 30, "Phoenix", 11),
    ("Stephen Riley", 38, "Phoenix", 14),
    ("Carolyn Gonzalez", 19, "Los Angeles", 13),
];

lazy_static! {
    static ref EMBEDDING_DATA: Vec<(i32, PGVector)> = {
        vec![
            (1, PGVector::from(vec![0.1, 0.2, 0.3])),
            (2, PGVector::from(vec![0.4, 0.5, 0.6])),
            (3, PGVector::from(vec![0.7, 0.8, 0.9])),
            (4, PGVector::from(vec![0.2, 0.9, 0.1])),
            (5, PGVector::from(vec![0.4, 0.3, 0.5])),
            (6, PGVector::from(vec![0.1, 0.7, 0.8])),
        ]
    };
}

pub async fn create_mock_postgres_db() -> Result<(tokio_postgres::Client, tokio_postgres::Config), tokio_postgres::Error> {
    let mut config = Config::new();
    config
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("postgres")
        .dbname("postgres");

    let (mut client, connection) = config.connect(NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    cleanup_database(&mut client).await.expect("Failed to clean up database");

    let parents_table = format!("parents");
    let users_table = format!("users");
    let embeddings_table = format!("embeddings");

    client
        .batch_execute(&format!(
            r#"
            CREATE EXTENSION IF NOT EXISTS vector;

            CREATE SEQUENCE {parents}_id_seq;
            CREATE SEQUENCE {users}_id_seq;
            CREATE SEQUENCE {embeddings}_id_seq;

            CREATE TABLE {parents} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{parents}_id_seq'),
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                grew_up_in TEXT NOT NULL
            );

            CREATE TABLE {users} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{users}_id_seq'),
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                city TEXT NOT NULL,
                parent_id INTEGER REFERENCES {parents}(id)
            );

            CREATE TABLE {embeddings} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{embeddings}_id_seq'),
                parent_id INTEGER REFERENCES {parents}(id),
                embedding VECTOR(3) NOT NULL
            );
            "#,
            parents = parents_table,
            users = users_table,
            embeddings = embeddings_table
        ))
        .await?;

    for parent in PARENTS_DATA {
        client
            .execute(
                &format!(
                    "INSERT INTO {} (name, age, grew_up_in) VALUES ($1, $2, $3)",
                    parents_table
                ),
                &[&parent.0, &(parent.1 as i32), &parent.2],
            )
            .await?;
    }

    for user in USERS_DATA {
        client
            .execute(
                &format!(
                    "INSERT INTO {} (name, age, city, parent_id) VALUES ($1, $2, $3, $4)",
                    users_table
                ),
                &[&user.0, &(user.1 as i32), &user.2, &(user.3 as i32)],
            )
            .await?;
    }

    for (parent_id, embedding) in EMBEDDING_DATA.iter() {
        client
            .execute(
                &format!(
                    "INSERT INTO {} (parent_id, embedding) VALUES ($1, $2)",
                    embeddings_table
                ),
                &[&parent_id, &embedding],
            )
            .await?;
        }

    Ok((client, config))
}

fn create_temp_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("postgres_test")
        .tempdir()
        .expect("Failed to create temp directory")
}

#[tokio::test]
async fn test_postgres_custom_ingestion() {
    // just to make sure insertion into db actually works
    match create_mock_postgres_db().await {
        Ok((mut client, _)) => {
            let row = client
                .query_one("SELECT COUNT(*) FROM parents", &[])
                .await
                .expect("Failed to query parents table");
            let parent_count: i64 = row.get(0);
            assert_eq!(parent_count, 20, "Expected 20 parents in the table");

            let row = client
                .query_one("SELECT COUNT(*) FROM users", &[])
                .await
                .expect("Failed to query users table");
            let user_count: i64 = row.get(0);
            assert_eq!(user_count, 20, "Expected 20 users in the table");

            // ------------------------------------------------------------------------------------

            let parent_rows = client
                .query("SELECT id, name, age, grew_up_in FROM parents ORDER BY id", &[])
                .await
                .expect("Failed to query parents table");

            assert_eq!(parent_rows.len(), PARENTS_DATA.len(), "Unexpected number of parents");
            for (i, row) in parent_rows.iter().enumerate() {
                let id: i32 = row.get(0);
                let name: &str = row.get(1);
                let age: i32 = row.get(2);
                let grew_up_in: &str = row.get(3);

                assert_eq!(id, (i + 1) as i32, "Parent ID mismatch at index {}", i);
                assert_eq!(name, PARENTS_DATA[i].0, "Parent name mismatch at index {}", i);
                assert_eq!(age, PARENTS_DATA[i].1, "Parent age mismatch at index {}", i);
                assert_eq!(grew_up_in, PARENTS_DATA[i].2, "Parent grew_up_in mismatch at index {}", i);
            }

            let user_rows = client
                .query("SELECT id, name, age, city, parent_id FROM users ORDER BY id", &[])
                .await
                .expect("Failed to query users table");

            assert_eq!(user_rows.len(), USERS_DATA.len(), "Unexpected number of users");
            for (i, row) in user_rows.iter().enumerate() {
                let id: i32 = row.get(0);
                let name: &str = row.get(1);
                let age: i32 = row.get(2);
                let city: &str = row.get(3);
                let parent_id: i32 = row.get(4);

                assert_eq!(id, (i + 1) as i32, "User ID mismatch at index {}", i);
                assert_eq!(name, USERS_DATA[i].0, "User name mismatch at index {}", i);
                assert_eq!(age, USERS_DATA[i].1, "User age mismatch at index {}", i);
                assert_eq!(city, USERS_DATA[i].2, "User city mismatch at index {}", i);
                assert_eq!(parent_id, USERS_DATA[i].3, "User parent_id mismatch at index {}", i);
            }

            let invalid_parents = client
                .query_one(
                    "SELECT COUNT(*) FROM users WHERE parent_id NOT IN (SELECT id FROM parents)",
                    &[],
                )
                .await
                .expect("Failed to verify foreign key integrity");
            let invalid_count: i64 = invalid_parents.get(0);
            assert_eq!(invalid_count, 0, "Found users with invalid parent_id references");

            // ------------------------------------------------------------------------------------

            cleanup_database(&mut client)
                .await
                .expect("Failed to clean up database");
        }
        Err(e) => {
            panic!("Failed to create a mock database: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_vector_ingestion() {
    let mut config = Config::new();
    config
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("postgres")
        .dbname("postgres");

    let (mut client, connection) = config
        .connect(NoTls)
        .await
        .expect("Failed to connect to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    cleanup_database(&mut client)
        .await
        .expect("Failed to clean up database");

    // Create schema with unique table names
    let parents_table = "parents".to_string();
    let embeddings_table = "embeddings".to_string();

    // Create the tables and enable vector extension
    client
        .batch_execute(&format!(
                r#"
            CREATE EXTENSION IF NOT EXISTS vector;

            CREATE TABLE {parents} (
                id INTEGER PRIMARY KEY
            );

            CREATE TABLE {embeddings} (
                parent_id INTEGER REFERENCES {parents_ref}(id),
                embedding VECTOR(3) NOT NULL
            );
            "#,
            parents = parents_table,
            embeddings = embeddings_table,
            parents_ref = parents_table
        ))
        .await
        .expect("Failed to create vector schema");

    // Insert sample data
    for (parent_id, _) in EMBEDDING_DATA.iter() {
        client
            .execute(
                &format!("INSERT INTO {} (id) VALUES ($1)", parents_table),
                &[parent_id],
            )
            .await
            .expect("Failed to insert parent");
            }

    for (parent_id, embedding) in EMBEDDING_DATA.iter() {
        client
            .execute(
                &format!("INSERT INTO {} (parent_id, embedding) VALUES ($1, $2)", embeddings_table),
                &[parent_id, embedding],
            )
            .await
            .expect("Failed to insert embedding");
            }

    // Create a temporary directory for test outputs
    let temp_dir = create_temp_dir();
    let output_dir = temp_dir.path().to_str().unwrap();

    // Create a PostgreSQL ingestor
    let mut ingestor = PostgresIngestor::new(
        "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
        Some("test_instance".to_string()),
        1000,
        true,
    )
        .await
        .expect("Failed to create PostgreSQL ingestor");

    // Run the ingestion process
    ingestor
        .ingest(output_dir)
        .await
        .expect("Failed to run ingestion process");

    // Verify that the output files were created
    let path = Path::new(output_dir).join("ingestion.jsonl");
    let schema_path = Path::new(output_dir).join("schema.hx");

    assert!(path.exists(), "ingestion.jsonl file missing");
    assert!(schema_path.exists(), "schema.hx file missing");

    // Read and verify the nodes file
    let content = fs::read_to_string(&path).expect("Failed to read nodes file");
    let mut found_node_types: HashSet<String> = HashSet::new();
    let mut embedding_nodes: Vec<JsonValue> = Vec::new();

    content.lines().for_each(|line| {
        let value: JsonValue = serde_json::from_str(line).expect("Failed to parse JSONL line");
        if value["payload_type"] == "node" {
            found_node_types.insert(value["label"].as_str().unwrap().to_string());
            if value["label"] == "embeddings" {
                embedding_nodes.push(value);
            }
        }
    });

    // Verify node types (expecting parents and embeddings)
    assert_eq!(
        found_node_types.len(),
        2,
        "Expected 2 node types (parents, embeddings), found: {:?}", found_node_types
    );
    assert!(
        found_node_types.contains("parents"),
        "Parents node type missing"
    );
    assert!(
        found_node_types.contains("embeddings"),
        "Embeddings node type missing"
    );

    // Verify embedding nodes
    assert_eq!(
        embedding_nodes.len(),
        EMBEDDING_DATA.len(),
        "Expected {} embedding nodes, found {}",
        EMBEDDING_DATA.len(),
        embedding_nodes.len()
    );

    for node in &embedding_nodes {
        let properties = node["properties"].as_object().expect("Properties should be an object");
        assert!(properties.contains_key("embedding"), "Embedding property missing");

        let embedding = properties["embedding"].as_object().expect("Embedding should be an object");
        assert!(embedding.contains_key("data"), "HVector data missing");
        let data = embedding["data"].as_array().expect("HVector data should be an array");
        assert_eq!(data.len(), 3, "HVector data should have 3 dimensions");

        // Verify vector values match EMBEDDING_DATA
        let parent_id = properties["parent_id"].as_i64().expect("parent_id should be a number") as i32;
        if let Some(expected) = EMBEDDING_DATA.iter().find(|(id, _)| *id == parent_id) {
            let expected_data = expected.1.to_vec();
            let expected_data: Vec<f64> = expected_data.into_iter().map(|x| x as f64).collect();
            let actual_data: Vec<f64> = data
                .iter()
                .map(|v| v.as_f64().expect("Vector value should be a float"))
                .collect();
            for (i, (actual, expected)) in actual_data.iter().zip(expected_data.iter()).enumerate() {
                assert!(
                    (actual - expected).abs() < 1e-6,
                    "Vector value mismatch for parent_id {} at index {}: {} vs {}",
                    parent_id,
                    i,
                    actual,
                    expected
                );
            }
        } else {
            panic!("No matching parent_id {} in EMBEDDING_DATA", parent_id);
        }
    }

    // Read and verify the schema file
    let schema_content = fs::read_to_string(&schema_path).expect("Failed to read schema file");
    assert!(
        schema_content.contains("N::Parents"),
        "Parents node schema missing"
    );
    assert!(
        schema_content.contains("N::Embeddings"),
        "Embeddings node schema missing"
    );
    assert!(
        schema_content.contains("embedding: Vector"),
        "Embedding vector type missing"
    );

    // Clean up
    cleanup_database(&mut client)
        .await
        .expect("Failed to clean up database");
    temp_dir.close().expect("Failed to clean up temp directory");
}

#[test]
fn test_to_camel_case() {
    assert_eq!(to_camel_case("hello_world"), "HelloWorld");
    assert_eq!(to_camel_case("hello-world"), "HelloWorld");
    assert_eq!(to_camel_case("hello world"), "HelloWorld");
    assert_eq!(to_camel_case("helloWorld"), "HelloWorld");
    assert_eq!(to_camel_case("HelloWorld"), "HelloWorld");
    assert_eq!(to_camel_case(""), "");
    assert_eq!(to_camel_case("hello"), "Hello");
    assert_eq!(to_camel_case("HELLO"), "Hello");
    assert_eq!(to_camel_case("hello_world_test"), "HelloWorldTest");
}

#[test]
fn test_map_sql_type_to_helix_type() {
    use super::postgres_ingestion::map_sql_type_to_helix_type;

    assert_eq!(map_sql_type_to_helix_type("integer"), "Integer");
    assert_eq!(map_sql_type_to_helix_type("bigint"), "Integer");
    assert_eq!(map_sql_type_to_helix_type("smallint"), "Integer");
    assert_eq!(map_sql_type_to_helix_type("numeric"), "Float");
    assert_eq!(map_sql_type_to_helix_type("decimal"), "Float");
    assert_eq!(map_sql_type_to_helix_type("real"), "Float");
    assert_eq!(map_sql_type_to_helix_type("double precision"), "Float");
    assert_eq!(map_sql_type_to_helix_type("character varying"), "String");
    assert_eq!(map_sql_type_to_helix_type("character"), "String");
    assert_eq!(map_sql_type_to_helix_type("text"), "String");
    assert_eq!(map_sql_type_to_helix_type("boolean"), "Boolean");
    assert_eq!(map_sql_type_to_helix_type("date"), "String");
    assert_eq!(map_sql_type_to_helix_type("time"), "String");
    assert_eq!(map_sql_type_to_helix_type("timestamp"), "String");
    assert_eq!(map_sql_type_to_helix_type("uuid"), "String");
    assert_eq!(map_sql_type_to_helix_type("json"), "String");
    assert_eq!(map_sql_type_to_helix_type("jsonb"), "String");
    assert_eq!(map_sql_type_to_helix_type("bytea"), "String");
}

#[tokio::test]
async fn test_postgres_full_ingestion() {
    // Create a configuration for the PostgreSQL connection
    let mut config = Config::new();
    config
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("postgres")
        .dbname("postgres");

    let (mut client, connection) = config
        .connect(NoTls)
        .await
        .expect("Failed to connect to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    cleanup_database(&mut client)
        .await
        .expect("Failed to clean up database");

    // Create a more complex schema with unique table names
    let users_table = format!("users");
    let posts_table = format!("posts");
    let comments_table = format!("comments");
    let tags_table = format!("tags");
    let post_tags_table = format!("post_tags");

    // Now create the tables with explicit sequence names
    client
        .batch_execute(&format!(
            r#"
            CREATE SEQUENCE {users}_id_seq;
            CREATE SEQUENCE {posts}_id_seq;
            CREATE SEQUENCE {comments}_id_seq;
            CREATE SEQUENCE {tags}_id_seq;
            CREATE SEQUENCE {post_tags}_id_seq;

            CREATE TABLE {users} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{users}_id_seq'),
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE {posts} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{posts}_id_seq'),
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                author_id INTEGER REFERENCES {users_ref}(id),
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE {comments} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{comments}_id_seq'),
                content TEXT NOT NULL,
                post_id INTEGER REFERENCES {posts_ref}(id),
                author_id INTEGER REFERENCES {users_ref}(id),
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE {tags} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{tags}_id_seq'),
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE {post_tags} (
                post_id INTEGER REFERENCES {posts_ref}(id),
                tag_id INTEGER REFERENCES {tags_ref}(id),
                PRIMARY KEY (post_id, tag_id)
            );
            "#,
            post_tags = post_tags_table,
            comments = comments_table,
            posts = posts_table,
            users = users_table,
            tags = tags_table,
            users_ref = users_table,
            posts_ref = posts_table,
            tags_ref = tags_table
        ))
        .await
        .expect("Failed to create complex schema");

    // Insert sample data
    client
        .execute(
            &format!(
                "INSERT INTO {} (username, email) VALUES ($1, $2)",
                users_table
            ),
            &[&"john_doe", &"john@example.com"],
        )
        .await
        .expect("Failed to insert user");

    client
        .execute(
            &format!(
                "INSERT INTO {} (username, email) VALUES ($1, $2)",
                users_table
            ),
            &[&"jane_smith", &"jane@example.com"],
        )
        .await
        .expect("Failed to insert user");

    client
        .execute(
            &format!(
                "INSERT INTO {} (title, content, author_id) VALUES ($1, $2, $3)",
                posts_table
            ),
            &[
                &"First Post",
                &"This is the content of the first post",
                &(1 as i32),
            ],
        )
        .await
        .expect("Failed to insert post");

    client
        .execute(
            &format!(
                "INSERT INTO {} (title, content, author_id) VALUES ($1, $2, $3)",
                posts_table
            ),
            &[
                &"Second Post",
                &"This is the content of the second post",
                &(2 as i32),
            ],
        )
        .await
        .expect("Failed to insert post");

    client
        .execute(
            &format!(
                "INSERT INTO {} (content, post_id, author_id) VALUES ($1, $2, $3)",
                comments_table
            ),
            &[&"Great post!", &(1 as i32), &(2 as i32)],
        )
        .await
        .expect("Failed to insert comment");

    client
        .execute(
            &format!(
                "INSERT INTO {} (content, post_id, author_id) VALUES ($1, $2, $3)",
                comments_table
            ),
            &[&"Thanks for sharing!", &(2 as i32), &(1 as i32)],
        )
        .await
        .expect("Failed to insert comment");

    client
        .execute(
            &format!("INSERT INTO {} (name) VALUES ($1)", tags_table),
            &[&"technology"],
        )
        .await
        .expect("Failed to insert tag");

    client
        .execute(
            &format!("INSERT INTO {} (name) VALUES ($1)", tags_table),
            &[&"programming"],
        )
        .await
        .expect("Failed to insert tag");

    client
        .execute(
            &format!(
                "INSERT INTO {} (post_id, tag_id) VALUES ($1, $2)",
                post_tags_table
            ),
            &[&(1 as i32), &(1 as i32)],
        )
        .await
        .expect("Failed to insert post tag");

    client
        .execute(
            &format!(
                "INSERT INTO {} (post_id, tag_id) VALUES ($1, $2)",
                post_tags_table
            ),
            &[&(2 as i32), &(2 as i32)],
        )
        .await
        .expect("Failed to insert post tag");

    // Create a temporary directory for test outputs
    let temp_dir = create_temp_dir();
    let output_dir = temp_dir.path().to_str().unwrap();

    // Create a PostgreSQL ingestor
    let mut ingestor = PostgresIngestor::new(
        &format!("postgres://postgres:postgres@localhost:5432/postgres"),
        Some("test_instance".to_string()),
        1000,
        true,
    )
    .await
    .expect("Failed to create PostgreSQL ingestor");

    // Run the full ingestion process
    ingestor
        .ingest(output_dir)
        .await
        .expect("Failed to run ingestion process");

    // Verify that the output files were created
    let path = Path::new(output_dir).join("ingestion.jsonl");
    let schema_path = Path::new(output_dir).join("schema.hx");

    assert!(path.exists());
    assert!(schema_path.exists());

    // Read and verify the nodes file
    let content = fs::read_to_string(path).expect("Failed to read nodes file");
    let mut found_node_types: HashSet<String> = HashSet::new();
    let mut found_edge_types: HashSet<String> = HashSet::new();
    content.lines().for_each(|line| {
        let value: JsonValue = serde_json::from_str(line).unwrap();
        if value["payload_type"] == "node" {
            found_node_types.insert(value["label"].as_str().unwrap().to_string());
        } else if value["payload_type"] == "edge" {
            found_edge_types.insert(value["label"].as_str().unwrap().to_string());
        }
    });

    // We should have 8 nodes (2 users + 2 posts + 2 comments + 2 tags)
    assert_eq!(found_node_types.len(), 10);

    // We should have 6 edges (2 post-author + 2 comment-post + 2 comment-author + 2 post-tag)
    assert_eq!(found_edge_types.len(), 9);

    // Read and verify the schema file
    let schema_content = fs::read_to_string(schema_path).expect("Failed to read schema file");
    found_node_types.iter().for_each(|node_type| {
        assert!(schema_content.contains(&to_camel_case(node_type)));
    });
    found_edge_types.iter().for_each(|edge_type| {
        assert!(schema_content.contains(&to_camel_case(edge_type)));
    });
}

// Test error handling
#[tokio::test]
async fn test_postgres_error_handling() {
    // Test with an invalid connection string
    let result = PostgresIngestor::new(
        "postgres://invalid:invalid@localhost:5432/invalid",
        Some("test_instance".to_string()),
        1000,
        true,
    )
    .await;

    // Should return an error
    assert!(result.is_err());

    // Test with a valid connection but invalid database
    let result = PostgresIngestor::new(
        "postgres://postgres:postgres@localhost:5432/nonexistent_db",
        Some("test_instance".to_string()),
        1000,
        true,
    )
    .await;

    // Should return an error
    assert!(result.is_err());
}

// Test with a more complex schema
#[tokio::test]
async fn test_postgres_complex_schema() -> Result<(), Box<dyn std::error::Error>> {
    // Create a configuration for the PostgreSQL connection
    let mut config = Config::new();
    config
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("postgres")
        .dbname("postgres");

    // Connect to the database
    let (mut client, connection) = config
        .connect(NoTls)
        .await?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Clean up the database first
    cleanup_database(&mut client)
        .await
        .expect("Failed to clean up database");

    // Create a more complex schema with unique table names
    let categories_table = format!("categories");
    let products_table = format!("products");
    let customers_table = format!("customers");
    let orders_table = format!("orders");
    let order_items_table = format!("order_items");

    // Now create the tables with explicit sequence names
    client
        .batch_execute(&format!(
            r#"
            CREATE SEQUENCE {categories}_id_seq;
            CREATE SEQUENCE {products}_id_seq;
            CREATE SEQUENCE {customers}_id_seq;
            CREATE SEQUENCE {orders}_id_seq;
            CREATE SEQUENCE {order_items}_id_seq;

            CREATE TABLE {categories} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{categories}_id_seq'),
                name TEXT NOT NULL,
                description TEXT
            );

            CREATE TABLE {products} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{products}_id_seq'),
                name TEXT NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                category_id INTEGER REFERENCES {categories_ref}(id),
                description TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE {customers} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{customers}_id_seq'),
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                address TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE {orders} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{orders}_id_seq'),
                customer_id INTEGER REFERENCES {customers_ref}(id),
                order_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL,
                total_amount DECIMAL(10, 2) NOT NULL
            );

            CREATE TABLE {order_items} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{order_items}_id_seq'),
                order_id INTEGER REFERENCES {orders_ref}(id),
                product_id INTEGER REFERENCES {products_ref}(id),
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(10, 2) NOT NULL
            );
            "#,
            order_items = order_items_table,
            orders = orders_table,
            products = products_table,
            categories = categories_table,
            customers = customers_table,
            categories_ref = categories_table,
            products_ref = products_table,
            customers_ref = customers_table,
            orders_ref = orders_table
        ))
        .await
        .expect("Failed to create complex schema");

    // Insert sample data
    client
        .execute(
            &format!(
                "INSERT INTO {} (name, description) VALUES ($1, $2)",
                categories_table
            ),
            &[&"Electronics", &"Electronic devices and accessories"],
        )
        .await
        .expect("Failed to insert category");

    client
        .execute(
            &format!(
                "INSERT INTO {} (name, description) VALUES ($1, $2)",
                categories_table
            ),
            &[&"Clothing", &"Apparel and accessories"],
        )
        .await
        .expect("Failed to insert category");

    // Use numeric type for price values
    let laptop_price = Decimal::from_str("999.99").unwrap();
    let tshirt_price = Decimal::from_str("19.99").unwrap();
    let total_amount = Decimal::from_str("1019.98").unwrap();

    client
        .execute(
            &format!(
                "INSERT INTO {} (name, price, category_id, description) VALUES ($1, $2, $3, $4)",
                products_table
            ),
            &[
                &"Laptop",
                &laptop_price,
                &(1 as i32),
                &"High-performance laptop",
            ],
        )
        .await
        .expect("Failed to insert product");

    client
        .execute(
            &format!(
                "INSERT INTO {} (name, price, category_id, description) VALUES ($1, $2, $3, $4)",
                products_table
            ),
            &[&"T-Shirt", &tshirt_price, &(2 as i32), &"Cotton T-shirt"],
        )
        .await
        .expect("Failed to insert product");

    client
        .execute(
            &format!(
                "INSERT INTO {} (name, email, address) VALUES ($1, $2, $3)",
                customers_table
            ),
            &[&"John Doe", &"john@example.com", &"123 Main St"],
        )
        .await
        .expect("Failed to insert customer");

    client
        .execute(
            &format!(
                "INSERT INTO {} (customer_id, status, total_amount) VALUES ($1, $2, $3)",
                orders_table
            ),
            &[&(1 as i32), &"Completed", &total_amount],
        )
        .await
        .expect("Failed to insert order");

    client.execute(
        &format!("INSERT INTO {} (order_id, product_id, quantity, unit_price) VALUES ($1, $2, $3, $4)", order_items_table),
        &[&(1 as i32), &(1 as i32), &(1 as i32), &laptop_price],
    ).await.expect("Failed to insert order item");

    client.execute(
        &format!("INSERT INTO {} (order_id, product_id, quantity, unit_price) VALUES ($1, $2, $3, $4)", order_items_table),
        &[&(1 as i32), &(2 as i32), &(1 as i32), &tshirt_price],
    ).await.expect("Failed to insert order item");

    // Create a temporary directory for test outputs
    let temp_dir = create_temp_dir();
    let output_dir = temp_dir.path().to_str().unwrap();

    // Create a PostgreSQL ingestor
    let mut ingestor = PostgresIngestor::new(
        &format!("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
        Some("test_instance".to_string()),
        1000,
        true,
    )
    .await
    .expect("Failed to create PostgreSQL ingestor");

    // Run the full ingestion process
    ingestor
        .ingest(output_dir)
        .await
        .expect("Failed to run ingestion process");

    // Verify that the output files were created
    let path = Path::new(output_dir).join("ingestion.jsonl");
    let schema_path = Path::new(output_dir).join("schema.hx");

    assert!(path.exists());
    assert!(schema_path.exists());

    // Read and verify the nodes file
    let content = fs::read_to_string(path).expect("Failed to read nodes file");
    let mut found_node_types: HashSet<String> = HashSet::new();
    let mut found_edge_types: HashSet<String> = HashSet::new();
    content.lines().for_each(|line| {
        let value: JsonValue = serde_json::from_str(line).unwrap();
        if value["payload_type"] == "node" {
            found_node_types.insert(value["label"].as_str().unwrap().to_string());
        } else if value["payload_type"] == "edge" {
            found_edge_types.insert(value["label"].as_str().unwrap().to_string());
        }
    });

    assert_eq!(found_node_types.len(), 10);
    assert_eq!(found_edge_types.len(), 9);

    // Read and verify the schema file
    let schema_content = fs::read_to_string(schema_path).expect("Failed to read schema file");
    found_node_types.iter().for_each(|node_type| {
        assert!(schema_content.contains(&to_camel_case(node_type)));
    });
    found_edge_types.iter().for_each(|edge_type| {
        assert!(schema_content.contains(&to_camel_case(edge_type)));
    });

    Ok(())
}

#[tokio::test]
async fn test_postgres_simple_schema() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(5432);
    config.user("postgres");
    config.password("postgres");
    config.dbname("postgres");

    let (mut client, connection) = config.connect(NoTls).await?;
    tokio::spawn(connection);

    cleanup_database(&mut client).await?;
    // ... rest of the test code ...
    Ok(())
}
