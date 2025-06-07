use super::sql_ingestion::GraphSchema;
use crate::ingestion_engine::sql_ingestion::{to_camel_case, SqliteIngestor};
use rusqlite::{params, Connection, Result as SqliteResult};
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    fs,
};

pub fn create_mock_sqlite_db(file_path: Option<&str>) -> SqliteResult<Connection> {
    let conn = match file_path {
        Some(path) => Connection::open(path)?,
        None => Connection::open_in_memory()?,
    };

    // Enable foreign key constraints
    conn.execute("PRAGMA foreign_keys = ON;", params![])?;

    conn.execute(
        r#"
        CREATE TABLE parents (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            grew_up_in TEXT NOT NULL
        )
        "#,
        params![],
    )?;

    conn.execute(
        r#"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            city TEXT NOT NULL,
            parent_id INTEGER,
            FOREIGN KEY (parent_id) REFERENCES parents(id)
        )
        "#,
        params![],
    )?;

    let parents_data = vec![
        (1, "Ryan Williams", 61, "Dallas"),
        (2, "David Christian", 47, "New York"),
        (3, "Lawrence Dorsey", 66, "Dallas"),
        (4, "Kayla Mendoza", 63, "New York"),
        (5, "Aaron Stewart", 60, "San Antonio"),
        (6, "Victoria Edwards", 50, "Philadelphia"),
        (7, "James Perry", 50, "San Antonio"),
        (8, "Taylor Riddle", 55, "San Diego"),
        (9, "Christopher Garcia", 46, "San Diego"),
        (10, "Julie Dudley", 44, "Los Angeles"),
        (11, "Linda Chen", 48, "Dallas"),
        (12, "Lisa Lee", 45, "Philadelphia"),
        (13, "Samantha Lewis", 60, "Phoenix"),
        (14, "Michael Silva", 42, "Los Angeles"),
        (15, "Sherry Pena", 42, "Chicago"),
        (16, "Joel Bolton", 46, "New York"),
        (17, "Samuel Jones", 40, "Phoenix"),
        (18, "Scott Jones", 49, "Phoenix"),
        (19, "Kevin Wright", 43, "San Diego"),
        (20, "Lisa Garza", 62, "Philadelphia"),
    ];

    for parent in parents_data {
        conn.execute(
            "INSERT INTO parents (id, name, age, grew_up_in) VALUES (?1, ?2, ?3, ?4)",
            params![parent.0, parent.1, parent.2, parent.3],
        )?;
    }

    let users_data = vec![
        (1, "Heather Pittman", 28, "Dallas", 8),
        (2, "Angela Wallace", 19, "San Diego", 19),
        (3, "Barry Kelly", 19, "Chicago", 5),
        (4, "Lisa Barnes", 33, "New York", 5),
        (5, "Stephen Reynolds", 33, "Dallas", 18),
        (6, "Seth Gomez", 27, "San Diego", 5),
        (7, "Michelle Vance", 21, "Houston", 11),
        (8, "Regina Kirby", 27, "Dallas", 20),
        (9, "Linda Johnson", 36, "Philadelphia", 10),
        (10, "Virginia Copeland", 39, "San Jose", 9),
        (11, "Timothy Reed", 30, "Los Angeles", 7),
        (12, "Ashley Olsen", 28, "Chicago", 4),
        (13, "Kelly Walter", 21, "Chicago", 8),
        (14, "Anita Manning", 25, "Philadelphia", 15),
        (15, "Carl Dillon", 26, "San Diego", 11),
        (16, "James Keller", 36, "Houston", 11),
        (17, "Joe Moore", 30, "San Diego", 9),
        (18, "Brian Silva", 30, "Phoenix", 11),
        (19, "Stephen Riley", 38, "Phoenix", 14),
        (20, "Carolyn Gonzalez", 19, "Los Angeles", 13),
    ];

    for user in users_data {
        conn.execute(
            "INSERT INTO users (id, name, age, city, parent_id) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![user.0, user.1, user.2, user.3, user.4],
        )?;
    }

    Ok(conn)
}

/*
#[test]
fn test_ingest_extract_schema() {
    let conn = create_mock_sqlite_db(None).expect("Failed to create mock database");

    let mut ingestor = SqliteIngestor {
        sqlite_conn: conn,
        instance: "http://localhost:6969".to_string(),
        batch_size: 10,
        id_mappings: HashMap::new(),
    };

    let schemas = ingestor.extract_schema().expect("Failed to extract schema");

    assert_eq!(schemas.len(), 2, "Expected exactly 2 tables, found {}", schemas.len());

    let schema_map: HashMap<String, &TableSchema> = schemas
        .iter()
        .map(|s| (s.name.clone(), s))
        .collect();

    assert!(
        schema_map.contains_key("users"),
        "Expected 'users' table in schema"
    );
    assert!(
        schema_map.contains_key("parents"),
        "Expected 'parents' table in schema"
    );

    if let Some(parents_schema) = schema_map.get("parents") {
        assert_eq!(
            parents_schema.columns.len(),
            4,
            "Expected 4 columns in parents table, found {}",
            parents_schema.columns.len()
        );

        let expected_columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                is_primary_key: true,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
                is_primary_key: false,
            },
            ColumnInfo {
                name: "age".to_string(),
                data_type: "INTEGER".to_string(),
                is_primary_key: false,
            },
            ColumnInfo {
                name: "grew_up_in".to_string(),
                data_type: "TEXT".to_string(),
                is_primary_key: false,
            },
            ];

        for expected_col in &expected_columns {
            assert!(
                parents_schema.columns.iter().any(|col| {
                    col.name == expected_col.name
                        && col.data_type == expected_col.data_type
                        && col.is_primary_key == expected_col.is_primary_key
                }),
                "Column {}:{} (is_primary_key: {}) not found in parents table",
                expected_col.name,
                expected_col.data_type,
                expected_col.is_primary_key
            );
        }

        let expected_pks: HashSet<String> = HashSet::from(["id".to_string()]);
        assert_eq!(
            parents_schema.primary_keys, expected_pks,
            "Primary keys mismatch in parents table"
        );

        assert!(
            parents_schema.foreign_keys.is_empty(),
            "Expected no foreign keys in parents table, found {}",
            parents_schema.foreign_keys.len()
        );

        let count: i64 = ingestor
            .sqlite_conn
            .query_row("SELECT COUNT(*) FROM parents", params![], |row| row.get(0))
            .expect("Failed to count parents rows");
        assert_eq!(count, 20, "Expected 20 rows in parents table, found {}", count);
    }

    if let Some(users_schema) = schema_map.get("users") {
        assert_eq!(
            users_schema.columns.len(),
            5,
            "Expected 5 columns in users table, found {}",
            users_schema.columns.len()
        );

        let expected_columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                is_primary_key: true,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
                is_primary_key: false,
            },
            ColumnInfo {
                name: "age".to_string(),
                data_type: "INTEGER".to_string(),
                is_primary_key: false,
            },
            ColumnInfo {
                name: "city".to_string(),
                data_type: "TEXT".to_string(),
                is_primary_key: false,
            },
            ColumnInfo {
                name: "parent_id".to_string(),
                data_type: "INTEGER".to_string(),
                is_primary_key: false,
            },
            ];

        for expected_col in &expected_columns {
            assert!(
                users_schema.columns.iter().any(|col| {
                    col.name == expected_col.name
                        && col.data_type == expected_col.data_type
                        && col.is_primary_key == expected_col.is_primary_key
                }),
                "Column {}:{} (is_primary_key: {}) not found in users table",
                expected_col.name,
                expected_col.data_type,
                expected_col.is_primary_key
            );
        }

        let expected_pks: HashSet<String> = HashSet::from(["id".to_string()]);
        assert_eq!(
            users_schema.primary_keys, expected_pks,
            "Primary keys mismatch in users table"
        );

        assert_eq!(
            users_schema.foreign_keys.len(),
            1,
            "Expected 1 foreign key in users table, found {}",
            users_schema.foreign_keys.len()
        );
        if let Some(fk) = users_schema.foreign_keys.first() {
            let expected_fk = ForeignKey {
                from_table: "users".to_string(),
                from_column: "parent_id".to_string(),
                to_table: "parents".to_string(),
                to_column: "id".to_string(),
            };
            assert_eq!(
                fk.from_table, expected_fk.from_table,
                "Foreign key from_table mismatch"
            );
            assert_eq!(
                fk.from_column, expected_fk.from_column,
                "Foreign key from_column mismatch"
            );
            assert_eq!(
                fk.to_table, expected_fk.to_table,
                "Foreign key to_table mismatch"
            );
            assert_eq!(
                fk.to_column, expected_fk.to_column,
                "Foreign key to_column mismatch"
            );
        }

        let count: i64 = ingestor
            .sqlite_conn
            .query_row("SELECT COUNT(*) FROM users", params![], |row| row.get(0))
            .expect("Failed to count users rows");
        assert_eq!(count, 20, "Expected 20 rows in users table, found {}", count);
    }

    for schema in &schemas {
        println!("{}", schema);
    }
}
*/

#[test]
fn test_ingest_basics() {
    let conn = create_mock_sqlite_db(None).expect("Failed to create mock database");

    let mut ingestor = SqliteIngestor {
        sqlite_conn: conn,
        instance: "http://localhost:6969".to_string(),
        batch_size: 10,
        id_mappings: HashMap::new(),
        graph_schema: GraphSchema::new(),
    };

    let schemas = ingestor.extract_schema().unwrap();
    for schema in &schemas {
        println!("{}", schema);
    }
}

#[test]
fn test_dump_to_json_basic() {
    // Create a temporary file path for the JSONL output
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir;
    let output_path_str = output_path.to_str().unwrap();

    // Create a mock database and ingestor
    let conn = create_mock_sqlite_db(None).expect("Failed to create mock database");
    let mut ingestor = SqliteIngestor {
        sqlite_conn: conn,
        instance: "http://localhost:6969".to_string(),
        batch_size: 10,
        id_mappings: HashMap::new(),
        graph_schema: GraphSchema::new(),
    };

    // Dump the database to JSONL
    ingestor
        .dump_to_json(output_path_str)
        .expect("Failed to dump to JSONL");

    // Verify the file was created
    assert!(output_path.exists(), "JSONL file was not created");

    // Read the JSONL file
    // Verify that the file contains multiple lines (at least one node and one edge)
    let jsonl_content =
        fs::read_to_string(output_path.join("ingestion.jsonl")).expect("Failed to read JSONL file");
    let nodes_lines: Vec<&str> = jsonl_content.lines().filter(|line| {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        json_obj.get("payload_type").unwrap().as_str().unwrap() == "node"
    }).collect();
    let edges_lines: Vec<&str> = jsonl_content.lines().filter(|line| {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        json_obj.get("payload_type").unwrap().as_str().unwrap() == "edge"
    }).collect();

    assert!(
        nodes_lines.len() >= 1,
        "JSONL file should contain at least 1 node"
    );
    assert!(
            edges_lines.len() >= 1,
        "JSONL file should contain at least 1 edge"
    );

    // Verify that each line is valid JSON
    for line in &nodes_lines {
        //println!("node line: {}", line);
        serde_json::from_str::<JsonValue>(line).expect("Failed to parse JSON line");
    }
    for line in &edges_lines {
        //println!("edge line: {}", line);
        serde_json::from_str::<JsonValue>(line).expect("Failed to parse JSON line");
    }

}

#[test]
fn test_dump_to_json_content() {
    // Create a temporary file path for the JSONL output
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir;
    let output_path_str = output_path.to_str().unwrap();

    // Create a mock database and ingestor
    let conn = create_mock_sqlite_db(None).expect("Failed to create mock database");
    let mut ingestor = SqliteIngestor {
        sqlite_conn: conn,
        instance: "http://localhost:6969".to_string(),
        batch_size: 10,
        id_mappings: HashMap::new(),
        graph_schema: GraphSchema::new(),
    };

    // Dump the database to JSONL
    ingestor
        .dump_to_json(output_path_str)
        .expect("Failed to dump to JSONL");

    // Read the JSONL file
    let jsonl_content =
        fs::read_to_string(output_path.join("ingestion.jsonl")).expect("Failed to read JSONL file");
    let nodes_lines: Vec<&str> = jsonl_content.lines().filter(|line| {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        json_obj.get("payload_type").unwrap().as_str().unwrap() == "node"
    }).collect();
    let edges_lines: Vec<&str> = jsonl_content.lines().filter(|line| {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        json_obj.get("payload_type").unwrap().as_str().unwrap() == "edge"
    }).collect();

    // Parse each line as a JSON object
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for line in nodes_lines {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");

        nodes.push(json_obj);
    }
    for line in edges_lines {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        edges.push(json_obj);
    }

    // Verify the number of nodes (should be 40: 20 parents + 20 users)
    assert_eq!(nodes.len(), 40, "Expected 40 nodes, found {}", nodes.len());

    // Count nodes by label
    let mut parent_count = 0;
    let mut user_count = 0;

    for node in &nodes {
        let label = node.get("label").unwrap().as_str().unwrap();
        if label == "parents" {
            parent_count += 1;
        } else if label == "users" {
            user_count += 1;
        }
    }

    assert_eq!(
        parent_count, 20,
        "Expected 20 parent nodes, found {}",
        parent_count
    );
    assert_eq!(
        user_count, 20,
        "Expected 20 user nodes, found {}",
        user_count
    );

    // Verify the number of edges (should be 20: one for each user-parent relationship)
    assert_eq!(edges.len(), 20, "Expected 20 edges, found {}", edges.len());

    // Verify edge types
    for edge in &edges {
        let edge_type = edge.get("label").unwrap().as_str().unwrap();
        assert_eq!(
            edge_type, "USERS_TO_PARENTS",
            "Expected edge type 'USERS_TO_PARENTS', found {}",
            edge_type
        );
    }

}

#[test]
fn test_dump_to_json_node_properties() {
    // Create a temporary file path for the JSONL output
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir;
    let output_path_str = output_path.to_str().unwrap();

    // Create a mock database and ingestor
    let conn = create_mock_sqlite_db(None).expect("Failed to create mock database");
    let mut ingestor = SqliteIngestor {
        sqlite_conn: conn,
        instance: "http://localhost:6969".to_string(),
        batch_size: 10,
        id_mappings: HashMap::new(),
        graph_schema: GraphSchema::new(),
    };

    // Dump the database to JSONL
    ingestor
        .dump_to_json(output_path_str)
        .expect("Failed to dump to JSONL");

    // Read the JSONL file
    let jsonl_content =
        fs::read_to_string(output_path.join("ingestion.jsonl")).expect("Failed to read JSONL file");
    let nodes_lines: Vec<&str> = jsonl_content.lines().filter(|line| {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        json_obj.get("payload_type").unwrap().as_str().unwrap() == "node"
    }).collect();
    let edges_lines: Vec<&str> = jsonl_content.lines().filter(|line| {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        json_obj.get("payload_type").unwrap().as_str().unwrap() == "edge"
    }).collect();

    // Parse each line as a JSON object
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    for line in nodes_lines {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        nodes.push(json_obj);
    }
    for line in edges_lines {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        edges.push(json_obj);
    }

    // Find a parent node and verify its properties
    let parent_node = nodes
        .iter()
        .find(|node| node.get("label").unwrap().as_str().unwrap() == "parents")
        .expect("No parent node found");

    let properties = parent_node.get("properties").unwrap().as_object().unwrap();

    // Verify parent node properties
    assert!(
        properties.contains_key("id"),
        "Parent node should have 'id' property"
    );
    assert!(
        properties.contains_key("name"),
        "Parent node should have 'name' property"
    );
    assert!(
        properties.contains_key("age"),
        "Parent node should have 'age' property"
    );
    assert!(
        properties.contains_key("grew_up_in"),
        "Parent node should have 'grew_up_in' property"
    );

    // Find a user node and verify its properties
    let user_node = nodes
        .iter()
        .find(|node| node.get("label").unwrap().as_str().unwrap() == "users")
        .expect("No user node found");

    let properties = user_node.get("properties").unwrap().as_object().unwrap();

    // Verify user node properties
    assert!(
        properties.contains_key("id"),
        "User node should have 'id' property"
    );
    assert!(
        properties.contains_key("name"),
        "User node should have 'name' property"
    );
    assert!(
        properties.contains_key("age"),
        "User node should have 'age' property"
    );
    assert!(
        properties.contains_key("city"),
        "User node should have 'city' property"
    );
    assert!(
        properties.contains_key("parent_id"),
        "User node should have 'parent_id' property"
    );
}

#[test]
fn test_dump_to_json_edge_relationships() {
    // Create a temporary file path for the JSONL output
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir;
    let output_path_str = output_path.to_str().unwrap();

    // Create a mock database and ingestor
    let conn = create_mock_sqlite_db(None).expect("Failed to create mock database");
    let mut ingestor = SqliteIngestor {
        sqlite_conn: conn,
        instance: "http://localhost:6969".to_string(),
        batch_size: 10,
        id_mappings: HashMap::new(),
        graph_schema: GraphSchema::new(),
    };

    // Dump the database to JSONL
    println!("Dumping to {}", output_path_str);
    ingestor
        .dump_to_json(output_path_str)
        .expect("Failed to dump to JSONL");

    // Read the JSONL file
    let jsonl_content =
        fs::read_to_string(output_path.join("ingestion.jsonl")).expect("Failed to read JSONL file");
    let nodes_lines: Vec<&str> = jsonl_content.lines().filter(|line| {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        json_obj.get("payload_type").unwrap().as_str().unwrap() == "node"
    }).collect();
    let edges_lines: Vec<&str> = jsonl_content.lines().filter(|line| {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        json_obj.get("payload_type").unwrap().as_str().unwrap() == "edge"
    }).collect();

    // Parse each line as a JSON object
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for line in nodes_lines {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        nodes.push(json_obj);
    }
    for line in edges_lines {
        let json_obj: JsonValue = serde_json::from_str(line).expect("Failed to parse JSON line");
        edges.push(json_obj);
    }

    // Create a map of node indices by label and id
    let mut node_indices = HashMap::new();

    for (idx, node) in nodes.iter().enumerate() {
        let label = node.get("label").unwrap().as_str().unwrap();
        let properties = node.get("properties").unwrap().as_object().unwrap();
        let id = properties.get("id").unwrap().as_i64().unwrap();

        node_indices.insert((label.to_string(), id), idx);
    }

    // Verify that each edge connects a user to a parent
    for edge in &edges {
        let from_idx = edge.get("from").unwrap().as_u64().unwrap() as usize;
        let to_idx = edge.get("to").unwrap().as_u64().unwrap() as usize;

        let from_node = &nodes[from_idx];
        let to_node = &nodes[to_idx];

        let from_label = from_node.get("label").unwrap().as_str().unwrap();
        let to_label = to_node.get("label").unwrap().as_str().unwrap();

        // The edge should go from a user to a parent
        assert_eq!(from_label, "users", "Edge should start from a user node");
        assert_eq!(to_label, "parents", "Edge should end at a parent node");

        // Get the user's parent_id and the parent's id
        let user_properties = from_node.get("properties").unwrap().as_object().unwrap();
        let parent_properties = to_node.get("properties").unwrap().as_object().unwrap();

        let user_parent_id = user_properties.get("parent_id").unwrap().as_i64().unwrap();
        let parent_id = parent_properties.get("id").unwrap().as_i64().unwrap();

        // Verify that the edge connects the correct user to the correct parent
        assert_eq!(
            user_parent_id, parent_id,
            "Edge should connect user with parent_id {} to parent with id {}",
            user_parent_id, parent_id
        );
    }

}

#[test]
fn test_ingest_basic() {
    let conn = create_mock_sqlite_db(None).expect("Failed to create mock database");
    let mut ingestor = SqliteIngestor {
        sqlite_conn: conn,
        instance: "http://localhost:6969".to_string(),
        batch_size: 10,
        id_mappings: HashMap::new(),
        graph_schema: GraphSchema::new(),
    };

    ingestor.ingest().expect("Failed to ingest");
}

#[test]
fn test_to_camel_case() {
    assert_eq!(to_camel_case("INTEGER"), "Integer");
    assert_eq!(to_camel_case("hello_world"), "HelloWorld");
    assert_eq!(to_camel_case("hello world"), "HelloWorld");
    assert_eq!(to_camel_case("hello-world"), "HelloWorld");
    assert_eq!(to_camel_case("helloWorld"), "HelloWorld");
    assert_eq!(to_camel_case(""), "");
}
