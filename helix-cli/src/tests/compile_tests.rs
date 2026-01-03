use crate::commands::compile::run;
use crate::config::HelixConfig;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper function to create a test project with valid schema and queries
fn setup_compile_project() -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml
    let config = HelixConfig::default_config("test-project");
    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    // Create queries directory
    let queries_dir = project_path.join("db");
    fs::create_dir_all(&queries_dir).expect("Failed to create queries directory");

    // Create valid schema.hx
    let schema_content = r#"
N::User {
    name: String,
    email: String,
}

N::Post {
    title: String,
    content: String,
}

E::Authored {
    From: User,
    To: Post,
}
"#;
    fs::write(queries_dir.join("schema.hx"), schema_content).expect("Failed to write schema.hx");

    // Create valid queries.hx
    let queries_content = r#"
QUERY GetUser(user_id: ID) =>
    user <- N<User>(user_id)
    RETURN user

QUERY GetUserPosts(user_id: ID) =>
    posts <- N<User>(user_id)::Out<Authored>
    RETURN posts
"#;
    fs::write(queries_dir.join("queries.hx"), queries_content).expect("Failed to write queries.hx");

    (temp_dir, project_path)
}

#[tokio::test]
async fn test_compile_success() {
    let (_temp_dir, project_path) = setup_compile_project();
    let _guard = std::env::set_current_dir(&project_path);

    let result = run(None, None).await;
    assert!(
        result.is_ok(),
        "Compile should succeed with valid project: {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let queries_file = project_path.join("queries.rs");
    assert!(
        queries_file.exists(),
        "Compiled queries.rs should be created"
    );
}

#[tokio::test]
async fn test_compile_with_custom_output_path() {
    let (_temp_dir, project_path) = setup_compile_project();
    let _guard = std::env::set_current_dir(&project_path);

    let output_dir = project_path.join("custom_output");
    fs::create_dir_all(&output_dir).expect("Failed to create custom output dir");

    let result = run(Some(output_dir.to_str().unwrap().to_string()), None).await;
    assert!(
        result.is_ok(),
        "Compile should succeed with custom output path: {:?}",
        result.err()
    );

    // Check that compiled output files were created in custom location
    let query_file = output_dir.join("queries.rs");
    assert!(
        query_file.exists(),
        "Compiled queries.rs should be created in custom output directory"
    );
}

#[tokio::test]
async fn test_compile_with_explicit_project_path() {
    let (_temp_dir, project_path) = setup_compile_project();

    let result = run(None, Some(project_path.to_str().unwrap().to_string())).await;
    assert!(
        result.is_ok(),
        "Compile should succeed with explicit project path: {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let query_file = project_path.join("queries.rs");
    assert!(query_file.exists(), "Compiled queries.rs should be created");
}

#[tokio::test]
async fn test_compile_fails_without_schema() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml
    let config = HelixConfig::default_config("test-project");
    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    // Create queries directory with only queries, no schema
    let queries_dir = project_path.join("db");
    fs::create_dir_all(&queries_dir).expect("Failed to create queries directory");

    let queries_content = r#"
QUERY GetUser(user_id: ID) =>
    user <- N<User>(user_id)
    RETURN user
"#;
    fs::write(queries_dir.join("queries.hx"), queries_content).expect("Failed to write queries.hx");

    let _guard = std::env::set_current_dir(&project_path);

    let result = run(None, None).await;
    assert!(result.is_err(), "Compile should fail without schema");
    let error_msg = format!("{:?}", result.err().unwrap());
    assert!(
        error_msg.contains("schema") || error_msg.contains("N::") || error_msg.contains("E::"),
        "Error should mention missing schema definitions"
    );
}

#[tokio::test]
async fn test_compile_fails_with_invalid_syntax() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml
    let config = HelixConfig::default_config("test-project");
    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    // Create queries directory
    let queries_dir = project_path.join("db");
    fs::create_dir_all(&queries_dir).expect("Failed to create queries directory");

    // Create schema
    let schema_content = r#"
N::User {
    name: String,
}
"#;
    fs::write(queries_dir.join("schema.hx"), schema_content).expect("Failed to write schema.hx");

    // Create queries with invalid syntax
    let invalid_queries = r#"
QUERY InvalidQuery
    this is not valid helix syntax!!!
"#;
    fs::write(queries_dir.join("queries.hx"), invalid_queries).expect("Failed to write queries.hx");

    let _guard = std::env::set_current_dir(&project_path);

    let result = run(None, None).await;
    assert!(result.is_err(), "Compile should fail with invalid syntax");
}

#[tokio::test]
async fn test_compile_fails_without_helix_toml() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();
    let _guard = std::env::set_current_dir(&project_path);

    let result = run(None, None).await;
    assert!(
        result.is_err(),
        "Compile should fail without helix.toml in project"
    );
    let error_msg = format!("{:?}", result.err().unwrap());
    assert!(
        error_msg.contains("not found") || error_msg.contains("helix.toml"),
        "Error should mention missing helix.toml"
    );
}

#[tokio::test]
async fn test_compile_with_schema_only() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml
    let config = HelixConfig::default_config("test-project");
    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    // Create queries directory with only schema
    let queries_dir = project_path.join("db");
    fs::create_dir_all(&queries_dir).expect("Failed to create queries directory");

    let schema_content = r#"
N::User {
    name: String,
    email: String,
}

E::Follows {
    From: User,
    To: User,
}
"#;
    fs::write(queries_dir.join("schema.hx"), schema_content).expect("Failed to write schema.hx");

    let _guard = std::env::set_current_dir(&project_path);

    let result = run(None, None).await;
    assert!(
        result.is_ok(),
        "Compile should succeed with schema only (queries are optional): {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let query_file = project_path.join("queries.rs");
    assert!(
        query_file.exists(),
        "Compiled queries.rs should be created even with schema only"
    );
}

#[tokio::test]
async fn test_compile_with_multiple_hx_files() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml
    let config = HelixConfig::default_config("test-project");
    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    // Create queries directory
    let queries_dir = project_path.join("db");
    fs::create_dir_all(&queries_dir).expect("Failed to create queries directory");

    // Create schema in one file
    let schema_content = r#"
N::User {
    name: String,
}
"#;
    fs::write(queries_dir.join("schema.hx"), schema_content).expect("Failed to write schema.hx");

    // Create additional schema in another file
    let more_schema = r#"
N::Post {
    title: String,
}

E::Authored {
    From: User,
    To: Post,
}
"#;
    fs::write(queries_dir.join("more_schema.hx"), more_schema)
        .expect("Failed to write more_schema.hx");

    // Create queries in yet another file
    let queries = r#"
QUERY GetUser(id: ID) =>
    user <- N<User>(id)
    RETURN user
"#;
    fs::write(queries_dir.join("queries.hx"), queries).expect("Failed to write queries.hx");

    let _guard = std::env::set_current_dir(&project_path);

    let result = run(None, None).await;
    assert!(
        result.is_ok(),
        "Compile should succeed with multiple .hx files: {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let query_file = project_path.join("queries.rs");
    assert!(query_file.exists(), "Compiled queries.rs should be created");
}

#[tokio::test]
async fn test_compile_with_custom_queries_path() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml with custom queries path
    let mut config = HelixConfig::default_config("test-project");
    config.project.queries = PathBuf::from("custom/helix/queries");
    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    // Create custom queries directory
    let queries_dir = project_path.join("custom/helix/queries");
    fs::create_dir_all(&queries_dir).expect("Failed to create custom queries directory");

    let schema_content = r#"
N::User {
    name: String,
}
"#;
    fs::write(queries_dir.join("schema.hx"), schema_content).expect("Failed to write schema.hx");

    let _guard = std::env::set_current_dir(&project_path);

    let result = run(None, None).await;
    assert!(
        result.is_ok(),
        "Compile should work with custom queries path: {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let query_file = project_path.join("queries.rs");
    assert!(query_file.exists(), "Compiled queries.rs should be created");
}

#[tokio::test]
async fn test_compile_creates_all_required_files() {
    let (_temp_dir, project_path) = setup_compile_project();
    let _guard = std::env::set_current_dir(&project_path);

    let result = run(None, None).await;
    assert!(result.is_ok(), "Compile should succeed");

    // Check for common generated files
    let query_file = project_path.join("queries.rs");
    assert!(query_file.exists(), "queries.rs should be created");

    // Verify the generated file has content
    let query_content = fs::read_to_string(&query_file).expect("Failed to read queries.rs");
    assert!(
        !query_content.is_empty(),
        "Generated queries.rs should not be empty"
    );
    assert!(
        query_content.contains("pub")
            || query_content.contains("use")
            || query_content.contains("impl"),
        "Generated queries.rs should contain Rust code"
    );
}
