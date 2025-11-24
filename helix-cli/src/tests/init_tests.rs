use crate::commands::init::run;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Helper function to create a temporary test directory
fn setup_test_dir() -> TempDir {
    TempDir::new().expect("Failed to create temp dir")
}

/// Helper function to check if helix.toml exists and is valid
fn assert_helix_config_exists(project_dir: &Path) {
    let config_path = project_dir.join("helix.toml");
    assert!(
        config_path.exists(),
        "helix.toml should exist at {:?}",
        config_path
    );

    let content = fs::read_to_string(&config_path).expect("Failed to read helix.toml");
    assert!(
        content.contains("[project]"),
        "helix.toml should contain [project] section"
    );
}

/// Helper function to check project structure
fn assert_project_structure(project_dir: &PathBuf, queries_path: &str) {
    // Check .helix directory
    let helix_dir = project_dir.join(".helix");
    assert!(helix_dir.exists(), ".helix directory should exist");
    assert!(helix_dir.is_dir(), ".helix should be a directory");

    // Check queries directory
    let queries_dir = project_dir.join(queries_path);
    assert!(
        queries_dir.exists(),
        "Queries directory should exist at {:?}",
        queries_dir
    );
    assert!(queries_dir.is_dir(), "Queries path should be a directory");

    // Check schema.hx
    let schema_file = queries_dir.join("schema.hx");
    assert!(schema_file.exists(), "schema.hx should exist");
    let schema_content = fs::read_to_string(&schema_file).expect("Failed to read schema.hx");
    assert!(
        schema_content.contains("N::"),
        "schema.hx should contain Node type example"
    );
    assert!(
        schema_content.contains("E::"),
        "schema.hx should contain Edge type example"
    );

    // Check queries.hx
    let queries_file = queries_dir.join("queries.hx");
    assert!(queries_file.exists(), "queries.hx should exist");
    let queries_content = fs::read_to_string(&queries_file).expect("Failed to read queries.hx");
    assert!(
        queries_content.contains("QUERY"),
        "queries.hx should contain QUERY example"
    );

    // Check .gitignore
    let gitignore = project_dir.join(".gitignore");
    assert!(gitignore.exists(), ".gitignore should exist");
    let gitignore_content = fs::read_to_string(&gitignore).expect("Failed to read .gitignore");
    assert!(
        gitignore_content.contains(".helix/"),
        ".gitignore should contain .helix/"
    );
}

#[tokio::test]
async fn test_init_creates_project_structure() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().to_path_buf();

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init should succeed: {:?}", result.err());
    assert_helix_config_exists(&project_path);
    assert_project_structure(&project_path, "queries");
}

#[tokio::test]
async fn test_init_with_default_path() {
    let temp_dir = setup_test_dir();
    let _guard = std::env::set_current_dir(temp_dir.path());

    let result = run(
        None, // Use current directory
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init with default path should succeed");
    assert_helix_config_exists(temp_dir.path());
}

#[tokio::test]
async fn test_init_with_custom_queries_path() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().to_path_buf();

    let custom_path = "custom/helix/queries";
    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        custom_path.to_string(),
        None,
    )
    .await;

    assert!(
        result.is_ok(),
        "Init with custom queries path should succeed"
    );
    assert_project_structure(&project_path, custom_path);

    // Verify config contains custom path
    let config_content =
        fs::read_to_string(project_path.join("helix.toml")).expect("Failed to read config");
    assert!(
        config_content.contains(custom_path),
        "Config should contain custom queries path"
    );
}

#[tokio::test]
async fn test_init_fails_if_helix_toml_exists() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml first
    fs::write(project_path.join("helix.toml"), "[project]").expect("Failed to create helix.toml");

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_err(), "Init should fail if helix.toml exists");
    let error_msg = format!("{:?}", result.err().unwrap());
    assert!(
        error_msg.contains("already exists"),
        "Error should mention file already exists"
    );
}

#[tokio::test]
async fn test_init_creates_directory_if_not_exists() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().join("new_project_dir");

    // Directory should not exist yet
    assert!(
        !project_path.exists(),
        "Project directory should not exist initially"
    );

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init should create directory");
    assert!(project_path.exists(), "Project directory should be created");
    assert!(project_path.is_dir(), "Project path should be a directory");
}

#[tokio::test]
async fn test_init_project_name_from_directory() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().join("my-awesome-project");

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init should succeed");

    let config_content =
        fs::read_to_string(project_path.join("helix.toml")).expect("Failed to read config");
    assert!(
        config_content.contains("my-awesome-project"),
        "Project name should be derived from directory name"
    );
}

#[tokio::test]
async fn test_init_gitignore_content() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().to_path_buf();

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init should succeed");

    let gitignore_path = project_path.join(".gitignore");
    let gitignore_content = fs::read_to_string(&gitignore_path).expect("Failed to read .gitignore");

    assert!(
        gitignore_content.contains(".helix/"),
        ".gitignore should ignore .helix/"
    );
    assert!(
        gitignore_content.contains("target/"),
        ".gitignore should ignore target/"
    );
    assert!(
        gitignore_content.contains("*.log"),
        ".gitignore should ignore log files"
    );
}

#[tokio::test]
async fn test_init_schema_hx_contains_examples() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().to_path_buf();

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init should succeed");

    let schema_path = project_path.join("queries/schema.hx");
    let schema_content = fs::read_to_string(&schema_path).expect("Failed to read schema.hx");

    // Check for Node type example
    assert!(
        schema_content.contains("N::User"),
        "schema.hx should contain N::User example"
    );
    assert!(
        schema_content.contains("Name: String"),
        "schema.hx should contain field examples"
    );

    // Check for Edge type example
    assert!(
        schema_content.contains("E::Knows"),
        "schema.hx should contain E::Knows example"
    );
    assert!(
        schema_content.contains("From: User"),
        "schema.hx should contain From field"
    );
    assert!(
        schema_content.contains("To: User"),
        "schema.hx should contain To field"
    );
}

#[tokio::test]
async fn test_init_queries_hx_contains_examples() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().to_path_buf();

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init should succeed");

    let queries_path = project_path.join("queries/queries.hx");
    let queries_content = fs::read_to_string(&queries_path).expect("Failed to read queries.hx");

    assert!(
        queries_content.contains("QUERY"),
        "queries.hx should contain QUERY keyword"
    );
    assert!(
        queries_content.contains("GetUserFriends"),
        "queries.hx should contain example query name"
    );
    assert!(
        queries_content.contains("RETURN"),
        "queries.hx should contain RETURN keyword"
    );
}

#[tokio::test]
async fn test_init_with_nested_queries_path() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().to_path_buf();
    let nested_path = "src/helix/queries";

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        nested_path.to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init with nested path should succeed");
    assert_project_structure(&project_path, nested_path);

    // Verify nested directories are created
    let nested_dir = project_path.join(nested_path);
    assert!(nested_dir.exists(), "Nested directory should exist");
    assert!(
        nested_dir.join("schema.hx").exists(),
        "schema.hx should be in nested directory"
    );
}

#[tokio::test]
async fn test_init_helix_dir_is_created() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().to_path_buf();

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init should succeed");

    let helix_dir = project_path.join(".helix");
    assert!(helix_dir.exists(), ".helix directory should exist");
    assert!(helix_dir.is_dir(), ".helix should be a directory");
}

#[tokio::test]
async fn test_init_config_has_valid_structure() {
    let temp_dir = setup_test_dir();
    let project_path = temp_dir.path().to_path_buf();

    let result = run(
        Some(project_path.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;

    assert!(result.is_ok(), "Init should succeed");

    let config_path = project_path.join("helix.toml");
    let config_content = fs::read_to_string(&config_path).expect("Failed to read helix.toml");

    // Check TOML structure
    assert!(
        config_content.contains("[project]"),
        "Config should have [project] section"
    );
    assert!(
        config_content.contains("name ="),
        "Config should have name field"
    );
    assert!(
        config_content.contains("queries ="),
        "Config should have queries field"
    );

    // Verify it's valid TOML
    let parsed: Result<toml::Value, _> = toml::from_str(&config_content);
    assert!(parsed.is_ok(), "Config should be valid TOML");
}

#[tokio::test]
async fn test_init_multiple_times_in_different_dirs() {
    let temp_dir = setup_test_dir();

    // Create first project
    let project1 = temp_dir.path().join("project1");
    let result1 = run(
        Some(project1.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;
    assert!(result1.is_ok(), "First init should succeed");

    // Create second project
    let project2 = temp_dir.path().join("project2");
    let result2 = run(
        Some(project2.to_str().unwrap().to_string()),
        "default".to_string(),
        "queries".to_string(),
        None,
    )
    .await;
    assert!(result2.is_ok(), "Second init should succeed");

    // Both should have independent configs
    assert!(project1.join("helix.toml").exists());
    assert!(project2.join("helix.toml").exists());
}
