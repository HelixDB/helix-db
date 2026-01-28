use crate::commands::check::run;
use crate::config::{DbConfig, HelixConfig, LocalInstanceConfig};
use crate::metrics_sender::MetricsSender;
use crate::tests::support;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper to create a metrics sender for tests
fn create_test_metrics_sender() -> MetricsSender {
    MetricsSender::new().expect("Failed to create metrics sender")
}

/// Helper function to create a test project with valid schema and queries
fn setup_valid_project() -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml with a local instance
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
// Node types
N::User {
    name: String,
    email: String,
}

N::Post {
    title: String,
    content: String,
}

// Edge types
E::Authored {
    From: User,
    To: Post,
}

E::Likes {
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

/// Helper function to create a project with empty schema
fn setup_project_without_schema() -> (TempDir, PathBuf) {
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

    // Create queries.hx without schema definitions
    let queries_content = r#"
QUERY GetUser(user_id: ID) =>
    user <- N<User>(user_id)
    RETURN user
"#;
    fs::write(queries_dir.join("queries.hx"), queries_content).expect("Failed to write queries.hx");

    (temp_dir, project_path)
}

/// Helper function to create a project with invalid syntax
fn setup_project_with_invalid_syntax() -> (TempDir, PathBuf) {
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

    // Create schema with valid definitions
    let schema_content = r#"
N::User {
    name: String,
}
"#;
    fs::write(queries_dir.join("schema.hx"), schema_content).expect("Failed to write schema.hx");

    // Create queries.hx with invalid syntax
    let invalid_queries = r#"
QUERY InvalidQuery {
    this is not valid helix syntax!!!
}
"#;
    fs::write(queries_dir.join("queries.hx"), invalid_queries).expect("Failed to write queries.hx");

    (temp_dir, project_path)
}

#[tokio::test]
async fn test_check_all_instances_success() {
    let (_temp_dir, project_path) = setup_valid_project();
    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(None, &metrics_sender).await
    })
    .await;
    assert!(
        result.is_ok(),
        "Check should succeed with valid project: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_check_specific_instance_success() {
    let (_temp_dir, project_path) = setup_valid_project();
    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(Some("dev".to_string()), &metrics_sender).await
    })
    .await;
    assert!(
        result.is_ok(),
        "Check should succeed for valid instance: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_check_nonexistent_instance_fails() {
    let (_temp_dir, project_path) = setup_valid_project();
    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(Some("nonexistent".to_string()), &metrics_sender).await
    })
    .await;
    assert!(
        result.is_err(),
        "Check should fail for nonexistent instance"
    );
    let error_msg = result.err().unwrap().to_string();
    assert!(
        error_msg.contains("not found") || error_msg.contains("nonexistent"),
        "Error should mention instance not found"
    );
}

#[tokio::test]
async fn test_check_fails_without_schema() {
    let (_temp_dir, project_path) = setup_project_without_schema();
    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(None, &metrics_sender).await
    })
    .await;
    assert!(result.is_err(), "Check should fail without schema");
    let error_msg = result.err().unwrap().to_string();
    assert!(
        error_msg.contains("schema") || error_msg.contains("N::") || error_msg.contains("E::"),
        "Error should mention missing schema definitions"
    );
}

#[tokio::test]
async fn test_check_fails_with_invalid_syntax() {
    let (_temp_dir, project_path) = setup_project_with_invalid_syntax();
    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(None, &metrics_sender).await
    })
    .await;
    assert!(result.is_err(), "Check should fail with invalid syntax");
}

#[tokio::test]
async fn test_check_fails_without_helix_toml() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();
    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(None, &metrics_sender).await
    })
    .await;
    assert!(
        result.is_err(),
        "Check should fail without helix.toml in project"
    );
    let error_msg = result.err().unwrap().to_string();
    assert!(
        error_msg.contains("not found") || error_msg.contains("helix.toml"),
        "Error should mention missing helix.toml"
    );
}

#[tokio::test]
async fn test_check_with_multiple_instances() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml with multiple instances
    let mut config = HelixConfig::default_config("test-project");
    config.local.insert(
        "staging".to_string(),
        LocalInstanceConfig {
            port: Some(6970),
            build_mode: crate::config::BuildMode::Dev,
            db_config: DbConfig::default(),
        },
    );
    config.local.insert(
        "production".to_string(),
        LocalInstanceConfig {
            port: Some(6971),
            build_mode: crate::config::BuildMode::Dev,
            db_config: DbConfig::default(),
        },
    );
    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    // Create valid queries and schema
    let queries_dir = project_path.join("db");
    fs::create_dir_all(&queries_dir).expect("Failed to create queries directory");

    let schema_content = r#"
N::User {
    name: String,
}

E::Follows {
    From: User,
    To: User,
}
"#;
    fs::write(queries_dir.join("schema.hx"), schema_content).expect("Failed to write schema.hx");

    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(None, &metrics_sender).await
    })
    .await;
    assert!(
        result.is_ok(),
        "Check should succeed with multiple instances: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_check_validates_each_instance_individually() {
    let (_temp_dir, project_path) = setup_valid_project();
    let metrics_sender = create_test_metrics_sender();

    // Check the specific instance
    let result = support::with_current_dir(&project_path, || async {
        run(Some("dev".to_string()), &metrics_sender).await
    })
    .await;
    assert!(result.is_ok(), "Check should validate dev instance");
}

#[tokio::test]
async fn test_check_with_empty_queries_directory() {
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

    // Create queries directory but leave it empty
    let queries_dir = project_path.join("db");
    fs::create_dir_all(&queries_dir).expect("Failed to create queries directory");

    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(None, &metrics_sender).await
    })
    .await;
    assert!(
        result.is_err(),
        "Check should fail with empty queries directory"
    );
}

#[tokio::test]
async fn test_check_with_schema_only() {
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

    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(None, &metrics_sender).await
    })
    .await;
    assert!(
        result.is_ok(),
        "Check should succeed with schema only (queries are optional): {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_check_with_multiple_hx_files() {
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

    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(None, &metrics_sender).await
    })
    .await;
    assert!(
        result.is_ok(),
        "Check should succeed with multiple .hx files: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_check_with_custom_queries_path() {
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

    let metrics_sender = create_test_metrics_sender();

    let result = support::with_current_dir(&project_path, || async {
        run(None, &metrics_sender).await
    })
    .await;
    assert!(
        result.is_ok(),
        "Check should work with custom queries path: {:?}",
        result.err()
    );
}
