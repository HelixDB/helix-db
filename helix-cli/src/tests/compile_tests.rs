use crate::commands::compile::run;
use crate::config::HelixConfig;
use crate::tests::test_utils::TestContext;
use std::fs;
use std::path::PathBuf;

#[tokio::test]
async fn test_compile_success() {
    let ctx = TestContext::new();
    ctx.setup_valid_project();

    // Use explicit path instead of changing current directory
    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(
        result.is_ok(),
        "Compile should succeed with valid project: {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let queries_file = ctx.project_path.join("queries.rs");
    assert!(
        queries_file.exists(),
        "Compiled queries.rs should be created"
    );
}

#[tokio::test]
async fn test_compile_with_custom_output_path() {
    let ctx = TestContext::new();
    ctx.setup_valid_project();

    let output_dir = ctx.project_path.join("custom_output");
    fs::create_dir_all(&output_dir).expect("Failed to create custom output dir");

    let result = run(
        Some(output_dir.to_str().unwrap().to_string()),
        Some(ctx.project_path.to_str().unwrap().to_string()),
    )
    .await;
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
    let ctx = TestContext::new();
    ctx.setup_valid_project();

    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(
        result.is_ok(),
        "Compile should succeed with explicit project path: {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let query_file = ctx.project_path.join("queries.rs");
    assert!(query_file.exists(), "Compiled queries.rs should be created");
}

#[tokio::test]
async fn test_compile_fails_without_schema() {
    let ctx = TestContext::new();
    ctx.setup_project_without_schema();

    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(result.is_err(), "Compile should fail without schema");
    let error_msg = result.err().unwrap().to_string();
    assert!(
        error_msg.contains("schema") || error_msg.contains("N::") || error_msg.contains("E::"),
        "Error should mention missing schema definitions"
    );
}

#[tokio::test]
async fn test_compile_fails_with_invalid_syntax() {
    let ctx = TestContext::new();
    ctx.setup_project_with_invalid_syntax();

    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(result.is_err(), "Compile should fail with invalid syntax");
}

#[tokio::test]
async fn test_compile_fails_without_helix_toml() {
    let ctx = TestContext::new();
    // Don't set up any project

    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(
        result.is_err(),
        "Compile should fail without helix.toml in project"
    );
    let error_msg = result.err().unwrap().to_string();
    assert!(
        error_msg.contains("not found") || error_msg.contains("helix.toml"),
        "Error should mention missing helix.toml"
    );
}

#[tokio::test]
async fn test_compile_with_schema_only() {
    let ctx = TestContext::new();
    ctx.setup_schema_only_project();

    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(
        result.is_ok(),
        "Compile should succeed with schema only (queries are optional): {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let query_file = ctx.project_path.join("queries.rs");
    assert!(
        query_file.exists(),
        "Compiled queries.rs should be created even with schema only"
    );
}

#[tokio::test]
async fn test_compile_with_multiple_hx_files() {
    let ctx = TestContext::new();

    // Create helix.toml
    let config = HelixConfig::default_config("test-project");
    let config_path = ctx.project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(ctx.project_path.join(".helix")).expect("Failed to create .helix");

    // Create queries directory
    let queries_dir = ctx.project_path.join("db");
    fs::create_dir_all(&queries_dir).expect("Failed to create queries directory");

    // Create schema in one file (named 1_ to sort first alphabetically)
    let schema_content = r#"
N::User {
    name: String,
}
"#;
    fs::write(queries_dir.join("1_schema.hx"), schema_content)
        .expect("Failed to write 1_schema.hx");

    // Create additional schema in another file (named 2_ to sort second)
    let more_schema = r#"
N::Post {
    title: String,
}

E::Authored {
    From: User,
    To: Post,
}
"#;
    fs::write(queries_dir.join("2_more_schema.hx"), more_schema)
        .expect("Failed to write 2_more_schema.hx");

    // Create queries in yet another file (named 3_ to sort last)
    let queries = r#"
QUERY GetUser(id: ID) =>
    user <- N<User>(id)
    RETURN user
"#;
    fs::write(queries_dir.join("3_queries.hx"), queries).expect("Failed to write 3_queries.hx");

    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(
        result.is_ok(),
        "Compile should succeed with multiple .hx files: {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let query_file = ctx.project_path.join("queries.rs");
    assert!(query_file.exists(), "Compiled queries.rs should be created");
}

#[tokio::test]
async fn test_compile_with_custom_queries_path() {
    let ctx = TestContext::new();

    // Create helix.toml with custom queries path
    let mut config = HelixConfig::default_config("test-project");
    config.project.queries = PathBuf::from("custom/helix/queries");
    let config_path = ctx.project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(ctx.project_path.join(".helix")).expect("Failed to create .helix");

    // Create custom queries directory
    let queries_dir = ctx.project_path.join("custom/helix/queries");
    fs::create_dir_all(&queries_dir).expect("Failed to create custom queries directory");

    let schema_content = r#"
N::User {
    name: String,
}
"#;
    fs::write(queries_dir.join("schema.hx"), schema_content).expect("Failed to write schema.hx");

    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(
        result.is_ok(),
        "Compile should work with custom queries path: {:?}",
        result.err()
    );

    // Check that compiled output files were created
    let query_file = ctx.project_path.join("queries.rs");
    assert!(query_file.exists(), "Compiled queries.rs should be created");
}

#[tokio::test]
async fn test_compile_creates_all_required_files() {
    let ctx = TestContext::new();
    ctx.setup_valid_project();

    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(result.is_ok(), "Compile should succeed");

    // Check for common generated files
    let query_file = ctx.project_path.join("queries.rs");
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

#[tokio::test]
async fn test_compile_out_to_search_v_fetches_vector_data() {
    let ctx = TestContext::new();

    // Create helix.toml
    let config = HelixConfig::default_config("test-project");
    let config_path = ctx.project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(ctx.project_path.join(".helix")).expect("Failed to create .helix");

    // Create queries directory
    let queries_dir = ctx.project_path.join("db");
    fs::create_dir_all(&queries_dir).expect("Failed to create queries directory");

    let schema = r#"
N::Call {
    INDEX room_id: String,
}

V::CallTranscriptChunk {
    content: String,
}

E::Call_Has_TranscriptChunk {
    From: Call,
    To: CallTranscriptChunk,
}
"#;
    fs::write(queries_dir.join("schema.hx"), schema).expect("Failed to write schema.hx");

    let queries = r#"
QUERY SearchCallTranscriptChunks(room_id: String, query: String, limit: I64) =>
      call <- N<Call>({room_id: room_id})
      results <- call::Out<Call_Has_TranscriptChunk>::SearchV<CallTranscriptChunk>(Embed(query), limit)
      RETURN results
"#;
    fs::write(queries_dir.join("queries.hx"), queries).expect("Failed to write queries.hx");

    let result = run(None, Some(ctx.project_path.to_str().unwrap().to_string())).await;
    assert!(
        result.is_ok(),
        "Compile should succeed for Out<...>::SearchV<...>: {:?}",
        result.err()
    );

    let query_file = ctx.project_path.join("queries.rs");
    let query_content = fs::read_to_string(&query_file).expect("Failed to read queries.rs");

    assert!(
        query_content.contains(".out_vec(\"Call_Has_TranscriptChunk\", true)"),
        "Out<...>::SearchV<...> should request full vector data"
    );
    assert!(
        query_content.contains(".brute_force_search_v("),
        "Expected generated SearchV graph step in query code"
    );
    assert!(
        !query_content.contains(".out_vec(\"Call_Has_TranscriptChunk\", false)"),
        "Out<...>::SearchV<...> should not emit lazy vector fetch"
    );
}
