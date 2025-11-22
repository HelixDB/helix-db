use crate::config::HelixConfig;
use crate::project::{ProjectContext, get_helix_cache_dir};
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper function to create a test project structure
fn setup_test_project() -> (TempDir, PathBuf) {
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

    (temp_dir, project_path)
}

#[test]
fn test_find_project_root_in_root_directory() {
    let (_temp_dir, project_path) = setup_test_project();

    let result = ProjectContext::find_and_load(Some(&project_path));
    assert!(result.is_ok(), "Should find project root");
    assert_eq!(result.unwrap().root, project_path);
}

#[test]
fn test_find_project_root_in_subdirectory() {
    let (_temp_dir, project_path) = setup_test_project();

    // Create a subdirectory
    let sub_dir = project_path.join("src/queries");
    fs::create_dir_all(&sub_dir).expect("Failed to create subdirectory");

    let result = ProjectContext::find_and_load(Some(&sub_dir));
    assert!(result.is_ok(), "Should find project root from subdirectory");
    assert_eq!(result.unwrap().root, project_path);
}

#[test]
fn test_find_project_root_in_nested_subdirectory() {
    let (_temp_dir, project_path) = setup_test_project();

    // Create deeply nested directory
    let nested_dir = project_path.join("a/b/c/d/e");
    fs::create_dir_all(&nested_dir).expect("Failed to create nested directory");

    let result = ProjectContext::find_and_load(Some(&nested_dir));
    assert!(
        result.is_ok(),
        "Should find project root from deeply nested directory"
    );
    assert_eq!(result.unwrap().root, project_path);
}

#[test]
fn test_find_project_root_fails_without_config() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    let result = ProjectContext::find_and_load(Some(&project_path));
    assert!(result.is_err(), "Should fail when no helix.toml exists");
    let error_msg = format!("{:?}", result.err().unwrap());
    assert!(
        error_msg.contains("not found"),
        "Error should mention config not found"
    );
}

#[test]
fn test_find_project_root_detects_v1_config() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create v1 config file
    let v1_config_path = project_path.join("config.hx.json");
    fs::write(&v1_config_path, "{}").expect("Failed to create v1 config");

    let result = ProjectContext::find_and_load(Some(&project_path));
    assert!(result.is_err(), "Should fail on v1 config");
    let error_msg = format!("{:?}", result.err().unwrap());
    assert!(
        error_msg.contains("v1"),
        "Error should mention v1 configuration"
    );
    assert!(
        error_msg.contains("migrate"),
        "Error should suggest migration"
    );
}

#[test]
fn test_project_context_find_and_load() {
    let (_temp_dir, project_path) = setup_test_project();

    let result = ProjectContext::find_and_load(Some(&project_path));
    assert!(result.is_ok(), "Should load project context");

    let context = result.unwrap();
    assert_eq!(context.root, project_path);
    assert_eq!(context.helix_dir, project_path.join(".helix"));
}

#[test]
fn test_project_context_instance_workspace() {
    let (_temp_dir, project_path) = setup_test_project();
    let context = ProjectContext::find_and_load(Some(&project_path)).unwrap();

    let workspace = context.instance_workspace("dev");
    assert_eq!(workspace, project_path.join(".helix/dev"));
}

#[test]
fn test_project_context_volumes_dir() {
    let (_temp_dir, project_path) = setup_test_project();
    let context = ProjectContext::find_and_load(Some(&project_path)).unwrap();

    let volumes_dir = context.volumes_dir();
    assert_eq!(volumes_dir, project_path.join(".helix/.volumes"));
}

#[test]
fn test_project_context_instance_volume() {
    let (_temp_dir, project_path) = setup_test_project();
    let context = ProjectContext::find_and_load(Some(&project_path)).unwrap();

    let volume = context.instance_volume("production");
    assert_eq!(volume, project_path.join(".helix/.volumes/production"));
}

#[test]
fn test_project_context_docker_compose_path() {
    let (_temp_dir, project_path) = setup_test_project();
    let context = ProjectContext::find_and_load(Some(&project_path)).unwrap();

    let compose_path = context.docker_compose_path("staging");
    assert_eq!(
        compose_path,
        project_path.join(".helix/staging/docker-compose.yml")
    );
}

#[test]
fn test_project_context_dockerfile_path() {
    let (_temp_dir, project_path) = setup_test_project();
    let context = ProjectContext::find_and_load(Some(&project_path)).unwrap();

    let dockerfile_path = context.dockerfile_path("dev");
    assert_eq!(dockerfile_path, project_path.join(".helix/dev/Dockerfile"));
}

#[test]
fn test_project_context_container_dir() {
    let (_temp_dir, project_path) = setup_test_project();
    let context = ProjectContext::find_and_load(Some(&project_path)).unwrap();

    let container_dir = context.container_dir("dev");
    assert_eq!(
        container_dir,
        project_path.join(".helix/dev/helix-container")
    );
}

#[test]
fn test_project_context_ensure_instance_dirs() {
    let (_temp_dir, project_path) = setup_test_project();
    let context = ProjectContext::find_and_load(Some(&project_path)).unwrap();

    // Directories should not exist initially
    let workspace = context.instance_workspace("test-instance");
    let volume = context.instance_volume("test-instance");
    let container = context.container_dir("test-instance");

    assert!(!workspace.exists(), "Workspace should not exist initially");
    assert!(!volume.exists(), "Volume should not exist initially");
    assert!(
        !container.exists(),
        "Container dir should not exist initially"
    );

    let result = context.ensure_instance_dirs("test-instance");
    assert!(result.is_ok(), "Should create instance directories");

    // Directories should now exist
    assert!(workspace.exists(), "Workspace should be created");
    assert!(workspace.is_dir(), "Workspace should be a directory");
    assert!(volume.exists(), "Volume should be created");
    assert!(volume.is_dir(), "Volume should be a directory");
    assert!(container.exists(), "Container dir should be created");
    assert!(container.is_dir(), "Container dir should be a directory");
}

#[test]
fn test_project_context_ensure_instance_dirs_idempotent() {
    let (_temp_dir, project_path) = setup_test_project();
    let context = ProjectContext::find_and_load(Some(&project_path)).unwrap();

    // Create directories first time
    let result1 = context.ensure_instance_dirs("test-instance");
    assert!(result1.is_ok(), "First call should succeed");

    // Create directories second time (should not fail)
    let result2 = context.ensure_instance_dirs("test-instance");
    assert!(result2.is_ok(), "Second call should be idempotent");
}

#[test]
fn test_get_helix_cache_dir_creates_directory() {
    // This test might affect the user's actual home directory,
    // so we'll just verify it doesn't error and returns a path
    let result = get_helix_cache_dir();
    assert!(result.is_ok(), "Should get helix cache directory");

    let cache_dir = result.unwrap();
    assert!(
        cache_dir.exists(),
        "Cache directory should exist after calling get_helix_cache_dir"
    );
    assert!(
        cache_dir.ends_with(".helix"),
        "Cache directory should end with .helix"
    );
}

#[test]
fn test_project_context_with_custom_queries_path() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create config with custom queries path
    let mut config = HelixConfig::default_config("test-project");
    config.project.queries = PathBuf::from("custom/queries");
    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    let result = ProjectContext::find_and_load(Some(&project_path));
    assert!(
        result.is_ok(),
        "Should load project with custom queries path"
    );

    let context = result.unwrap();
    assert_eq!(
        context.config.project.queries,
        PathBuf::from("custom/queries")
    );
}

#[test]
fn test_project_context_multiple_instances() {
    let (_temp_dir, project_path) = setup_test_project();
    let context = ProjectContext::find_and_load(Some(&project_path)).unwrap();

    // Create multiple instances
    let instances = vec!["dev", "staging", "production"];
    for instance in &instances {
        let result = context.ensure_instance_dirs(instance);
        assert!(
            result.is_ok(),
            "Should create directories for instance {}",
            instance
        );
    }

    // Verify all instances have their own directories
    for instance in &instances {
        let workspace = context.instance_workspace(instance);
        assert!(
            workspace.exists(),
            "Workspace for {} should exist",
            instance
        );

        let volume = context.instance_volume(instance);
        assert!(volume.exists(), "Volume for {} should exist", instance);

        let container = context.container_dir(instance);
        assert!(
            container.exists(),
            "Container dir for {} should exist",
            instance
        );
    }
}

#[test]
fn test_find_project_root_stops_at_filesystem_root() {
    // This test verifies we don't search infinitely
    // Start from a directory that definitely won't have helix.toml
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let deep_path = temp_dir.path().join("a/b/c/d/e/f/g/h/i/j");
    fs::create_dir_all(&deep_path).expect("Failed to create deep path");

    let result = ProjectContext::find_and_load(Some(&deep_path));
    assert!(
        result.is_err(),
        "Should fail after reaching filesystem root"
    );
}
