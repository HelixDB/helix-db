// These tests manipulate the process's current working directory and environment variables.
// Due to shared process state, tests must be run serially to avoid interference:
//
//     cargo test --package helix-cli docker_tests -- --test-threads=1
//
// The EnvRestorer helper cleans up API key environment variables between tests.

use crate::config::{BuildMode, HelixConfig, LocalInstanceConfig};
use crate::docker::DockerManager;
use crate::project::ProjectContext;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper struct to save and restore API key environment variables
/// This prevents dotenvy::dotenv() from polluting the test environment
struct EnvRestorer {
    openai_key: Option<String>,
    gemini_key: Option<String>,
}

impl EnvRestorer {
    fn new() -> Self {
        Self {
            openai_key: env::var("OPENAI_API_KEY").ok(),
            gemini_key: env::var("GEMINI_API_KEY").ok(),
        }
    }

    fn clear_api_keys() {
        unsafe {
            env::remove_var("OPENAI_API_KEY");
            env::remove_var("GEMINI_API_KEY");
        }
    }
}

impl Drop for EnvRestorer {
    fn drop(&mut self) {
        unsafe {
            // Restore or remove OPENAI_API_KEY
            match &self.openai_key {
                Some(val) => env::set_var("OPENAI_API_KEY", val),
                None => env::remove_var("OPENAI_API_KEY"),
            }
            // Restore or remove GEMINI_API_KEY
            match &self.gemini_key {
                Some(val) => env::set_var("GEMINI_API_KEY", val),
                None => env::remove_var("GEMINI_API_KEY"),
            }
        }
    }
}

/// Helper function to create a test project structure with instances
fn setup_test_project_with_instance(
    instance_name: &str,
    port: Option<u16>,
) -> (TempDir, PathBuf, ProjectContext) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    // Create helix.toml with an instance
    let mut config = HelixConfig::default_config("test-project");

    let mut local_instances = HashMap::new();
    local_instances.insert(
        instance_name.to_string(),
        LocalInstanceConfig {
            port,
            build_mode: BuildMode::Dev,
            db_config: Default::default(),
        },
    );
    config.local = local_instances;

    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    // Create .helix directory
    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    // Load the project context
    let project =
        ProjectContext::find_and_load(Some(&project_path)).expect("Failed to load project context");

    (temp_dir, project_path, project)
}

/// Helper function to create a .env file in the project directory
fn create_env_file(project_path: &PathBuf, contents: &str) {
    let env_path = project_path.join(".env");
    fs::write(&env_path, contents).expect("Failed to create .env file");
}

/// Helper to parse environment variables from docker-compose.yml
fn parse_docker_compose_env_vars(compose_yaml: &str) -> Vec<String> {
    let mut env_vars = Vec::new();
    let mut in_environment_section = false;

    for line in compose_yaml.lines() {
        let trimmed = line.trim();
        if trimmed == "environment:" {
            in_environment_section = true;
            continue;
        }
        if in_environment_section {
            // If we hit a line that doesn't start with "- ", we're done with environment section
            if !trimmed.starts_with("- ") {
                break;
            }
            // Extract the env var (remove "- " prefix)
            if let Some(env_var) = trimmed.strip_prefix("- ") {
                env_vars.push(env_var.to_string());
            }
        }
    }

    env_vars
}

// ===== UNIT TESTS FOR environment_variables() =====

#[test]
fn test_environment_variables_basic_without_env_file() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", Some(8080));

    // Ensure no .env file exists
    let env_path = project_path.join(".env");
    if env_path.exists() {
        fs::remove_file(&env_path).expect("Failed to remove .env");
    }

    // Change to project directory
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);
    let env_vars = docker_manager.environment_variables("test-instance");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Should have exactly 4 built-in vars (API keys are cleared)
    assert_eq!(
        env_vars.len(),
        4,
        "Should have exactly 4 built-in environment variables"
    );

    // Check that all required built-in vars are present
    assert!(
        env_vars.iter().any(|v| v.starts_with("HELIX_PORT=")),
        "Should contain HELIX_PORT"
    );
    assert!(
        env_vars.iter().any(|v| v.starts_with("HELIX_DATA_DIR=")),
        "Should contain HELIX_DATA_DIR"
    );
    assert!(
        env_vars.iter().any(|v| v.starts_with("HELIX_INSTANCE=")),
        "Should contain HELIX_INSTANCE"
    );
    assert!(
        env_vars.iter().any(|v| v.starts_with("HELIX_PROJECT=")),
        "Should contain HELIX_PROJECT"
    );

    // Check specific values
    assert!(
        env_vars.contains(&"HELIX_PORT=8080".to_string()),
        "HELIX_PORT should be 8080"
    );
    assert!(
        env_vars.contains(&"HELIX_DATA_DIR=/data".to_string()),
        "HELIX_DATA_DIR should be /data"
    );
    assert!(
        env_vars.contains(&"HELIX_INSTANCE=test-instance".to_string()),
        "HELIX_INSTANCE should be test-instance"
    );
    assert!(
        env_vars.contains(&"HELIX_PROJECT=test-project".to_string()),
        "HELIX_PROJECT should be test-project"
    );
}

#[test]
fn test_environment_variables_with_default_port() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", None); // No port specified

    // Ensure no .env file exists
    let env_path = project_path.join(".env");
    if env_path.exists() {
        fs::remove_file(&env_path).expect("Failed to remove .env");
    }

    // Change to project directory
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);
    let env_vars = docker_manager.environment_variables("test-instance");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Should use default port 6969
    assert!(
        env_vars.contains(&"HELIX_PORT=6969".to_string()),
        "Should use default port 6969 when not specified"
    );
}

#[test]
fn test_environment_variables_with_env_file() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", Some(8080));

    // Create .env file with API keys
    create_env_file(
        &project_path,
        "OPENAI_API_KEY=sk-test-openai-key\nGEMINI_API_KEY=test-gemini-key\n",
    );

    // Change to project directory so dotenvy can find .env
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);
    let env_vars = docker_manager.environment_variables("test-instance");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Should have exactly 6 vars: 4 built-in + 2 API keys
    assert_eq!(
        env_vars.len(),
        6,
        "Should have exactly 6 environment variables"
    );

    // Check API keys are present
    assert!(
        env_vars.contains(&"OPENAI_API_KEY=sk-test-openai-key".to_string()),
        "Should contain OPENAI_API_KEY from .env file"
    );
    assert!(
        env_vars.contains(&"GEMINI_API_KEY=test-gemini-key".to_string()),
        "Should contain GEMINI_API_KEY from .env file"
    );
}

#[test]
fn test_environment_variables_missing_env_file_no_panic() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", Some(8080));

    // Ensure no .env file exists
    let env_path = project_path.join(".env");
    if env_path.exists() {
        fs::remove_file(&env_path).expect("Failed to remove .env");
    }

    // Change to project directory
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);
    // This should not panic even though .env doesn't exist
    let env_vars = docker_manager.environment_variables("test-instance");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Should have exactly 4 built-in vars (main test is that it doesn't panic)
    assert_eq!(
        env_vars.len(),
        4,
        "Should have exactly 4 built-in vars when .env is missing"
    );

    // Verify built-in vars are present
    assert!(env_vars.iter().any(|v| v.starts_with("HELIX_PORT=")));
    assert!(env_vars.iter().any(|v| v.starts_with("HELIX_DATA_DIR=")));
    assert!(env_vars.iter().any(|v| v.starts_with("HELIX_INSTANCE=")));
    assert!(env_vars.iter().any(|v| v.starts_with("HELIX_PROJECT=")));
}

#[test]
fn test_environment_variables_api_keys_from_environment() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", Some(8080));

    // Create .env file with API keys to simulate environment variables
    create_env_file(
        &project_path,
        "OPENAI_API_KEY=sk-env-openai-key\nGEMINI_API_KEY=env-gemini-key\n",
    );

    // Change to project directory so dotenvy can find .env
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);
    let env_vars = docker_manager.environment_variables("test-instance");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Should have exactly 6 vars: 4 built-in + 2 API keys
    assert_eq!(
        env_vars.len(),
        6,
        "Should have exactly 6 environment variables including API keys from environment"
    );

    // Check API keys from environment are present
    assert!(
        env_vars.contains(&"OPENAI_API_KEY=sk-env-openai-key".to_string()),
        "Should contain OPENAI_API_KEY from environment"
    );
    assert!(
        env_vars.contains(&"GEMINI_API_KEY=env-gemini-key".to_string()),
        "Should contain GEMINI_API_KEY from environment"
    );
}

#[test]
fn test_environment_variables_only_openai_key() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", Some(8080));

    // Create .env file with only OPENAI_API_KEY
    create_env_file(&project_path, "OPENAI_API_KEY=sk-only-openai\n");

    // Change to project directory so dotenvy can find .env
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);
    let env_vars = docker_manager.environment_variables("test-instance");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Should have exactly 5 vars: 4 built-in + 1 API key
    assert_eq!(
        env_vars.len(),
        5,
        "Should have exactly 5 environment variables (OPENAI_API_KEY)"
    );

    assert!(
        env_vars.contains(&"OPENAI_API_KEY=sk-only-openai".to_string()),
        "Should contain OPENAI_API_KEY"
    );
}

#[test]
fn test_environment_variables_only_gemini_key() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", Some(8080));

    // Create .env file with only GEMINI_API_KEY
    create_env_file(&project_path, "GEMINI_API_KEY=only-gemini\n");

    // Change to project directory so dotenvy can find .env
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);
    let env_vars = docker_manager.environment_variables("test-instance");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Should have exactly 5 vars: 4 built-in + 1 API key
    assert_eq!(
        env_vars.len(),
        5,
        "Should have exactly 5 environment variables (GEMINI_API_KEY)"
    );

    assert!(
        env_vars.contains(&"GEMINI_API_KEY=only-gemini".to_string()),
        "Should contain GEMINI_API_KEY"
    );
}

#[test]
fn test_environment_variables_different_instance_names() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("prod-instance", Some(9090));

    // Ensure no .env file exists
    let env_path = project_path.join(".env");
    if env_path.exists() {
        fs::remove_file(&env_path).expect("Failed to remove .env");
    }

    // Change to project directory
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);
    let env_vars = docker_manager.environment_variables("prod-instance");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    assert!(
        env_vars.contains(&"HELIX_INSTANCE=prod-instance".to_string()),
        "HELIX_INSTANCE should match instance name"
    );
    assert!(
        env_vars.contains(&"HELIX_PORT=9090".to_string()),
        "HELIX_PORT should match instance port"
    );
}

// ===== INTEGRATION TESTS FOR docker-compose GENERATION =====

#[test]
fn test_generate_docker_compose_contains_env_vars() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", Some(8080));

    // Create .env file with OPENAI_API_KEY
    create_env_file(&project_path, "OPENAI_API_KEY=sk-compose-test\n");

    // Change to project directory so dotenvy can find .env
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);

    // Get instance info
    let instance_info = project
        .config
        .get_instance("test-instance")
        .expect("Instance should exist");

    let compose_yaml = docker_manager
        .generate_docker_compose("test-instance", instance_info)
        .expect("Failed to generate docker-compose");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Parse env vars from the generated YAML
    let env_vars = parse_docker_compose_env_vars(&compose_yaml);

    // Should have exactly 5 vars: 4 built-in + 1 API key
    assert_eq!(
        env_vars.len(),
        5,
        "Docker-compose should contain exactly 5 environment variables"
    );

    // Check that env vars are in the YAML
    assert!(
        env_vars.contains(&"HELIX_PORT=8080".to_string()),
        "Docker-compose should contain HELIX_PORT"
    );
    assert!(
        env_vars.contains(&"HELIX_DATA_DIR=/data".to_string()),
        "Docker-compose should contain HELIX_DATA_DIR"
    );
    assert!(
        env_vars.contains(&"HELIX_INSTANCE=test-instance".to_string()),
        "Docker-compose should contain HELIX_INSTANCE"
    );
    assert!(
        env_vars.contains(&"HELIX_PROJECT=test-project".to_string()),
        "Docker-compose should contain HELIX_PROJECT"
    );
    assert!(
        env_vars.contains(&"OPENAI_API_KEY=sk-compose-test".to_string()),
        "Docker-compose should contain OPENAI_API_KEY"
    );
}

#[test]
fn test_generate_docker_compose_with_env_file() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", Some(8080));

    // Create .env file
    create_env_file(
        &project_path,
        "OPENAI_API_KEY=sk-from-file\nGEMINI_API_KEY=gemini-from-file\n",
    );

    // Change to project directory
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);

    // Get instance info
    let instance_info = project
        .config
        .get_instance("test-instance")
        .expect("Instance should exist");

    let compose_yaml = docker_manager
        .generate_docker_compose("test-instance", instance_info)
        .expect("Failed to generate docker-compose");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Parse env vars from the generated YAML
    let env_vars = parse_docker_compose_env_vars(&compose_yaml);

    // Should have exactly 6 vars: 4 built-in + 2 API keys
    assert_eq!(
        env_vars.len(),
        6,
        "Docker-compose should contain exactly 6 environment variables when .env file exists"
    );

    assert!(
        env_vars.contains(&"OPENAI_API_KEY=sk-from-file".to_string()),
        "Docker-compose should contain OPENAI_API_KEY from .env file"
    );
    assert!(
        env_vars.contains(&"GEMINI_API_KEY=gemini-from-file".to_string()),
        "Docker-compose should contain GEMINI_API_KEY from .env file"
    );
}

#[test]
fn test_generate_docker_compose_without_env_file() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("test-instance", Some(8080));

    // Ensure no .env file
    let env_path = project_path.join(".env");
    if env_path.exists() {
        fs::remove_file(&env_path).expect("Failed to remove .env");
    }

    // Change to project directory
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);

    // Get instance info
    let instance_info = project
        .config
        .get_instance("test-instance")
        .expect("Instance should exist");

    let compose_yaml = docker_manager
        .generate_docker_compose("test-instance", instance_info)
        .expect("Failed to generate docker-compose");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Parse env vars from the generated YAML
    let env_vars = parse_docker_compose_env_vars(&compose_yaml);
    assert_eq!(
        env_vars.len(),
        4,
        "Docker-compose should contain exactly 4 built-in environment variables without .env file"
    );

    // Verify built-in vars are present
    assert!(
        env_vars.iter().any(|v| v.starts_with("HELIX_PORT=")),
        "Docker-compose should contain HELIX_PORT"
    );
    assert!(
        env_vars.iter().any(|v| v.starts_with("HELIX_DATA_DIR=")),
        "Docker-compose should contain HELIX_DATA_DIR"
    );
    assert!(
        env_vars.iter().any(|v| v.starts_with("HELIX_INSTANCE=")),
        "Docker-compose should contain HELIX_INSTANCE"
    );
    assert!(
        env_vars.iter().any(|v| v.starts_with("HELIX_PROJECT=")),
        "Docker-compose should contain HELIX_PROJECT"
    );
}

#[test]
fn test_generate_docker_compose_yaml_format() {
    let _env_restorer = EnvRestorer::new();
    EnvRestorer::clear_api_keys();

    let (_temp_dir, project_path, project) =
        setup_test_project_with_instance("my-instance", Some(7070));

    // Ensure no .env file exists
    let env_path = project_path.join(".env");
    if env_path.exists() {
        fs::remove_file(&env_path).expect("Failed to remove .env");
    }

    // Change to project directory
    let original_dir = env::current_dir().expect("Failed to get current dir");
    env::set_current_dir(&project_path).expect("Failed to change directory");

    let docker_manager = DockerManager::new(&project);

    // Get instance info
    let instance_info = project
        .config
        .get_instance("my-instance")
        .expect("Instance should exist");

    let compose_yaml = docker_manager
        .generate_docker_compose("my-instance", instance_info)
        .expect("Failed to generate docker-compose");

    // Restore original directory (ignore if it fails due to parallel test cleanup)
    let _ = env::set_current_dir(original_dir);

    // Basic YAML structure checks
    assert!(
        compose_yaml.contains("services:"),
        "Should contain services section"
    );
    assert!(
        compose_yaml.contains("environment:"),
        "Should contain environment section"
    );
    assert!(
        compose_yaml.contains("networks:"),
        "Should contain networks section"
    );
    assert!(
        compose_yaml.contains("ports:"),
        "Should contain ports section"
    );
    assert!(compose_yaml.contains("7070:7070"), "Should map port 7070");
    assert!(
        compose_yaml.contains("helix-test-project-my-instance"),
        "Should contain correct project name in container name"
    );
}
