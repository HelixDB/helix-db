use crate::commands::branch::{persist_branch_config, prepare_output_user_dir, resolve_output_dir};
use crate::config::{BuildMode, DbConfig, HelixConfig, LocalInstanceConfig};
use crate::project::ProjectContext;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

fn setup_test_project() -> (TempDir, ProjectContext) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();

    let config = HelixConfig::default_config("test-project");
    let config_path = project_path.join("helix.toml");
    config
        .save_to_file(&config_path)
        .expect("Failed to save config");

    fs::create_dir_all(project_path.join(".helix")).expect("Failed to create .helix");

    let context =
        ProjectContext::find_and_load(Some(&project_path)).expect("Failed to load project context");

    (temp_dir, context)
}

#[test]
fn test_resolve_output_dir_defaults() {
    let (_temp_dir, project) = setup_test_project();
    let timestamp = "20240101-000000";

    let deploy_output = resolve_output_dir(&project, None, true, "branch-1", timestamp);
    assert_eq!(
        deploy_output,
        project.helix_dir.join(".volumes").join("branch-1")
    );

    let branch_output = resolve_output_dir(&project, None, false, "", timestamp);
    assert_eq!(
        branch_output,
        project
            .helix_dir
            .join(".branches")
            .join(format!("branch-{timestamp}"))
    );
}

#[test]
fn test_resolve_output_dir_relative_path() {
    let (_temp_dir, project) = setup_test_project();
    let timestamp = "20240101-000000";
    let output = resolve_output_dir(
        &project,
        Some(PathBuf::from("custom-output")),
        false,
        "",
        timestamp,
    );
    assert_eq!(output, project.root.join("custom-output"));
}

#[test]
fn test_branch_persists_deploy_config() {
    let (_temp_dir, mut project) = setup_test_project();
    let branch_name = "branch-1";
    let output_dir = project.root.join("branch-output");

    let branch_config = LocalInstanceConfig {
        port: Some(7777),
        build_mode: BuildMode::Debug,
        data_dir: Some(output_dir.clone()),
        db_config: DbConfig::default(),
    };

    persist_branch_config(&mut project, branch_name, branch_config)
        .expect("Failed to persist branch config");

    let config_path = project.root.join("helix.toml");
    let reloaded = HelixConfig::from_file(&config_path).expect("Failed to reload config");
    let saved = reloaded
        .local
        .get(branch_name)
        .expect("Missing persisted branch config");

    assert_eq!(saved.port, Some(7777));
    assert_eq!(saved.data_dir.as_ref(), Some(&output_dir));
}

#[test]
fn test_prepare_output_user_dir_rejects_existing_data() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_user_dir = temp_dir.path().join("branch-output").join("user");
    fs::create_dir_all(&output_user_dir).expect("Failed to create output dir");
    fs::write(output_user_dir.join("data.mdb"), "stub").expect("Failed to write data.mdb");

    let result = prepare_output_user_dir(&output_user_dir);
    assert!(
        result.is_err(),
        "Expected error when output directory already exists"
    );
}
