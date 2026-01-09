use crate::commands::backup::backup_instance_to_dir;
use crate::config::{BuildMode, DbConfig, HelixConfig, LocalInstanceConfig};
use crate::project::ProjectContext;
use heed3::EnvOpenOptions;
use std::fs;
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
fn test_backup_instance_to_dir_copies_data_file() {
    let (temp_dir, mut project) = setup_test_project();
    let instance_name = "primary";
    let data_dir = project.root.join("data-dir");

    project.config.local.insert(
        instance_name.to_string(),
        LocalInstanceConfig {
            port: Some(7777),
            build_mode: BuildMode::Debug,
            data_dir: Some(data_dir.clone()),
            db_config: DbConfig::default(),
        },
    );

    let user_dir = data_dir.join("user");
    fs::create_dir_all(&user_dir).expect("Failed to create user dir");
    {
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(1)
                .open(&user_dir)
                .expect("Failed to create LMDB env")
        };
        drop(env);
    }
    assert!(
        user_dir.join("data.mdb").exists(),
        "Expected LMDB data.mdb to exist"
    );

    let output_dir = temp_dir.path().join("backup-output");
    let completed =
        backup_instance_to_dir(&project, instance_name, &output_dir).expect("Backup failed");

    assert!(completed, "Expected backup to complete");
    assert!(
        output_dir.join("data.mdb").exists(),
        "Expected backup data.mdb to exist"
    );
}
