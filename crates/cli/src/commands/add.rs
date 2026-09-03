use crate::config::{
    EnterpriseInstanceConfig, LocalInstanceConfig, LocalStorageMode, S3StorageConfig,
};
use crate::output::Operation;
use crate::project::ProjectContext;
use crate::prompts;
use crate::AddTarget;
use eyre::{eyre, Result};
use std::path::PathBuf;

pub async fn run(path: Option<String>, target: Option<AddTarget>) -> Result<()> {
    let start_dir = path.map(PathBuf::from);
    let mut project = ProjectContext::find_and_load_allow_no_instances(start_dir.as_deref())?;
    let config_path = project.root.join("helix.toml");
    let target = match target {
        Some(target) => target,
        None if prompts::is_interactive() => prompts::select_add_target()?,
        None => {
            return Err(eyre!(
                "Specify an instance type: 'helix add local' or 'helix add cloud'"
            ));
        }
    };

    match target {
        AddTarget::Local {
            name,
            port,
            disk,
            s3,
        } => {
            validate_name(&name)?;
            ensure_available(&project, &name)?;
            let op = Operation::new("Adding", &name);
            project
                .config
                .local
                .insert(name.clone(), local_instance_config(port, disk, &s3)?);
            project.config.save_to_file(&config_path)?;
            op.success();
        }
        AddTarget::Enterprise {
            name,
            database,
            project: target_project,
            workspace,
        } => {
            validate_name(&name)?;
            ensure_available(&project, &name)?;
            let target = crate::commands::config::resolve_cloud_target(
                database,
                target_project.or_else(|| project.config.project.id.clone()),
                workspace.or_else(|| project.config.project.workspace_id.clone()),
            )
            .await?;
            let op = Operation::new("Adding", &name);
            project.config.enterprise.insert(
                name.clone(),
                EnterpriseInstanceConfig {
                    database: target.database,
                    workspace_id: Some(target.workspace_id),
                    project_id: Some(target.project_id),
                },
            );
            project.config.save_to_file(&config_path)?;
            op.success();
        }
    }

    Ok(())
}

fn local_instance_config(
    port: u16,
    disk: bool,
    s3: &crate::S3StorageArgs,
) -> Result<LocalInstanceConfig> {
    let mut config = LocalInstanceConfig {
        port,
        storage: LocalStorageMode::from_disk_flag(disk),
        ..LocalInstanceConfig::default()
    };
    if s3.has_any() {
        let uri = s3
            .storage_uri
            .as_deref()
            .ok_or_else(|| eyre!("--storage-uri is required when using S3 storage flags"))?;
        config.storage = LocalStorageMode::S3;
        config.s3 = Some(
            S3StorageConfig::from_uri(
                uri,
                s3.s3_region.clone(),
                s3.s3_endpoint_url.clone(),
                s3.s3_allow_http,
            )
            .map_err(|message| eyre!("{message}"))?,
        );
    }
    Ok(config)
}

fn ensure_available(project: &ProjectContext, name: &str) -> Result<()> {
    if project.config.local.contains_key(name) || project.config.enterprise.contains_key(name) {
        return Err(eyre::eyre!(
            "instance '{name}' already exists in helix.toml"
        ));
    }
    Ok(())
}

/// Rejects a `--name` the interactive prompt (`prompts::input_name`) would never
/// let through, before it's written to `helix.toml`. Without this, `helix add
/// --name <bad>` would save the config successfully and then every subsequent
/// `helix.toml` load would fail `HelixConfig::validate`, locking the project out
/// until the name was fixed by hand.
fn validate_name(name: &str) -> Result<()> {
    crate::config::validate_instance_name(name).map_err(|message| eyre::eyre!(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_name_rejects_disallowed_characters() {
        assert!(validate_name("my.prod").is_err());
        assert!(validate_name("../evil").is_err());
        assert!(validate_name("").is_err());
    }

    #[test]
    fn validate_name_accepts_the_normal_charset() {
        assert!(validate_name("dev").is_ok());
        assert!(validate_name("prod-1").is_ok());
        assert!(validate_name("my_instance").is_ok());
    }
}
