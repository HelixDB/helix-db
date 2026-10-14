use crate::cloud::model::Named as _;
use crate::cloud::resolve::{Kind, Link, ResolvedDatabase, Scope};
use crate::config::{
    EnterpriseInstanceConfig, LocalInstanceConfig, LocalStorageMode, S3StorageConfig,
};
use crate::errors::CliError;
use crate::output::{self, Operation};
use crate::project::ProjectContext;
use crate::prompts;
use crate::{AddTarget, ScopeArgs};
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
            output::emit(&serde_json::json!({"instance": name}), |_| Ok(()))?;
        }
        AddTarget::Enterprise {
            name,
            database,
            project: target_project,
            workspace,
        } => {
            validate_name(&name)?;
            ensure_available(&project, &name)?;
            let op = Operation::new("Adding", &name);
            let scope = Scope::new(Link::from_config(&project.config)).await?;
            let args = ScopeArgs {
                workspace,
                project: target_project,
            };
            let database = match database {
                Some(database) => scope.database(Some(&database), &args).await?,
                // Adding means choosing a database that is not in helix.toml
                // yet, from the linked (or chosen) project.
                None => {
                    let added: Vec<_> = project
                        .config
                        .enterprise
                        .values()
                        .map(|instance| instance.database.clone())
                        .collect();
                    let owner = scope.project(&args).await?;
                    let (already_added, candidates): (Vec<_>, Vec<_>) = scope
                        .resolved_databases_in(&owner)
                        .await?
                        .into_iter()
                        .partition(|resolved| added.contains(&resolved.database.reference()));
                    if candidates.is_empty() && !already_added.is_empty() {
                        return Err(CliError::new(format!(
                            "every database in {} is already in helix.toml",
                            owner.label()
                        ))
                        .with_hint(
                            "create another with `helix database create <name>`, or pass --database to add one again",
                        )
                        .into());
                    }
                    scope.choose(Kind::Database, candidates)?
                }
            };
            let ResolvedDatabase { database, owner } = database;
            let linked = project.config.project.id.as_deref();
            if linked.is_some_and(|linked| linked != owner.project_id) {
                return Err(CliError::new(format!(
                    "{} belongs to project {}, but helix.toml is linked to {}",
                    database.reference(),
                    owner.project_id,
                    linked.unwrap_or_default()
                ))
                .with_hint(
                    "add a database from the linked project, or relink with `helix project link`",
                )
                .into());
            }
            project
                .config
                .project
                .id
                .get_or_insert_with(|| owner.project_id.clone());
            project
                .config
                .project
                .workspace_id
                .get_or_insert_with(|| owner.workspace_id.clone());
            project.config.enterprise.insert(
                name.clone(),
                EnterpriseInstanceConfig {
                    database: database.reference(),
                    workspace_id: Some(owner.workspace_id),
                    project_id: Some(owner.project_id),
                },
            );
            project.config.save_to_file(&config_path)?;
            output::step(&format!(
                "Linked {} ({})",
                database.label(),
                database.reference()
            ));
            op.success();
            output::emit(
                &serde_json::json!({"instance": name, "database": database.reference()}),
                |_| Ok(()),
            )?;
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
