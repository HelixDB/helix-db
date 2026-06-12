use crate::AddTarget;
use crate::config::{
    DEFAULT_QUERY_AUTH_ENV, DEFAULT_QUERY_AUTH_HEADER, EnterpriseInstanceConfig,
    LocalInstanceConfig, LocalStorageMode,
};
use crate::output::Operation;
use crate::project::ProjectContext;
use crate::prompts;
use eyre::{Result, eyre};
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
        AddTarget::Local { name, port, disk } => {
            ensure_available(&project, &name)?;
            let op = Operation::new("Adding", &name);
            project.config.local.insert(
                name.clone(),
                LocalInstanceConfig {
                    port,
                    storage: LocalStorageMode::from_disk_flag(disk),
                    ..LocalInstanceConfig::default()
                },
            );
            project.config.save_to_file(&config_path)?;
            op.success();
        }
        AddTarget::Enterprise {
            name,
            cluster_id,
            gateway_url,
        } => {
            ensure_available(&project, &name)?;
            let target = crate::commands::config::resolve_enterprise_target(
                cluster_id,
                gateway_url,
                project.config.project.id.clone(),
                project.config.project.workspace_id.clone(),
            )
            .await?;
            let op = Operation::new("Adding", &name);
            project.config.enterprise.insert(
                name.clone(),
                EnterpriseInstanceConfig {
                    cluster_id: target.cluster_id,
                    workspace_id: target.workspace_id,
                    project_id: target.project_id,
                    gateway_url: target.gateway_url,
                    query_auth_header: DEFAULT_QUERY_AUTH_HEADER.to_string(),
                    query_auth_env: DEFAULT_QUERY_AUTH_ENV.to_string(),
                    availability_mode: None,
                    gateway_node_type: None,
                    db_node_type: None,
                    min_instances: 1,
                    max_instances: 1,
                    db_config: Default::default(),
                },
            );
            project.config.save_to_file(&config_path)?;
            op.success();
        }
    }

    Ok(())
}

fn ensure_available(project: &ProjectContext, name: &str) -> Result<()> {
    if project.config.local.contains_key(name) || project.config.enterprise.contains_key(name) {
        return Err(eyre::eyre!(
            "instance '{name}' already exists in helix.toml"
        ));
    }
    Ok(())
}
