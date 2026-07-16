use crate::config::{InstanceInfo, LocalInstanceConfig, LocalStorageMode, S3StorageConfig};
use crate::local_runtime::LocalRuntime;
use crate::output::{Operation, Verbosity};
use crate::project::ProjectContext;
use crate::prompts;
use eyre::{eyre, Result};

pub async fn run(
    instance: Option<String>,
    foreground: bool,
    port: Option<u16>,
    disk: bool,
    s3: crate::S3StorageArgs,
    persist: bool,
) -> Result<()> {
    let mut project = ProjectContext::find_and_load(None)?;
    let _ = dotenvy::from_path(project.root.join(".env"));
    let instance = resolve_local_instance(&project, instance)?;
    let InstanceInfo::Local(config) = project.config.get_instance(&instance)? else {
        return Err(eyre!("'{instance}' is not a local v2 instance"));
    };
    let mut config = config.clone();
    if let Some(port) = port {
        config.port = port;
    }
    if disk {
        config.storage = LocalStorageMode::Disk;
        config.s3 = None;
    }
    apply_s3_overrides(&mut config, &s3)?;

    let op = Operation::new(if foreground { "Running" } else { "Starting" }, &instance);

    project.ensure_instance_dir(&instance)?;

    if persist {
        project
            .config
            .local
            .insert(instance.clone(), config.clone());
        project
            .config
            .save_to_file(&project.root.join("helix.toml"))?;
        crate::output::info("Saved port/storage settings to helix.toml.");
    }

    warn_about_storage(&project, &instance, &config);

    let runtime = LocalRuntime::new(&project);
    if foreground {
        crate::output::info("Running in foreground. Press Ctrl-C to stop.");
        runtime.run_foreground(&instance, &config).await?;
        op.success();
    } else {
        runtime.run_detached(&instance, &config)?;
        op.success();
        if Verbosity::current().show_normal() {
            Operation::print_details(&[
                ("URL", &format!("http://localhost:{}", config.port)),
                ("Container", &runtime.container_name(&instance)),
            ]);
        }
    }

    Ok(())
}

fn apply_s3_overrides(config: &mut LocalInstanceConfig, s3: &crate::S3StorageArgs) -> Result<()> {
    if !s3.has_any() {
        return Ok(());
    }

    if config.storage.is_s3() && s3.storage_uri.is_none() {
        let Some(existing) = &mut config.s3 else {
            return Err(eyre!("local instance uses s3 storage but has no s3 config"));
        };
        existing
            .apply_overrides(s3)
            .map_err(|message| eyre!("{message}"))?;
        return Ok(());
    }

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
    Ok(())
}

/// On-disk mode prints a one-line info note every run; in-memory mode warns about
/// data loss only the first time an instance is started (tracked with a marker in
/// the instance workspace) so repeat runs stay quiet.
fn warn_about_storage(project: &ProjectContext, instance: &str, config: &LocalInstanceConfig) {
    if config.storage.is_disk() {
        crate::output::info(
            "Local enterprise-dev is using on-disk storage. 'helix stop' preserves data; 'helix prune' deletes it.",
        );
        return;
    }
    if config.storage.is_s3() {
        crate::output::info(
            "Local enterprise-dev is using remote S3 storage. 'helix stop', 'helix restart', and 'helix prune' do not delete remote data.",
        );
        return;
    }

    let marker = project.instance_workspace(instance).join(".warned-memory");
    if marker.exists() {
        return;
    }
    crate::output::warning(
        "Local enterprise-dev uses in-memory storage. Stopping or restarting wipes local data.",
    );
    let _ = std::fs::write(&marker, b"");
}

fn resolve_local_instance(project: &ProjectContext, instance: Option<String>) -> Result<String> {
    if let Some(instance) = instance {
        return Ok(instance);
    }
    if prompts::is_interactive() && project.config.local.len() > 1 {
        return prompts::select_instance(&local_instances(project), "Run which local instance?");
    }
    if project.config.local.contains_key("dev") {
        return Ok("dev".to_string());
    }
    if project.config.local.len() == 1 {
        return Ok(project.config.local.keys().next().unwrap().clone());
    }
    Err(eyre!("No local instance specified"))
}

fn local_instances(project: &ProjectContext) -> Vec<(String, String)> {
    let mut instances: Vec<(String, String)> = project
        .config
        .local
        .iter()
        .map(|(name, config)| (name.clone(), format!("http://localhost:{}", config.port)))
        .collect();
    instances.sort_by(|a, b| a.0.cmp(&b.0));
    instances
}
