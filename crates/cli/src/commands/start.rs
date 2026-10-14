use crate::config::{InstanceInfo, LocalInstanceConfig, LocalStorageMode, S3StorageConfig};
use crate::errors::CliError;
use crate::local_runtime::LocalRuntime;
use crate::output::{self, Operation};
use crate::project::ProjectContext;
use eyre::{eyre, Result};

pub async fn run(
    instance: Option<String>,
    foreground: bool,
    port: Option<u16>,
    disk: bool,
    s3: crate::S3StorageArgs,
    image: crate::image::ImageArgs,
    persist: bool,
) -> Result<()> {
    if foreground && output::OutputMode::current().is_json() {
        return Err(
            CliError::new("--foreground streams container logs, so it has no JSON result")
                .with_hint("drop --foreground to start in the background and get a JSON result")
                .into(),
        );
    }
    let mut project = ProjectContext::find_and_load(None)?;
    let _ = dotenvy::from_path(project.root.join(".env"));
    let instance = project.resolve_local_instance(instance, "Start which local instance?")?;
    let InstanceInfo::Local(config) = project.config.get_instance(&instance)? else {
        unreachable!("resolve_local_instance only returns local instances");
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
    config.tag = image.image_version.unwrap_or(config.tag);
    config.pull = image.pull.or(config.pull);

    let op = Operation::new(if foreground { "Running" } else { "Starting" }, &instance);

    project.ensure_instance_dir(&instance)?;

    let runtime = LocalRuntime::new(&project);
    let prepared = runtime.prepare_start(&config)?;

    if persist {
        project
            .config
            .local
            .insert(instance.clone(), config.clone());
        project
            .config
            .save_to_file(&project.root.join("helix.toml"))?;
        output::info("Saved port, storage, and image settings to helix.toml.");
    }

    warn_about_storage(&project, &instance, &config);

    if foreground {
        output::info("Running in foreground. Press Ctrl-C to stop.");
        runtime.run_foreground(&instance, prepared).await?;
        op.success();
    } else {
        runtime.run_detached(&instance, prepared)?;
        let url = format!("http://localhost:{}", config.port);
        let container = runtime.container_name(&instance);
        output::note(
            &instance,
            &output::table::key_values(&[("URL", url.clone()), ("Container", container.clone())]),
        );
        op.success();
        output::emit(
            &serde_json::json!({"instance": instance, "url": url, "container": container}),
            |_| Ok(()),
        )?;
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
        output::info(
            "Local HelixDB is using on-disk storage. 'helix stop' preserves data; 'helix prune' deletes it.",
        );
        return;
    }
    if config.storage.is_s3() {
        output::info(
            "Local HelixDB is using remote S3 storage. 'helix stop', 'helix restart', and 'helix prune' do not delete remote data.",
        );
        return;
    }

    let marker = project.instance_workspace(instance).join(".warned-memory");
    if marker.exists() {
        return;
    }
    output::warning(
        "Local HelixDB uses in-memory storage. Stopping or restarting wipes local data.",
    );
    let _ = std::fs::write(&marker, b"");
}
