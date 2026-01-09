use crate::commands::{backup::backup_instance_to_dir, build};
use crate::config::{InstanceInfo, LocalInstanceConfig};
use crate::docker::DockerManager;
use crate::metrics_sender::MetricsSender;
use crate::project::ProjectContext;
use crate::prompts;
use crate::utils::{print_confirm, print_status, print_success, print_warning};
use eyre::{Result, eyre};
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};

pub async fn run(
    instance: String,
    output: Option<PathBuf>,
    name: Option<String>,
    port: Option<u16>,
    metrics_sender: &MetricsSender,
) -> Result<()> {
    let mut project = ProjectContext::find_and_load(None)?;
    let instance_config = project.config.get_instance(&instance)?;
    let source_config = match instance_config {
        InstanceInfo::Local(config) => config,
        _ => {
            return Err(eyre!("Branch is only supported for local instances"));
        }
    };

    let timestamp = chrono::Local::now().format("%Y%m%d-%H%M%S").to_string();
    let branch_name = name.unwrap_or_else(|| format!("{instance}-branch-{timestamp}"));
    ensure_branch_name_available(&project, &branch_name)?;
    let output_dir = resolve_output_dir(&project, output, &branch_name);
    let output_user_dir = output_dir.join("user");

    print_status(
        "BRANCH",
        &format!(
            "Creating new local instance from '{instance}' at {}",
            output_dir.display()
        ),
    );

    prepare_output_user_dir(&output_user_dir)?;
    let backup_completed = backup_instance_to_dir(&project, &instance, &output_user_dir)?;
    if !backup_completed {
        return Ok(());
    }

    print_success(&format!(
        "New local instance data created at {}",
        output_dir.display()
    ));

    let port = resolve_branch_port(port)?;

    let branch_config = LocalInstanceConfig {
        port: Some(port),
        build_mode: source_config.build_mode,
        data_dir: Some(output_dir.clone()),
        db_config: source_config.db_config.clone(),
    };

    persist_branch_config(&mut project, &branch_name, branch_config)?;

    print_status(
        "DEPLOY",
        &format!("Deploying branched instance '{branch_name}'"),
    );

    DockerManager::check_runtime_available(project.config.project.container_runtime)?;
    build::run(Some(branch_name.clone()), metrics_sender).await?;

    let docker = DockerManager::new(&project);
    docker.start_instance(&branch_name)?;

    print_success(&format!(
        "Branched local instance '{branch_name}' is now running"
    ));
    println!("  Local URL: http://localhost:{port}");
    let project_name = &project.config.project.name;
    println!("  Container: helix_{project_name}_{branch_name}");
    println!("  Data volume: {}", output_dir.display());

    Ok(())
}

pub(crate) fn resolve_output_dir(
    project: &ProjectContext,
    output: Option<PathBuf>,
    branch_name: &str,
) -> PathBuf {
    match output {
        Some(path) => {
            if path.is_absolute() {
                path
            } else {
                project.root.join(path)
            }
        }
        None => project.helix_dir.join(".volumes").join(branch_name),
    }
}

pub(crate) fn prepare_output_user_dir(output_user_dir: &Path) -> Result<()> {
    if output_user_dir.exists() {
        let data_path = output_user_dir.join("data.mdb");
        let has_entries = data_path.exists() || fs::read_dir(output_user_dir)?.next().is_some();
        if has_entries {
            if prompts::is_interactive() {
                print_warning(&format!(
                    "Output directory already exists at {}",
                    output_user_dir.display()
                ));
                let confirmed = print_confirm("Overwrite existing branch output directory?")?;
                if !confirmed {
                    return Err(eyre!(
                        "Output directory already exists at {}",
                        output_user_dir.display()
                    ));
                }
                fs::remove_dir_all(output_user_dir)?;
            } else {
                return Err(eyre!(
                    "Output directory already exists at {}",
                    output_user_dir.display()
                ));
            }
        }
    }

    fs::create_dir_all(output_user_dir)?;
    Ok(())
}

pub(crate) fn persist_branch_config(
    project: &mut ProjectContext,
    branch_name: &str,
    branch_config: LocalInstanceConfig,
) -> Result<()> {
    if project.config.local.contains_key(branch_name)
        || project.config.cloud.contains_key(branch_name)
    {
        return Err(eyre!("Instance '{branch_name}' already exists"));
    }

    project
        .config
        .local
        .insert(branch_name.to_string(), branch_config);
    let config_path = project.root.join("helix.toml");
    project.config.save_to_file(&config_path)?;
    Ok(())
}

fn select_available_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

fn resolve_branch_port(port: Option<u16>) -> Result<u16> {
    match port {
        Some(port) => {
            ensure_port_available(port)?;
            Ok(port)
        }
        None => select_available_port(),
    }
}

fn ensure_port_available(port: u16) -> Result<()> {
    TcpListener::bind(("127.0.0.1", port)).map_err(|err| {
        eyre!(
            "Port {port} is not available for the branched instance: {err}"
        )
    })?;
    Ok(())
}

fn ensure_branch_name_available(project: &ProjectContext, branch_name: &str) -> Result<()> {
    if project.config.local.contains_key(branch_name)
        || project.config.cloud.contains_key(branch_name)
    {
        return Err(eyre!("Instance '{branch_name}' already exists"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ensure_branch_name_available, ensure_port_available};
    use crate::config::{
        BuildMode, CloudConfig, CloudInstanceConfig, DbConfig, HelixConfig, LocalInstanceConfig,
    };
    use crate::project::ProjectContext;
    use std::collections::HashMap;
    use std::fs;
    use std::net::TcpListener;
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

        let context = ProjectContext::find_and_load(Some(&project_path))
            .expect("Failed to load project context");

        (temp_dir, context)
    }

    #[test]
    fn test_ensure_port_available_rejects_bound_port() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
        let port = listener
            .local_addr()
            .expect("Failed to read local addr")
            .port();

        let result = ensure_port_available(port);
        assert!(result.is_err(), "Expected port to be unavailable");
    }

    #[test]
    fn test_ensure_port_available_allows_free_port() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
        let port = listener
            .local_addr()
            .expect("Failed to read local addr")
            .port();
        drop(listener);

        let result = ensure_port_available(port);
        assert!(result.is_ok(), "Expected port to be available");
    }

    #[test]
    fn test_ensure_branch_name_available_rejects_existing() {
        let (_temp_dir, mut project) = setup_test_project();

        project.config.local.insert(
            "existing-local".to_string(),
            LocalInstanceConfig {
                port: Some(7777),
                build_mode: BuildMode::Debug,
                data_dir: None,
                db_config: DbConfig::default(),
            },
        );

        project.config.cloud.insert(
            "existing-cloud".to_string(),
            CloudConfig::Helix(CloudInstanceConfig {
                cluster_id: "cluster".to_string(),
                region: None,
                build_mode: BuildMode::Debug,
                env_vars: HashMap::new(),
                db_config: DbConfig::default(),
            }),
        );

        let local_result = ensure_branch_name_available(&project, "existing-local");
        assert!(local_result.is_err(), "Expected local name to be rejected");

        let cloud_result = ensure_branch_name_available(&project, "existing-cloud");
        assert!(cloud_result.is_err(), "Expected cloud name to be rejected");
    }

    #[test]
    fn test_ensure_branch_name_available_allows_unique() {
        let (_temp_dir, project) = setup_test_project();
        let result = ensure_branch_name_available(&project, "unique-branch");
        assert!(result.is_ok(), "Expected branch name to be available");
    }
}
