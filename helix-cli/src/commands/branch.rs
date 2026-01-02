use crate::commands::build;
use crate::config::{InstanceInfo, LocalInstanceConfig};
use crate::docker::DockerManager;
use crate::metrics_sender::MetricsSender;
use crate::project::ProjectContext;
use crate::prompts;
use crate::utils::{print_confirm, print_status, print_success, print_warning};
use eyre::{Result, eyre};
use heed3::{CompactionOption, EnvFlags, EnvOpenOptions};
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};

const TEN_GB: u64 = 10 * 1024 * 1024 * 1024;

pub async fn run(
    instance: String,
    output: Option<PathBuf>,
    deploy: bool,
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
    let branch_name = if deploy {
        name.unwrap_or_else(|| format!("{instance}-branch-{timestamp}"))
    } else {
        if name.is_some() {
            print_warning("--name is ignored unless --deploy is set");
        }
        String::new()
    };

    let output_dir = resolve_output_dir(&project, output, deploy, &branch_name, &timestamp);
    let output_user_dir = output_dir.join("user");

    print_status(
        "BRANCH",
        &format!(
            "Branching instance '{instance}' to {}",
            output_dir.display()
        ),
    );

    let env_path = project.instance_user_dir(&instance)?;
    let data_file = project.instance_data_file(&instance)?;
    let env_path = Path::new(&env_path);

    if !env_path.exists() {
        return Err(eyre!(
            "Instance LMDB environment not found at {:?}",
            env_path
        ));
    }

    if !data_file.exists() {
        return Err(eyre!("Instance data file not found at {:?}", data_file));
    }

    prepare_output_user_dir(&output_user_dir)?;

    let total_size = fs::metadata(&data_file)?.len();
    if total_size > TEN_GB {
        let size_gb = (total_size as f64) / (1024.0 * 1024.0 * 1024.0);
        print_warning(&format!(
            "Branch size is {:.2} GB. Taking atomic snapshot... this may take time depending on DB size",
            size_gb
        ));
        let confirmed = print_confirm("Do you want to continue?")?;
        if !confirmed {
            print_status("CANCEL", "Branch aborted by user");
            return Ok(());
        }
    }

    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .max_dbs(200)
            .max_readers(200)
            .open(env_path)?
    };

    env.copy_to_path(output_user_dir.join("data.mdb"), CompactionOption::Disabled)?;

    print_success(&format!(
        "Branch for '{instance}' created at {}",
        output_dir.display()
    ));

    if !deploy {
        return Ok(());
    }

    let port = match port {
        Some(port) => port,
        None => select_available_port()?,
    };

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

    print_success(&format!("Instance '{branch_name}' is now running"));
    println!("  Local URL: http://localhost:{port}");
    let project_name = &project.config.project.name;
    println!("  Container: helix_{project_name}_{branch_name}");
    println!("  Data volume: {}", output_dir.display());

    Ok(())
}

pub(crate) fn resolve_output_dir(
    project: &ProjectContext,
    output: Option<PathBuf>,
    deploy: bool,
    branch_name: &str,
    timestamp: &str,
) -> PathBuf {
    match output {
        Some(path) => {
            if path.is_absolute() {
                path
            } else {
                project.root.join(path)
            }
        }
        None => {
            if deploy {
                project.helix_dir.join(".volumes").join(branch_name)
            } else {
                project
                    .helix_dir
                    .join(".branches")
                    .join(format!("branch-{timestamp}"))
            }
        }
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
