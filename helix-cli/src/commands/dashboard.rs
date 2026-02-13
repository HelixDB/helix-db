//! Dashboard management for Helix projects

use crate::DashboardAction;
use crate::commands::auth::require_auth;
use crate::commands::integrations::helix::CLOUD_AUTHORITY;
use crate::config::{BuildMode, ContainerRuntime, InstanceInfo};
use crate::docker::DockerManager;
use crate::metrics_sender::MetricsSender;
use crate::output::{self, Operation};
use crate::project::ProjectContext;
use crate::prompts;
use eyre::{Result, eyre};
use std::process::Command;

// Dashboard configuration constants
const DASHBOARD_IMAGE: &str = "public.ecr.aws/p8l2s5f1/helix-dashboard";
const DASHBOARD_TAG: &str = "latest";
const DASHBOARD_CONTAINER_NAME: &str = "helix-dashboard";
const DEFAULT_HELIX_PORT: u16 = 6969;

struct DisplayInfo {
    host: String,
    helix_port: u16,
    instance_name: Option<String>,
    mode: String,
}

pub async fn run(action: DashboardAction) -> Result<()> {
    match action {
        DashboardAction::Start {
            instance,
            port,
            host,
            helix_port,
            attach,
            restart,
        } => start(instance, port, host, helix_port, attach, restart).await,
        DashboardAction::Stop => stop(),
        DashboardAction::Status => status(),
    }
}

async fn start(
    instance: Option<String>,
    port: u16,
    host: Option<String>,
    helix_port: u16,
    attach: bool,
    restart: bool,
) -> Result<()> {
    // Detect runtime (works without project)
    let runtime = detect_runtime()?;

    // Check Docker/Podman availability
    DockerManager::check_runtime_available(runtime)?;

    // Check if dashboard is already running
    if is_dashboard_running(runtime)? {
        if restart {
            output::info("Stopping existing dashboard...");
            stop_dashboard_container(runtime)?;
        } else {
            output::warning("Dashboard is already running");
            if let Ok(existing_port) = get_dashboard_port(runtime) {
                output::info(&format!("Access it at: http://localhost:{existing_port}"));
            }
            output::info("Use 'helix dashboard stop' to stop it, or '--restart' to restart");
            return Ok(());
        }
    }

    // Warn if --helix-port is specified without --host
    if host.is_none() && helix_port != DEFAULT_HELIX_PORT {
        output::warning("--helix-port is ignored without --host; using project config or defaults");
    }

    // Prepare environment variables based on connection mode
    let (env_vars, display_info) = if let Some(host) = host {
        // Direct connection mode - no project needed
        prepare_direct_env_vars(&host, helix_port, runtime)?
    } else {
        // Try to use project config, or fall back to defaults
        prepare_env_vars_from_context(instance, runtime).await?
    };

    // Pull the dashboard image
    pull_dashboard_image(runtime)?;

    // Start the dashboard container
    start_dashboard_container(runtime, port, &env_vars, attach)?;

    if !attach {
        let url = format!("http://localhost:{port}");

        output::success("Dashboard started successfully");
        let mut details: Vec<(&str, String)> = vec![
            ("URL", url.clone()),
            ("Helix Host", display_info.host.clone()),
            ("Helix Port", display_info.helix_port.to_string()),
        ];
        if let Some(instance_name) = &display_info.instance_name {
            details.push(("Instance", instance_name.clone()));
        }
        details.push(("Mode", display_info.mode.clone()));
        let details_refs: Vec<(&str, &str)> =
            details.iter().map(|(k, v)| (*k, v.as_str())).collect();
        Operation::print_details(&details_refs);
        println!();
        output::info("Run 'helix dashboard stop' to stop the dashboard");

        // Open the dashboard in the default browser
        if let Err(e) = open::that(&url) {
            output::warning(&format!("Could not open browser: {e}"));
        }
    }

    Ok(())
}

fn prepare_direct_env_vars(
    host: &str,
    helix_port: u16,
    runtime: ContainerRuntime,
) -> Result<(Vec<String>, DisplayInfo)> {
    // Use host.docker.internal for Docker, host.containers.internal for Podman
    // when connecting to localhost
    let docker_host = if host == "localhost" || host == "127.0.0.1" {
        match runtime {
            ContainerRuntime::Docker => "host.docker.internal",
            ContainerRuntime::Podman => "host.containers.internal",
        }
    } else {
        host
    };

    let env_vars = vec![
        format!("HELIX_HOST={docker_host}"),
        format!("HELIX_PORT={helix_port}"),
    ];

    let display_info = DisplayInfo {
        host: host.to_string(),
        helix_port,
        instance_name: None,
        mode: "Direct".to_string(),
    };

    Ok((env_vars, display_info))
}

async fn prepare_env_vars_from_context(
    instance: Option<String>,
    runtime: ContainerRuntime,
) -> Result<(Vec<String>, DisplayInfo)> {
    // Try to load project context
    match ProjectContext::find_and_load(None) {
        Ok(project) => {
            // Resolve instance from project (with interactive selection if needed)
            let (instance_name, instance_config) = resolve_instance(&project, instance)?;

            // Check if instance is in dev mode (required for dashboard)
            check_dev_mode_requirement(&project, &instance_name, &instance_config).await?;

            // For local instances, check if the instance is running
            if instance_config.is_local() {
                check_instance_running(&project, &instance_name).await?;
            }

            let env_vars =
                prepare_environment_vars(&project, &instance_name, &instance_config).await?;

            let (host, helix_port, mode) = if instance_config.is_local() {
                let port = instance_config.port().unwrap_or(DEFAULT_HELIX_PORT);
                ("localhost".to_string(), port, "Local".to_string())
            } else {
                ("cloud".to_string(), 443, "Cloud".to_string())
            };

            let display_info = DisplayInfo {
                host,
                helix_port,
                instance_name: Some(instance_name),
                mode,
            };

            Ok((env_vars, display_info))
        }
        Err(_) => {
            // No project found - use defaults
            output::info(&format!(
                "No helix.toml found, using default connection (localhost:{DEFAULT_HELIX_PORT})"
            ));
            prepare_direct_env_vars("localhost", DEFAULT_HELIX_PORT, runtime)
        }
    }
}

fn resolve_instance<'a>(
    project: &'a ProjectContext,
    instance: Option<String>,
) -> Result<(String, InstanceInfo<'a>)> {
    match instance {
        Some(name) => {
            let config = project.config.get_instance(&name)?;
            Ok((name, config))
        }
        None => {
            // Get all instances for interactive selection
            let instances = project.config.list_instances_with_types();

            if instances.is_empty() {
                return Err(eyre!("No instances configured in helix.toml"));
            }

            // If interactive terminal, prompt user to select instance
            let name = if prompts::is_interactive() {
                prompts::select_instance(&instances)?
            } else {
                // Non-interactive: use first local instance, or first cloud instance
                let local_instances: Vec<_> = project.config.local.keys().collect();
                if !local_instances.is_empty() {
                    let name = local_instances[0].clone();
                    output::info(&format!("Using local instance: {name}"));
                    name
                } else {
                    let cloud_instances: Vec<_> = project.config.cloud.keys().collect();
                    let name = cloud_instances[0].clone();
                    output::info(&format!("Using cloud instance: {name}"));
                    name
                }
            };

            let config = project.config.get_instance(&name)?;
            Ok((name, config))
        }
    }
}

/// Check if the instance is in dev mode. If not, prompt user to redeploy in dev mode.
/// The dashboard requires dev mode to access internal debugging endpoints.
async fn check_dev_mode_requirement(
    project: &ProjectContext,
    instance_name: &str,
    instance_config: &InstanceInfo<'_>,
) -> Result<()> {
    let build_mode = instance_config.build_mode();

    if build_mode == BuildMode::Dev {
        // Already in dev mode, nothing to do
        return Ok(());
    }

    // Not in dev mode - warn the user
    output::warning(&format!(
        "Instance '{}' is currently in {:?} mode, not dev mode.",
        instance_name, build_mode
    ));
    output::warning("The dashboard requires dev mode to access internal debugging endpoints.");

    // If not interactive, just fail
    if !prompts::is_interactive() {
        return Err(eyre!(
            "Instance '{}' must be in dev mode for the dashboard. \
            Redeploy with 'helix push {} --dev' or update build_mode to 'dev' in helix.toml.",
            instance_name,
            instance_name
        ));
    }

    // Ask user if they want to redeploy in dev mode
    println!();
    output::warning("⚠️  WARNING: Redeploying in dev mode will:");
    output::warning("   - Restart the running instance");
    output::warning("   - Expose internal debug and dashboard endpoints");
    output::warning("   - This should NOT be used with production workloads");
    println!();

    let should_redeploy = prompts::confirm(&format!(
        "Do you want to redeploy '{}' in dev mode?",
        instance_name
    ))?;

    if !should_redeploy {
        return Err(eyre!(
            "Dashboard requires dev mode. Update build_mode to 'dev' in helix.toml or use --host flag to connect directly."
        ));
    }

    // Redeploy the instance in dev mode
    output::info(&format!("Redeploying '{}' in dev mode...", instance_name));

    // Update the config to use dev mode and redeploy
    let metrics_sender = MetricsSender::new()?;

    if instance_config.is_local() {
        // For local instances, we need to rebuild with dev mode
        // First update the config file
        let mut config = project.config.clone();
        if let Some(local_config) = config.local.get_mut(instance_name) {
            local_config.build_mode = BuildMode::Dev;
        }
        let config_path = project.root.join("helix.toml");
        config.save_to_file(&config_path)?;

        // Reload the project context and push
        crate::commands::push::run(Some(instance_name.to_string()), false, false, &metrics_sender).await?;
    } else {
        // For cloud instances, use the --dev flag
        crate::commands::push::run(Some(instance_name.to_string()), true, false, &metrics_sender).await?;
    }

    output::success(&format!(
        "Instance '{}' redeployed in dev mode",
        instance_name
    ));
    Ok(())
}

/// Check if a local instance is running. If not, prompt user to start or push it.
async fn check_instance_running(project: &ProjectContext, instance_name: &str) -> Result<()> {
    let docker = DockerManager::new(project);

    // Check if Docker/Podman is available
    DockerManager::check_runtime_available(docker.runtime)?;

    // Get container status
    let statuses = docker.get_project_status()?;
    let container_prefix = format!("helix-{}-{}", project.config.project.name, instance_name);

    // Find the container for this instance
    let container_status = statuses
        .iter()
        .find(|s| s.container_name.starts_with(&container_prefix));

    // Check if container is running
    let is_running = container_status
        .map(|s| s.status.to_lowercase().starts_with("up"))
        .unwrap_or(false);

    if is_running {
        // Instance is running, nothing to do
        return Ok(());
    }

    // If not interactive, just fail with instructions
    if !prompts::is_interactive() {
        return Err(eyre!(
            "Instance '{}' is not running. Start it with 'helix push {}'",
            instance_name,
            instance_name
        ));
    }

    // Interactive mode - prompt user
    output::warning(&format!("Instance '{}' is not running.", instance_name));

    let should_push = prompts::confirm(&format!("Do you want to start '{}'?", instance_name))?;

    if !should_push {
        return Err(eyre!(
            "Dashboard requires a running instance. Build and start it with 'helix push {}'",
            instance_name
        ));
    }

    // Push (build and start) the instance
    output::info(&format!(
        "Building and starting instance '{}'...",
        instance_name
    ));
    let metrics_sender = MetricsSender::new()?;
    crate::commands::push::run(Some(instance_name.to_string()), false, false, &metrics_sender).await?;
    output::success(&format!("Instance '{}' built and started", instance_name));

    Ok(())
}

async fn prepare_environment_vars(
    project: &ProjectContext,
    instance_name: &str,
    instance_config: &InstanceInfo<'_>,
) -> Result<Vec<String>> {
    let mut env_vars = Vec::new();

    if instance_config.is_local() {
        // Local instance - connect via Docker host networking
        let port = instance_config.port().unwrap_or(DEFAULT_HELIX_PORT);

        // Use host.docker.internal for Docker, host.containers.internal for Podman
        let host = match project.config.project.container_runtime {
            ContainerRuntime::Docker => "host.docker.internal",
            ContainerRuntime::Podman => "host.containers.internal",
        };

        env_vars.push(format!("HELIX_HOST={host}"));
        env_vars.push(format!("HELIX_PORT={port}"));
        env_vars.push(format!("HELIX_INSTANCE={instance_name}"));
    } else {
        // Cloud instance - use cloud URL and API key
        let credentials = require_auth().await?;

        // Get cloud URL based on instance type
        let cloud_url = get_cloud_url(instance_config)?;

        env_vars.push(format!("HELIX_CLOUD_URL={cloud_url}"));
        env_vars.push(format!("HELIX_API_KEY={}", credentials.helix_admin_key));
        env_vars.push(format!("HELIX_USER_ID={}", credentials.user_id));
        env_vars.push(format!("HELIX_INSTANCE={instance_name}"));

        // Add cluster ID for Helix Cloud instances
        if let Some(cluster_id) = instance_config.cluster_id() {
            env_vars.push(format!("HELIX_CLUSTER_ID={cluster_id}"));
        }
    }

    Ok(env_vars)
}

fn get_cloud_url(instance_config: &InstanceInfo) -> Result<String> {
    match instance_config {
        InstanceInfo::Helix(config) => Ok(format!(
            "https://{}/clusters/{}",
            *CLOUD_AUTHORITY, config.cluster_id
        )),
        InstanceInfo::FlyIo(_) => Err(eyre!(
            "Fly.io instances are not yet supported for the dashboard"
        )),
        InstanceInfo::Ecr(_) => Err(eyre!(
            "ECR instances are not yet supported for the dashboard"
        )),
        InstanceInfo::Local(_) => Err(eyre!("Local instances should not call get_cloud_url")),
    }
}

fn is_dashboard_running(runtime: ContainerRuntime) -> Result<bool> {
    let output = Command::new(runtime.binary())
        .args([
            "ps",
            "-q",
            "-f",
            &format!("name={DASHBOARD_CONTAINER_NAME}"),
        ])
        .output()
        .map_err(|e| eyre!("Failed to check dashboard status: {e}"))?;

    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

fn get_dashboard_port(runtime: ContainerRuntime) -> Result<u16> {
    let output = Command::new(runtime.binary())
        .args(["port", DASHBOARD_CONTAINER_NAME, "3000"])
        .output()
        .map_err(|e| eyre!("Failed to get dashboard port: {e}"))?;

    let port_mapping = String::from_utf8_lossy(&output.stdout);
    // Parse "0.0.0.0:3000" format
    port_mapping
        .trim()
        .split(':')
        .next_back()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| eyre!("Failed to parse dashboard port"))
}

fn pull_dashboard_image(runtime: ContainerRuntime) -> Result<()> {
    output::info("Pulling dashboard image...");

    let _ = Command::new(runtime.binary())
        .args(["logout", "public.ecr.aws"])
        .output();

    let image = format!("{DASHBOARD_IMAGE}:{DASHBOARD_TAG}");
    let cmd_output = Command::new(runtime.binary())
        .args(["pull", &image])
        .output()
        .map_err(|e| eyre!("Failed to pull dashboard image: {e}"))?;

    if !cmd_output.status.success() {
        let stderr = String::from_utf8_lossy(&cmd_output.stderr);
        return Err(eyre!("Failed to pull dashboard image:\n{stderr}"));
    }

    output::info("Image pulled successfully");
    Ok(())
}

fn start_dashboard_container(
    runtime: ContainerRuntime,
    port: u16,
    env_vars: &[String],
    attach: bool,
) -> Result<()> {
    output::info("Starting dashboard container...");

    let image = format!("{DASHBOARD_IMAGE}:{DASHBOARD_TAG}");

    let mut args = vec![
        "run".to_string(),
        "--name".to_string(),
        DASHBOARD_CONTAINER_NAME.to_string(),
        "-p".to_string(),
        format!("{port}:3000"),
        "--rm".to_string(),
    ];

    // Add detach flag if not attaching
    if !attach {
        args.push("-d".to_string());
    }

    // Add environment variables
    for env in env_vars {
        args.push("-e".to_string());
        args.push(env.clone());
    }

    // Add the image name
    args.push(image);

    let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    if attach {
        // Run in foreground - use spawn and wait
        let status = Command::new(runtime.binary())
            .args(&args_refs)
            .status()
            .map_err(|e| eyre!("Failed to start dashboard: {e}"))?;

        if !status.success() {
            return Err(eyre!("Dashboard exited with error"));
        }
    } else {
        // Run detached
        let output = Command::new(runtime.binary())
            .args(&args_refs)
            .output()
            .map_err(|e| eyre!("Failed to start dashboard: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to start dashboard:\n{stderr}"));
        }
    }

    Ok(())
}

fn stop_dashboard_container(runtime: ContainerRuntime) -> Result<()> {
    let output = Command::new(runtime.binary())
        .args(["stop", DASHBOARD_CONTAINER_NAME])
        .output()
        .map_err(|e| eyre!("Failed to stop dashboard: {e}"))?;

    if !output.status.success() {
        // Container might already be stopped, which is fine
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("No such container") && !stderr.contains("no such container") {
            return Err(eyre!("Failed to stop dashboard:\n{stderr}"));
        }
    }

    Ok(())
}

fn stop() -> Result<()> {
    // Detect runtime - try to load project config, fallback to checking available runtimes
    let runtime = detect_runtime()?;

    if !is_dashboard_running(runtime)? {
        output::info("Dashboard is not running");
        return Ok(());
    }

    output::info("Stopping dashboard...");
    stop_dashboard_container(runtime)?;
    output::success("Dashboard stopped");

    Ok(())
}

fn detect_runtime() -> Result<ContainerRuntime> {
    // Try to load project config for runtime preference
    if let Ok(project) = ProjectContext::find_and_load(None) {
        return Ok(project.config.project.container_runtime);
    }

    // Fallback: check if Docker is available, then Podman
    if let Ok(output) = Command::new("docker").arg("--version").output()
        && output.status.success()
    {
        return Ok(ContainerRuntime::Docker);
    }

    if let Ok(output) = Command::new("podman").arg("--version").output()
        && output.status.success()
    {
        return Ok(ContainerRuntime::Podman);
    }

    Err(eyre!("Neither Docker nor Podman is available"))
}

fn status() -> Result<()> {
    use color_eyre::owo_colors::OwoColorize;

    let runtime = detect_runtime()?;

    println!("\n{}", "Dashboard Status".bold().underline());

    if !is_dashboard_running(runtime)? {
        println!("  {}: Not running", "Status".bright_white().bold());
        return Ok(());
    }

    println!("  {}: Running", "Status".bright_white().bold());

    // Get port
    if let Ok(port) = get_dashboard_port(runtime) {
        println!("  {}: http://localhost:{port}", "URL".bright_white().bold());
    }

    // Get container info
    let cmd_output = Command::new(runtime.binary())
        .args([
            "inspect",
            DASHBOARD_CONTAINER_NAME,
            "--format",
            "{{range .Config.Env}}{{println .}}{{end}}",
        ])
        .output();

    if let Ok(cmd_output) = cmd_output {
        let env_output = String::from_utf8_lossy(&cmd_output.stdout);

        // Extract connection info from environment
        for line in env_output.lines() {
            if let Some(instance) = line.strip_prefix("HELIX_INSTANCE=") {
                println!("  {}: {instance}", "Instance".bright_white().bold());
            }
            if let Some(host) = line.strip_prefix("HELIX_HOST=") {
                println!("  {}: {host}", "Helix Host".bright_white().bold());
            }
            if let Some(port) = line.strip_prefix("HELIX_PORT=") {
                println!("  {}: {port}", "Helix Port".bright_white().bold());
            }
            if line.starts_with("HELIX_CLOUD_URL=") {
                println!("  {}: Cloud", "Mode".bright_white().bold());
            }
        }
    }

    Ok(())
}
