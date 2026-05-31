use crate::DashboardAction;
use crate::config::{ContainerRuntime, InstanceInfo};
use crate::local_runtime::LocalRuntime;
use crate::output::Verbosity;
use crate::project::ProjectContext;
use crate::utils::command_exists;
use eyre::{Result, eyre};
use std::process::{Command, Stdio};

// The dashboard is always a container (Docker/Podman), never native.

const DASHBOARD_IMAGE: &str = "public.ecr.aws/p8l2s5f1/helix-dashboard:latest";
const DASHBOARD_CONTAINER_NAME: &str = "helix-dashboard";

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
    let runtime = detect_runtime()?;
    LocalRuntime::check_available(runtime)?;
    if restart {
        let _ = stop();
    }
    let (host, helix_port) = resolve_helix_target(instance, host, helix_port)?;
    let docker_host = if host == "localhost" || host == "127.0.0.1" {
        match runtime {
            ContainerRuntime::Docker => "host.docker.internal".to_string(),
            ContainerRuntime::Podman => "host.containers.internal".to_string(),
            ContainerRuntime::Native => "localhost".to_string(),
        }
    } else {
        host
    };

    let _ = remove_dashboard(runtime)?;
    pull_dashboard_image(runtime)?;

    let mut command = Command::new(runtime.binary());
    command.args([
        "run",
        "--name",
        DASHBOARD_CONTAINER_NAME,
        "-p",
        &format!("{port}:3000"),
        "-e",
        &format!("HELIX_HOST={docker_host}"),
        "-e",
        &format!("HELIX_PORT={helix_port}"),
    ]);
    if !attach {
        command.arg("-d");
    }
    command.arg(DASHBOARD_IMAGE);
    let status = command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;
    if !status.success() {
        return Err(eyre!("Failed to start dashboard"));
    }
    if !attach {
        crate::output::success(&format!("Dashboard started at http://localhost:{port}"));
    }
    Ok(())
}

fn resolve_helix_target(
    instance: Option<String>,
    host: Option<String>,
    helix_port: u16,
) -> Result<(String, u16)> {
    if let Some(host) = host {
        return Ok((host, helix_port));
    }
    let project = ProjectContext::find_and_load(None)?;
    let instance = instance.unwrap_or_else(|| "dev".to_string());
    match project.config.get_instance(&instance)? {
        InstanceInfo::Local(config) => Ok(("localhost".to_string(), config.port)),
        InstanceInfo::Enterprise(config) => {
            let gateway = config.gateway_url.as_deref().ok_or_else(|| {
                eyre!("Enterprise instance '{instance}' does not have gateway_url configured")
            })?;
            Ok((
                gateway
                    .trim_start_matches("https://")
                    .trim_start_matches("http://")
                    .to_string(),
                443,
            ))
        }
    }
}

fn stop() -> Result<()> {
    let runtime = detect_runtime()?;
    if remove_dashboard(runtime)? {
        crate::output::success("Dashboard stopped");
    } else {
        crate::output::info("Dashboard was not running");
    }
    Ok(())
}

fn remove_dashboard(runtime: ContainerRuntime) -> Result<bool> {
    let output = Command::new(runtime.binary())
        .args(["rm", "-f", DASHBOARD_CONTAINER_NAME])
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains("No such container") {
        return Ok(false);
    }
    if !output.status.success() {
        return Err(eyre!("Failed to stop dashboard:\n{stderr}"));
    }
    Ok(true)
}

fn pull_dashboard_image(runtime: ContainerRuntime) -> Result<()> {
    if Verbosity::current().show_verbose() {
        let status = Command::new(runtime.binary())
            .args(["pull", DASHBOARD_IMAGE])
            .status()?;
        if !status.success() {
            return Err(eyre!("Failed to pull dashboard image"));
        }
        return Ok(());
    }

    let output = Command::new(runtime.binary())
        .args(["pull", DASHBOARD_IMAGE])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre!("Failed to pull dashboard image:\n{stderr}"));
    }
    Ok(())
}

fn status() -> Result<()> {
    let runtime = detect_runtime()?;
    let output = Command::new(runtime.binary())
        .args([
            "ps",
            "-a",
            "--filter",
            &format!("name=^{DASHBOARD_CONTAINER_NAME}$"),
            "--format",
            "{{.Names}}\t{{.Status}}\t{{.Ports}}",
        ])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    if stdout.trim().is_empty() {
        crate::output::info("Dashboard not running");
    } else {
        print!("{stdout}");
    }
    Ok(())
}

fn detect_runtime() -> Result<ContainerRuntime> {
    if command_exists("docker") {
        Ok(ContainerRuntime::Docker)
    } else if command_exists("podman") {
        Ok(ContainerRuntime::Podman)
    } else {
        Err(eyre!("Docker or Podman is required"))
    }
}
