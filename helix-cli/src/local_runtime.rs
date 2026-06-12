use crate::config::{ContainerRuntime, LocalInstanceConfig};
use crate::errors::CliError;
use crate::output::Step;
use crate::project::ProjectContext;
use crate::utils::command_exists;
use eyre::{Result, eyre};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tokio::process::Command as TokioCommand;

pub const CONTAINER_PORT: u16 = 8080;
/// How long to wait for a runtime daemon to become ready after we start it.
/// Docker Desktop cold-boot can take 30–60s, so we allow generous headroom.
const RUNTIME_START_TIMEOUT: Duration = Duration::from_secs(120);
/// How often to re-probe the daemon while waiting for it to come up.
const RUNTIME_POLL_INTERVAL: Duration = Duration::from_secs(2);
const MINIO_IMAGE: &str = "minio/minio:latest";
const MINIO_MC_IMAGE: &str = "minio/mc:latest";
const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const LOCAL_S3_BUCKET: &str = "helix-db";
const LOCAL_S3_REGION: &str = "us-east-1";
const LOCAL_DB_PATH: &str = "db/";

#[derive(Debug, Clone)]
pub struct LocalRuntime {
    runtime: ContainerRuntime,
    project_name: String,
}

#[derive(Debug, Clone)]
pub struct LocalStatus {
    pub instance_name: String,
    pub container_name: String,
    pub status: String,
    pub ports: String,
}

#[derive(Debug, Clone)]
struct DiskRuntimeResources {
    minio_container: String,
    network: String,
    volume: String,
}

impl LocalRuntime {
    pub fn new(project: &ProjectContext) -> Self {
        Self {
            runtime: project.config.project.container_runtime,
            project_name: project.config.project.name.clone(),
        }
    }

    pub fn check_available(runtime: ContainerRuntime) -> Result<()> {
        let output = match Command::new(runtime.binary()).arg("info").output() {
            Ok(output) => output,
            // The binary itself couldn't be spawned — the runtime isn't installed,
            // so there's nothing for us to auto-start.
            Err(e) => {
                return Err(not_installed_error(runtime, &e.to_string(), command_exists).into());
            }
        };

        if output.status.success() {
            return Ok(());
        }

        // The binary exists but the daemon is down. Try to start it automatically,
        // then re-probe. Only surface an error if that doesn't bring it up.
        if Self::try_start_runtime(runtime).is_ok() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(CliError::new(format!("{} is not running", runtime.label()))
            .with_context(stderr.trim().to_string())
            .with_hint(
                "Start the daemon, then retry. macOS: `open -a Docker`, `colima start`, or \
                 `podman machine start`. Linux/headless (CI, sandboxes): `sudo systemctl start \
                 docker`, or run `sudo dockerd &` where there is no init system. Rootless Podman \
                 needs newuidmap/subuid setup and often fails in restricted containers — install \
                 Docker or use a privileged container there.",
            )
            .into())
    }

    /// Returns `true` if the runtime daemon answers an `info` probe. This is a
    /// quick, non-blocking check — it never tries to auto-start the daemon.
    pub(crate) fn is_running(runtime: ContainerRuntime) -> bool {
        Command::new(runtime.binary())
            .arg("info")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Auto-detect how to start the runtime daemon, launch it, and poll until it's
    /// ready (or we time out). Returns `Err` if there's no known launcher for this
    /// platform, the launch command fails, or the daemon never comes up.
    fn try_start_runtime(runtime: ContainerRuntime) -> Result<()> {
        let Some(start) = runtime_start_command(std::env::consts::OS, runtime, command_exists)
        else {
            return Err(eyre!(
                "no known way to start {} on this platform",
                runtime.label()
            ));
        };

        let mut step = Step::with_messages(
            &format!("Starting {}", runtime.label()),
            &format!("{} started", runtime.label()),
        );
        step.start();

        // Issue the start command. `open -a Docker` returns immediately; `colima start`
        // and `podman machine start` block until the VM is up — either way we poll below.
        let launched = Command::new(start.program)
            .args(&start.args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        match launched {
            Err(e) => {
                step.fail();
                return Err(eyre!("Failed to start {}: {e}", runtime.label()));
            }
            Ok(status) if !status.success() => {
                step.fail();
                return Err(eyre!(
                    "Failed to start {}: exited with {}",
                    runtime.label(),
                    status
                ));
            }
            Ok(_) => {}
        }

        let deadline = Instant::now() + RUNTIME_START_TIMEOUT;
        loop {
            if Self::is_running(runtime) {
                step.done();
                return Ok(());
            }
            if Instant::now() >= deadline {
                step.fail();
                return Err(eyre!(
                    "{} did not become ready within {}s",
                    runtime.label(),
                    RUNTIME_START_TIMEOUT.as_secs()
                ));
            }
            thread::sleep(RUNTIME_POLL_INTERVAL);
        }
    }

    pub fn runtime(&self) -> ContainerRuntime {
        self.runtime
    }

    pub fn container_name(&self, instance_name: &str) -> String {
        format!("helix-{}-{}", self.project_name, instance_name)
    }

    pub fn pull_image(&self, config: &LocalInstanceConfig) -> Result<()> {
        self.pull_image_ref(&config.image_ref())
    }

    fn pull_image_ref(&self, image: &str) -> Result<()> {
        Step::verbose_substep(&format!("Pulling {image}"));
        let output = Command::new(self.runtime.binary())
            .args(["pull", image])
            .output()
            .map_err(|e| eyre!("Failed to pull {image}: {e}"))?;

        if !output.status.success() {
            if self.image_exists(image) {
                Step::verbose_substep(&format!("Using local image {image}"));
                return Ok(());
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to pull {image}:\n{stderr}"));
        }

        Ok(())
    }

    fn image_exists(&self, image: &str) -> bool {
        Command::new(self.runtime.binary())
            .args(["image", "inspect", image])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    pub fn run_detached(&self, instance_name: &str, config: &LocalInstanceConfig) -> Result<()> {
        Self::check_available(self.runtime)?;
        self.pull_image(config)?;

        let name = self.container_name(instance_name);
        let image = config.image_ref();
        let _ = self.remove_container(&name);
        let disk_resources = if config.storage.is_disk() {
            Some(self.start_disk_dependencies(instance_name)?)
        } else {
            let _ = self.remove_disk_resources(instance_name, false);
            None
        };

        let args = helix_run_args(&name, &image, config.port, true, disk_resources.as_ref());
        let output = Command::new(self.runtime.binary())
            .args(&args)
            .output()
            .map_err(|e| eyre!("Failed to start {name}: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to start {name}:\n{stderr}"));
        }

        self.wait_ready(config.port)?;
        Ok(())
    }

    pub async fn run_foreground(
        &self,
        instance_name: &str,
        config: &LocalInstanceConfig,
    ) -> Result<()> {
        Self::check_available(self.runtime)?;
        self.pull_image(config)?;

        let name = self.container_name(instance_name);
        let image = config.image_ref();
        let _ = self.remove_container(&name);
        let disk_resources = if config.storage.is_disk() {
            Some(self.start_disk_dependencies(instance_name)?)
        } else {
            let _ = self.remove_disk_resources(instance_name, false);
            None
        };
        let args = helix_run_args(&name, &image, config.port, false, disk_resources.as_ref());

        let mut child = TokioCommand::new(self.runtime.binary())
            .args(&args)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| eyre!("Failed to run {name}: {e}"))?;

        let mut wait = Box::pin(child.wait());
        tokio::select! {
            status = &mut wait => {
                let status = status?;
                if !status.success() {
                    if config.storage.is_disk() {
                        let _ = self.remove_disk_resources(instance_name, false);
                    }
                    return Err(eyre!("{name} exited with status {status}"));
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                crate::output::info("Stopping foreground local Helix instance");
                let _ = self.remove_container(&name);
                if config.storage.is_disk() {
                    let _ = self.remove_disk_resources(instance_name, false);
                }
                match tokio::time::timeout(Duration::from_secs(10), &mut wait).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return Err(eyre!("Failed to wait for {name} to stop: {e}")),
                    Err(_) => return Err(eyre!("Timed out waiting for {name} to stop")),
                }
            }
        }

        if config.storage.is_disk() {
            let _ = self.remove_disk_resources(instance_name, false);
        }

        Ok(())
    }

    pub fn stop(&self, instance_name: &str) -> Result<bool> {
        let name = self.container_name(instance_name);
        let removed_helix = self.remove_container(&name)?;
        let removed_disk_resources = self.remove_disk_resources(instance_name, false)?;
        Ok(removed_helix || removed_disk_resources)
    }

    pub fn restart(&self, instance_name: &str, config: &LocalInstanceConfig) -> Result<()> {
        if config.storage.is_disk() {
            return self.run_detached(instance_name, config);
        }

        let name = self.container_name(instance_name);
        let output = Command::new(self.runtime.binary())
            .args(["restart", &name])
            .output()
            .map_err(|e| eyre!("Failed to restart {name}: {e}"))?;

        if output.status.success() {
            self.wait_ready(config.port)?;
            return Ok(());
        }

        self.run_detached(instance_name, config)
    }

    pub fn logs(&self, instance_name: &str, follow: bool) -> Result<()> {
        let name = self.container_name(instance_name);
        let mut command = Command::new(self.runtime.binary());
        command.arg("logs");
        if follow {
            command.arg("-f");
        }
        command.arg(&name);
        let status = command
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|e| eyre!("Failed to read logs for {name}: {e}"))?;

        if !status.success() {
            return Err(eyre!(
                "{} logs exited with status {status}",
                self.runtime.binary()
            ));
        }
        Ok(())
    }

    pub fn status(&self, instance_name: &str) -> Result<Option<LocalStatus>> {
        let name = self.container_name(instance_name);
        let output = Command::new(self.runtime.binary())
            .args([
                "ps",
                "-a",
                "--format",
                "{{.Names}}\t{{.Status}}\t{{.Ports}}",
                "--filter",
                &format!("name=^{name}$"),
            ])
            .output()
            .map_err(|e| eyre!("Failed to inspect {name}: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to inspect {name}:\n{stderr}"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let Some(line) = stdout.lines().find(|line| !line.trim().is_empty()) else {
            return Ok(None);
        };
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 3 {
            return Ok(None);
        }

        Ok(Some(LocalStatus {
            instance_name: instance_name.to_string(),
            container_name: parts[0].to_string(),
            status: parts[1].to_string(),
            ports: parts[2].to_string(),
        }))
    }

    pub fn prune_instance(&self, instance_name: &str) -> Result<bool> {
        let name = self.container_name(instance_name);
        let removed_helix = self.remove_container(&name)?;
        let removed_disk_resources = self.remove_disk_resources(instance_name, true)?;
        Ok(removed_helix || removed_disk_resources)
    }

    pub fn run_command(&self, args: &[&str]) -> Result<Output> {
        Command::new(self.runtime.binary())
            .args(args)
            .output()
            .map_err(|e| {
                eyre!(
                    "Failed to run {} {}: {e}",
                    self.runtime.binary(),
                    args.join(" ")
                )
            })
    }

    fn disk_resources(&self, instance_name: &str) -> DiskRuntimeResources {
        let base = self.container_name(instance_name);
        DiskRuntimeResources {
            minio_container: format!("{base}-minio"),
            network: format!("{base}-net"),
            volume: format!("{base}-minio-data"),
        }
    }

    fn start_disk_dependencies(&self, instance_name: &str) -> Result<DiskRuntimeResources> {
        let resources = self.disk_resources(instance_name);
        self.pull_image_ref(MINIO_IMAGE)?;
        self.pull_image_ref(MINIO_MC_IMAGE)?;
        self.ensure_network(&resources.network)?;
        self.ensure_volume(&resources.volume)?;
        let _ = self.remove_container(&resources.minio_container);

        let args = minio_run_args(&resources);
        let output = Command::new(self.runtime.binary())
            .args(&args)
            .output()
            .map_err(|e| eyre!("Failed to start {}: {e}", resources.minio_container))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!(
                "Failed to start {}:\n{stderr}",
                resources.minio_container
            ));
        }

        self.ensure_minio_bucket(&resources)?;
        Ok(resources)
    }

    fn ensure_network(&self, network: &str) -> Result<()> {
        if self.resource_exists(&["network", "inspect", network]) {
            return Ok(());
        }

        let output = Command::new(self.runtime.binary())
            .args(["network", "create", network])
            .output()
            .map_err(|e| eyre!("Failed to create network {network}: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.to_ascii_lowercase().contains("already exists") {
                return Err(eyre!("Failed to create network {network}:\n{stderr}"));
            }
        }

        Ok(())
    }

    fn ensure_volume(&self, volume: &str) -> Result<()> {
        let output = Command::new(self.runtime.binary())
            .args(["volume", "create", volume])
            .output()
            .map_err(|e| eyre!("Failed to create volume {volume}: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to create volume {volume}:\n{stderr}"));
        }

        Ok(())
    }

    fn ensure_minio_bucket(&self, resources: &DiskRuntimeResources) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let args = minio_bucket_init_args(resources);
        let mut last_stderr = String::new();

        while Instant::now() < deadline {
            let output = Command::new(self.runtime.binary())
                .args(&args)
                .output()
                .map_err(|e| eyre!("Failed to initialize local MinIO bucket: {e}"))?;

            if output.status.success() {
                return Ok(());
            }

            last_stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            thread::sleep(Duration::from_millis(500));
        }

        Err(eyre!(
            "Timed out initializing local MinIO bucket {LOCAL_S3_BUCKET}:\n{last_stderr}"
        ))
    }

    fn remove_disk_resources(&self, instance_name: &str, include_volume: bool) -> Result<bool> {
        let resources = self.disk_resources(instance_name);
        let removed_minio = self.remove_container(&resources.minio_container)?;
        let removed_network = self.remove_network(&resources.network)?;
        let removed_volume = if include_volume {
            self.remove_volume(&resources.volume)?
        } else {
            false
        };

        Ok(removed_minio || removed_network || removed_volume)
    }

    fn remove_network(&self, network: &str) -> Result<bool> {
        let output = Command::new(self.runtime.binary())
            .args(["network", "rm", network])
            .output()
            .map_err(|e| eyre!("Failed to remove network {network}: {e}"))?;

        let stderr = String::from_utf8_lossy(&output.stderr);
        if missing_resource(&stderr) {
            return Ok(false);
        }

        if !output.status.success() {
            return Err(eyre!("Failed to remove network {network}:\n{stderr}"));
        }
        Ok(true)
    }

    fn remove_volume(&self, volume: &str) -> Result<bool> {
        let output = Command::new(self.runtime.binary())
            .args(["volume", "rm", volume])
            .output()
            .map_err(|e| eyre!("Failed to remove volume {volume}: {e}"))?;

        let stderr = String::from_utf8_lossy(&output.stderr);
        if missing_resource(&stderr) {
            return Ok(false);
        }

        if !output.status.success() {
            return Err(eyre!("Failed to remove volume {volume}:\n{stderr}"));
        }
        Ok(true)
    }

    fn resource_exists(&self, args: &[&str]) -> bool {
        Command::new(self.runtime.binary())
            .args(args)
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn remove_container(&self, name: &str) -> Result<bool> {
        let output = Command::new(self.runtime.binary())
            .args(["rm", "-f", name])
            .output()
            .map_err(|e| eyre!("Failed to remove {name}: {e}"))?;

        let stderr = String::from_utf8_lossy(&output.stderr);
        if missing_resource(&stderr) {
            return Ok(false);
        }

        if !output.status.success() {
            return Err(eyre!("Failed to remove {name}:\n{stderr}"));
        }
        Ok(true)
    }

    fn wait_ready(&self, port: u16) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if self.query_endpoint_ready(port) {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(250));
        }

        Err(CliError::new("local Helix did not become ready in time")
            .with_hint(format!(
                "check logs with 'helix logs' or verify port {port} is reachable"
            ))
            .into())
    }

    fn query_endpoint_ready(&self, port: u16) -> bool {
        let Ok(mut stream) = TcpStream::connect_timeout(
            &(std::net::Ipv4Addr::LOCALHOST, port).into(),
            Duration::from_millis(500),
        ) else {
            return false;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_millis(750)));
        let _ = stream.set_write_timeout(Some(Duration::from_millis(750)));

        let body = r#"{"request_type":"read","query":{"queries":[{"Query":{"name":"readiness","steps":[{"NWhere":{"Eq":["$label",{"String":"__HelixReadiness__"}]}},"Count"],"condition":null}}],"returns":["readiness"]},"parameters":{}}"#;
        let request = format!(
            "POST /v1/query HTTP/1.1\r\nHost: localhost:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );

        if stream.write_all(request.as_bytes()).is_err() {
            return false;
        }

        let mut response = String::new();
        if stream.read_to_string(&mut response).is_err() {
            return false;
        }

        response.starts_with("HTTP/1.1 2") || response.starts_with("HTTP/1.0 2")
    }
}

/// Build the error for a container runtime whose binary is missing from PATH.
///
/// Pure helper — the installed-probe is injected so it can be unit-tested. If
/// the *other* supported runtime is installed, the hint points at switching
/// `container_runtime` in helix.toml (the cheapest recovery); otherwise it
/// gives per-platform install commands.
fn not_installed_error(
    runtime: ContainerRuntime,
    cause: &str,
    is_installed: impl Fn(&str) -> bool,
) -> CliError {
    let other = match runtime {
        ContainerRuntime::Docker => ContainerRuntime::Podman,
        ContainerRuntime::Podman => ContainerRuntime::Docker,
    };
    let hint = if is_installed(other.binary()) {
        format!(
            "{} is installed — set `container_runtime = \"{}\"` under [project] in helix.toml \
             to use it instead, or install {}.",
            other.label(),
            other.binary(),
            runtime.label()
        )
    } else {
        "Install a container runtime first. macOS: `brew install --cask docker` (Docker \
         Desktop) or `brew install colima docker && colima start`. Linux: `curl -fsSL \
         https://get.docker.com | sh`, or `apt-get install -y podman` plus \
         `container_runtime = \"podman\"` in helix.toml. Restricted sandboxes/CI without \
         root usually cannot run containers — run Helix on a host where Docker works, or \
         point `helix query --host/--port` at a reachable instance."
            .to_string()
    };
    CliError::new(format!(
        "{} is not installed (`{}` not found on PATH)",
        runtime.label(),
        runtime.binary()
    ))
    .with_context(cause.to_string())
    .with_hint(hint)
}

/// A command that starts a container runtime daemon, e.g. `open -a Docker`.
struct StartCommand {
    program: &'static str,
    args: Vec<&'static str>,
}

/// Resolve the command to start the given runtime's daemon for the current OS.
///
/// Pure helper — the OS string and an installed-probe are injected so it can be
/// unit-tested deterministically. Returns `None` when there's no known launcher
/// (e.g. Podman on Linux is daemonless, or an unsupported OS).
fn runtime_start_command(
    os: &str,
    runtime: ContainerRuntime,
    is_installed: impl Fn(&str) -> bool,
) -> Option<StartCommand> {
    match (os, runtime) {
        // macOS Docker: prefer Colima if it's installed, otherwise Docker Desktop.
        ("macos", ContainerRuntime::Docker) => {
            if is_installed("colima") {
                Some(StartCommand {
                    program: "colima",
                    args: vec!["start"],
                })
            } else {
                Some(StartCommand {
                    program: "open",
                    args: vec!["-a", "Docker"],
                })
            }
        }
        ("macos", ContainerRuntime::Podman) => Some(StartCommand {
            program: "podman",
            args: vec!["machine", "start"],
        }),
        // Linux Docker: best-effort via systemd (may need privileges; if it fails we
        // fall back to the manual-hint error).
        ("linux", ContainerRuntime::Docker) => Some(StartCommand {
            program: "systemctl",
            args: vec!["start", "docker"],
        }),
        // Podman on Linux is daemonless; nothing to start. Other OSes: unknown launcher.
        _ => None,
    }
}

fn helix_run_args(
    name: &str,
    image: &str,
    port: u16,
    detached: bool,
    disk_resources: Option<&DiskRuntimeResources>,
) -> Vec<String> {
    let mut args = vec!["run".to_string()];
    if detached {
        args.extend([
            "-d".to_string(),
            "--restart".to_string(),
            "unless-stopped".to_string(),
        ]);
    } else {
        args.push("--rm".to_string());
    }

    args.extend([
        "--name".to_string(),
        name.to_string(),
        "-p".to_string(),
        format!("{port}:{CONTAINER_PORT}"),
    ]);

    if let Some(resources) = disk_resources {
        args.extend(["--network".to_string(), resources.network.clone()]);
        for (key, value) in disk_env(resources) {
            args.extend(["-e".to_string(), format!("{key}={value}")]);
        }
    }

    args.push(image.to_string());
    args
}

fn minio_run_args(resources: &DiskRuntimeResources) -> Vec<String> {
    vec![
        "run".to_string(),
        "-d".to_string(),
        "--restart".to_string(),
        "unless-stopped".to_string(),
        "--name".to_string(),
        resources.minio_container.clone(),
        "--network".to_string(),
        resources.network.clone(),
        "-e".to_string(),
        format!("MINIO_ROOT_USER={MINIO_ACCESS_KEY}"),
        "-e".to_string(),
        format!("MINIO_ROOT_PASSWORD={MINIO_SECRET_KEY}"),
        "-v".to_string(),
        format!("{}:/data", resources.volume),
        MINIO_IMAGE.to_string(),
        "server".to_string(),
        "/data".to_string(),
        "--console-address".to_string(),
        ":9001".to_string(),
    ]
}

fn minio_bucket_init_args(resources: &DiskRuntimeResources) -> Vec<String> {
    let endpoint = format!("http://{}:9000", resources.minio_container);
    let command = format!(
        "mc alias set local {} {} {} && mc mb --ignore-existing local/{}",
        shell_quote(&endpoint),
        shell_quote(MINIO_ACCESS_KEY),
        shell_quote(MINIO_SECRET_KEY),
        LOCAL_S3_BUCKET
    );

    vec![
        "run".to_string(),
        "--rm".to_string(),
        "--network".to_string(),
        resources.network.clone(),
        "--entrypoint".to_string(),
        "/bin/sh".to_string(),
        MINIO_MC_IMAGE.to_string(),
        "-c".to_string(),
        command,
    ]
}

fn disk_env(resources: &DiskRuntimeResources) -> Vec<(&'static str, String)> {
    vec![
        ("S3_BUCKET", LOCAL_S3_BUCKET.to_string()),
        ("S3_REGION", LOCAL_S3_REGION.to_string()),
        ("DB_PATH", LOCAL_DB_PATH.to_string()),
        ("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY.to_string()),
        ("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY.to_string()),
        (
            "AWS_ENDPOINT",
            format!("http://{}:9000", resources.minio_container),
        ),
        ("AWS_ALLOW_HTTP", "true".to_string()),
    ]
}

fn missing_resource(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    stderr.contains("no such") || stderr.contains("not found") || stderr.contains("does not exist")
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk_resources() -> DiskRuntimeResources {
        DiskRuntimeResources {
            minio_container: "helix-demo-dev-minio".to_string(),
            network: "helix-demo-dev-net".to_string(),
            volume: "helix-demo-dev-minio-data".to_string(),
        }
    }

    fn has_pair(args: &[String], key: &str, value: &str) -> bool {
        args.windows(2)
            .any(|window| window[0] == key && window[1] == value)
    }

    #[test]
    fn memory_helix_args_match_existing_run_shape() {
        let args = helix_run_args(
            "helix-demo-dev",
            "ghcr.io/helixdb/enterprise-dev:latest",
            9090,
            true,
            None,
        );

        assert_eq!(
            args,
            vec![
                "run",
                "-d",
                "--restart",
                "unless-stopped",
                "--name",
                "helix-demo-dev",
                "-p",
                "9090:8080",
                "ghcr.io/helixdb/enterprise-dev:latest",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn disk_helix_args_include_network_and_s3_env() {
        let resources = disk_resources();
        let args = helix_run_args(
            "helix-demo-dev",
            "ghcr.io/helixdb/enterprise-dev:latest",
            8080,
            true,
            Some(&resources),
        );

        assert!(has_pair(&args, "--network", "helix-demo-dev-net"));
        assert!(args.contains(&"S3_BUCKET=helix-db".to_string()));
        assert!(args.contains(&"S3_REGION=us-east-1".to_string()));
        assert!(args.contains(&"DB_PATH=db/".to_string()));
        assert!(args.contains(&"AWS_ACCESS_KEY_ID=minioadmin".to_string()));
        assert!(args.contains(&"AWS_SECRET_ACCESS_KEY=minioadmin".to_string()));
        assert!(args.contains(&"AWS_ENDPOINT=http://helix-demo-dev-minio:9000".to_string()));
        assert!(args.contains(&"AWS_ALLOW_HTTP=true".to_string()));
    }

    #[test]
    fn minio_args_include_persistent_volume() {
        let resources = disk_resources();
        let args = minio_run_args(&resources);

        assert!(has_pair(&args, "--network", "helix-demo-dev-net"));
        assert!(args.contains(&"MINIO_ROOT_USER=minioadmin".to_string()));
        assert!(args.contains(&"MINIO_ROOT_PASSWORD=minioadmin".to_string()));
        assert!(args.contains(&"helix-demo-dev-minio-data:/data".to_string()));
    }

    #[test]
    fn minio_bucket_init_uses_shell_entrypoint() {
        let resources = disk_resources();
        let args = minio_bucket_init_args(&resources);

        assert!(has_pair(&args, "--entrypoint", "/bin/sh"));
        assert!(args.contains(&"minio/mc:latest".to_string()));
        assert!(args.iter().any(|arg| arg.contains("mc alias set local")));
    }

    fn start_cmd(
        os: &str,
        runtime: ContainerRuntime,
        colima: bool,
    ) -> Option<(String, Vec<String>)> {
        runtime_start_command(os, runtime, |bin| colima && bin == "colima").map(|c| {
            (
                c.program.to_string(),
                c.args.iter().map(|a| a.to_string()).collect(),
            )
        })
    }

    #[test]
    fn macos_docker_prefers_colima_when_installed() {
        assert_eq!(
            start_cmd("macos", ContainerRuntime::Docker, true),
            Some(("colima".to_string(), vec!["start".to_string()]))
        );
    }

    #[test]
    fn macos_docker_falls_back_to_docker_desktop() {
        assert_eq!(
            start_cmd("macos", ContainerRuntime::Docker, false),
            Some((
                "open".to_string(),
                vec!["-a".to_string(), "Docker".to_string()]
            ))
        );
    }

    #[test]
    fn macos_podman_starts_machine() {
        assert_eq!(
            start_cmd("macos", ContainerRuntime::Podman, false),
            Some((
                "podman".to_string(),
                vec!["machine".to_string(), "start".to_string()]
            ))
        );
    }

    #[test]
    fn linux_docker_uses_systemctl() {
        assert_eq!(
            start_cmd("linux", ContainerRuntime::Docker, false),
            Some((
                "systemctl".to_string(),
                vec!["start".to_string(), "docker".to_string()]
            ))
        );
    }

    #[test]
    fn no_launcher_for_linux_podman_or_unknown_os() {
        assert_eq!(start_cmd("linux", ContainerRuntime::Podman, false), None);
        assert_eq!(start_cmd("windows", ContainerRuntime::Docker, false), None);
    }

    #[test]
    fn not_installed_error_suggests_other_runtime_when_present() {
        let err = not_installed_error(ContainerRuntime::Docker, "spawn failed", |bin| {
            bin == "podman"
        });

        assert!(err.message.contains("Docker is not installed"));
        let hint = err.hint.expect("hint should be set");
        assert!(hint.contains("container_runtime = \"podman\""));
    }

    #[test]
    fn not_installed_error_gives_install_commands_when_nothing_present() {
        let err = not_installed_error(ContainerRuntime::Podman, "spawn failed", |_| false);

        assert!(err.message.contains("Podman is not installed"));
        let hint = err.hint.expect("hint should be set");
        assert!(hint.contains("get.docker.com"));
        assert!(hint.contains("sandboxes"));
    }
}
