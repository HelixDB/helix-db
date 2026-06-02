use crate::config::{ContainerRuntime, LocalInstanceConfig};
use crate::errors::CliError;
use crate::output::Step;
use crate::project::ProjectContext;
use crate::utils::command_exists;
use eyre::{Result, eyre};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::{fs, thread};
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
pub struct LocalStatus {
    pub instance_name: String,
    pub container_name: String,
    pub status: String,
    pub ports: String,
}

// ---------------------------------------------------------------------------
// Enum-based runtime dispatch
// ---------------------------------------------------------------------------

pub enum Runtime<'a> {
    Container(LocalRuntime),
    Native(NativeManager<'a>),
}

impl<'a> Runtime<'a> {
    pub fn for_project(project: &'a ProjectContext) -> Self {
        if project.config.project.container_runtime.is_native() {
            Self::Native(NativeManager::new(project))
        } else {
            Self::Container(LocalRuntime::new(project))
        }
    }

    pub async fn start_foreground(
        &self,
        instance_name: &str,
        config: &LocalInstanceConfig,
    ) -> Result<()> {
        match self {
            Self::Container(r) => r.start_foreground(instance_name, config).await,
            Self::Native(r) => r.start_foreground(instance_name, config).await,
        }
    }

    pub fn start(&self, instance_name: &str, config: &LocalInstanceConfig) -> Result<()> {
        match self {
            Self::Container(r) => r.start(instance_name, config),
            Self::Native(r) => r.start(instance_name, config),
        }
    }

    pub fn stop(&self, instance_name: &str) -> Result<bool> {
        match self {
            Self::Container(r) => r.stop(instance_name),
            Self::Native(r) => r.stop(instance_name),
        }
    }

    pub fn restart(&self, instance_name: &str, config: &LocalInstanceConfig) -> Result<()> {
        match self {
            Self::Container(r) => r.restart(instance_name, config),
            Self::Native(r) => r.restart(instance_name, config),
        }
    }

    pub fn logs(&self, instance_name: &str, follow: bool) -> Result<()> {
        match self {
            Self::Container(r) => r.logs(instance_name, follow),
            Self::Native(r) => r.logs(instance_name, follow),
        }
    }

    pub fn status(&self, instance_name: &str) -> Result<Option<LocalStatus>> {
        match self {
            Self::Container(r) => r.status(instance_name),
            Self::Native(r) => r.status(instance_name),
        }
    }

    pub fn prune(&self, instance_name: &str) -> Result<bool> {
        match self {
            Self::Container(r) => r.prune(instance_name),
            Self::Native(r) => r.prune(instance_name),
        }
    }

    pub fn display_name(&self, instance_name: &str) -> String {
        match self {
            Self::Container(r) => r.display_name(instance_name),
            Self::Native(r) => r.display_name(instance_name),
        }
    }
}

// ---------------------------------------------------------------------------
// Container-based runtime (Docker / Podman)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LocalRuntime {
    runtime: ContainerRuntime,
    project_name: String,
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
                return Err(eyre!(
                    "{} is not available. Install/start {} and try again: {e}",
                    runtime.label(),
                    runtime.binary()
                ));
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
                "Start it manually, then retry — e.g. `open -a Docker`, `colima start`, \
                 or `podman machine start`.",
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

    fn pull_image(&self, config: &LocalInstanceConfig) -> Result<()> {
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
        wait_ready(port)
    }

    fn start(&self, instance_name: &str, config: &LocalInstanceConfig) -> Result<()> {
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

    async fn start_foreground(
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

    fn stop(&self, instance_name: &str) -> Result<bool> {
        let name = self.container_name(instance_name);
        let removed_helix = self.remove_container(&name)?;
        let removed_disk_resources = self.remove_disk_resources(instance_name, false)?;
        Ok(removed_helix || removed_disk_resources)
    }

    fn restart(&self, instance_name: &str, config: &LocalInstanceConfig) -> Result<()> {
        if config.storage.is_disk() {
            return self.start(instance_name, config);
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

        self.start(instance_name, config)
    }

    fn logs(&self, instance_name: &str, follow: bool) -> Result<()> {
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

    fn status(&self, instance_name: &str) -> Result<Option<LocalStatus>> {
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

    fn prune(&self, instance_name: &str) -> Result<bool> {
        let name = self.container_name(instance_name);
        let removed_helix = self.remove_container(&name)?;
        let removed_disk_resources = self.remove_disk_resources(instance_name, true)?;
        Ok(removed_helix || removed_disk_resources)
    }

    fn display_name(&self, instance_name: &str) -> String {
        self.container_name(instance_name)
    }
}

// ---------------------------------------------------------------------------
// Native process-based runtime
// ---------------------------------------------------------------------------

pub struct NativeManager<'a> {
    project: &'a ProjectContext,
}

impl<'a> NativeManager<'a> {
    pub fn new(project: &'a ProjectContext) -> Self {
        Self { project }
    }

    fn pid_file(&self, instance_name: &str) -> PathBuf {
        self.project
            .instance_workspace(instance_name)
            .join("helix.pid")
    }

    fn log_file(&self, instance_name: &str) -> PathBuf {
        self.project
            .instance_workspace(instance_name)
            .join("helix.log")
    }

    fn binary_path(&self, instance_name: &str) -> PathBuf {
        self.project
            .instance_workspace(instance_name)
            .join("helix-container")
            .join("target")
            .join("release")
            .join("helix-container")
    }

    fn save_pid(&self, instance_name: &str, pid: u32) -> Result<()> {
        let pid_file = self.pid_file(instance_name);
        if let Some(parent) = pid_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let start_time = Self::process_start_time(pid).unwrap_or(0);
        fs::write(&pid_file, format!("{pid}\n{start_time}\n"))
            .map_err(|e| eyre!("Failed to write PID file: {e}"))
    }

    fn read_pid(&self, instance_name: &str) -> Result<Option<(u32, u64)>> {
        let pid_file = self.pid_file(instance_name);
        if !pid_file.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&pid_file)
            .map_err(|e| eyre!("Failed to read PID file: {e}"))?;
        let mut lines = content.lines();
        let pid = lines
            .next()
            .unwrap_or("")
            .trim()
            .parse::<u32>()
            .map_err(|e| eyre!("Invalid PID in file: {e}"))?;
        let start_time = lines
            .next()
            .unwrap_or("0")
            .trim()
            .parse::<u64>()
            .unwrap_or(0);
        Ok(Some((pid, start_time)))
    }

    fn remove_pid_file(&self, instance_name: &str) {
        let _ = fs::remove_file(self.pid_file(instance_name));
    }

    fn remove_log_file(&self, instance_name: &str) {
        let _ = fs::remove_file(self.log_file(instance_name));
    }

    fn is_process_running(pid: u32) -> bool {
        #[cfg(target_os = "linux")]
        {
            PathBuf::from(format!("/proc/{pid}")).exists()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Command::new("kill")
                .args(["-0", &pid.to_string()])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        }
    }

    /// Returns the process start time as jiffies since boot (Linux) or 0 on other platforms.
    fn process_start_time(pid: u32) -> Option<u64> {
        #[cfg(target_os = "linux")]
        {
            let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
            // Field 22 (0-indexed: 21) is starttime in jiffies since boot
            let fields: Vec<&str> = stat.splitn(22, ' ').collect();
            let start_str = fields.get(21)?;
            start_str.trim().parse::<u64>().ok()
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            None
        }
    }

    /// Verify that the PID still refers to the same process we started.
    fn verify_pid(&self, _instance_name: &str, pid: u32, saved_start_time: u64) -> bool {
        if !Self::is_process_running(pid) {
            return false;
        }
        if saved_start_time == 0 {
            // Can't verify on this platform; assume it's the same process
            return true;
        }
        Self::process_start_time(pid) == Some(saved_start_time)
    }

    fn stop_process(&self, instance_name: &str) -> Result<bool> {
        let Some((pid, start_time)) = self.read_pid(instance_name)? else {
            return Ok(false);
        };

        if !self.verify_pid(instance_name, pid, start_time) {
            self.remove_pid_file(instance_name);
            return Ok(false);
        }

        let output = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .output()
            .map_err(|e| eyre!("Failed to send SIGTERM to process {pid}: {e}"))?;

        if !output.status.success() {
            self.remove_pid_file(instance_name);
            return Ok(false);
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if !Self::is_process_running(pid) {
                self.remove_pid_file(instance_name);
                return Ok(true);
            }
            thread::sleep(Duration::from_millis(200));
        }

        let _ = Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .output();
        self.remove_pid_file(instance_name);
        Ok(true)
    }

    fn start(&self, instance_name: &str, config: &LocalInstanceConfig) -> Result<()> {
        let binary = self.binary_path(instance_name);
        if !binary.exists() {
            return Err(CliError::new(format!(
                "native binary not found at {}",
                binary.display()
            ))
            .with_hint(format!(
                "run 'helix build {instance_name}' first to compile the instance"
            ))
            .into());
        }

        let data_dir = self.project.instance_volume(instance_name);
        fs::create_dir_all(&data_dir)?;
        let port = config.port;

        let log_file = self.log_file(instance_name);
        if let Some(parent) = log_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let log = fs::File::create(&log_file)
            .map_err(|e| eyre!("Failed to create log file: {e}"))?;

        let mut cmd = Command::new(&binary);
        cmd.env("HELIX_PORT", port.to_string())
            .env("HELIX_DATA_DIR", &data_dir)
            .current_dir(&data_dir)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));

        if config.storage.is_disk() {
            cmd.env("S3_BUCKET", LOCAL_S3_BUCKET)
                .env("S3_REGION", LOCAL_S3_REGION)
                .env("DB_PATH", LOCAL_DB_PATH);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| eyre!("Failed to start native process: {e}"))?;

        if let Err(e) = self.save_pid(instance_name, child.id()) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(e);
        }

        wait_ready(port)?;
        Ok(())
    }

    async fn start_foreground(
        &self,
        instance_name: &str,
        config: &LocalInstanceConfig,
    ) -> Result<()> {
        let binary = self.binary_path(instance_name);
        if !binary.exists() {
            return Err(CliError::new(format!(
                "native binary not found at {}",
                binary.display()
            ))
            .with_hint(format!(
                "run 'helix build {instance_name}' first to compile the instance"
            ))
            .into());
        }

        let data_dir = self.project.instance_volume(instance_name);
        fs::create_dir_all(&data_dir)?;
        let port = config.port;

        let mut cmd = TokioCommand::new(&binary);
        cmd.env("HELIX_PORT", port.to_string())
            .env("HELIX_DATA_DIR", &data_dir)
            .current_dir(&data_dir)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        if config.storage.is_disk() {
            cmd.env("S3_BUCKET", LOCAL_S3_BUCKET)
                .env("S3_REGION", LOCAL_S3_REGION)
                .env("DB_PATH", LOCAL_DB_PATH);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| eyre!("Failed to start native process: {e}"))?;

        let pid = child.id();
        tokio::select! {
            status = child.wait() => {
                let status = status?;
                if !status.success() {
                    return Err(eyre!("Native process exited with status {status}"));
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                crate::output::info("Stopping foreground local Helix instance");
                match tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return Err(eyre!("Failed to wait for process to stop: {e}")),
                    Err(_) => {
                        // Timeout expired — force-kill by PID
                        if let Some(pid) = pid {
                            let _ = Command::new("kill")
                                .args(["-KILL", &pid.to_string()])
                                .output();
                        }
                        return Err(eyre!("Timed out waiting for process to stop"));
                    }
                }
            }
        }

        Ok(())
    }

    fn stop(&self, instance_name: &str) -> Result<bool> {
        self.stop_process(instance_name)
    }

    fn restart(&self, instance_name: &str, config: &LocalInstanceConfig) -> Result<()> {
        let _ = self.stop_process(instance_name);
        self.start(instance_name, config)
    }

    fn logs(&self, instance_name: &str, follow: bool) -> Result<()> {
        let log_file = self.log_file(instance_name);
        if !log_file.exists() {
            return Err(eyre!("No log file found for instance '{instance_name}'"));
        }

        if follow {
            let status = Command::new("tail")
                .args(["-f", &log_file.to_string_lossy()])
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .map_err(|e| eyre!("Failed to follow logs: {e}"))?;

            if !status.success() {
                return Err(eyre!("tail exited with status {status}"));
            }
        } else {
            let content = fs::read_to_string(&log_file)
                .map_err(|e| eyre!("Failed to read log file: {e}"))?;
            print!("{content}");
        }
        Ok(())
    }

    fn status(&self, instance_name: &str) -> Result<Option<LocalStatus>> {
        let Some((pid, start_time)) = self.read_pid(instance_name)? else {
            return Ok(None);
        };

        if self.verify_pid(instance_name, pid, start_time) {
            let port = self
                .project
                .config
                .local
                .get(instance_name)
                .map(|c| c.port)
                .unwrap_or(0);
            Ok(Some(LocalStatus {
                instance_name: instance_name.to_string(),
                container_name: format!("native-{pid}"),
                status: "Up (native)".to_string(),
                ports: format!("0.0.0.0:{port}->8080/tcp"),
            }))
        } else {
            self.remove_pid_file(instance_name);
            Ok(None)
        }
    }

    fn prune(&self, instance_name: &str) -> Result<bool> {
        let removed = self.stop_process(instance_name)?;
        self.remove_pid_file(instance_name);
        self.remove_log_file(instance_name);
        Ok(removed)
    }

    fn display_name(&self, instance_name: &str) -> String {
        match self.read_pid(instance_name) {
            Ok(Some((pid, _))) => format!("native process {pid}"),
            _ => format!("native:{instance_name}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn wait_ready(port: u16) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if query_endpoint_ready(port) {
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

fn query_endpoint_ready(port: u16) -> bool {
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
}
