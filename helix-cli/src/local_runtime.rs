use crate::config::{ContainerRuntime, LocalInstanceConfig};
use crate::errors::CliError;
use crate::output::Step;
use crate::project::ProjectContext;
use eyre::{Result, eyre};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::{fs, thread};
use std::time::{Duration, Instant};
use tokio::process::Command as TokioCommand;

pub const CONTAINER_PORT: u16 = 8080;
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
        let output = Command::new(runtime.binary())
            .arg("info")
            .output()
            .map_err(|e| {
                eyre!(
                    "{} is not available. Install/start {} and try again: {e}",
                    runtime.label(),
                    runtime.binary()
                )
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("{} is not running:\n{}", runtime.label(), stderr));
        }

        Ok(())
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
        fs::write(&pid_file, pid.to_string())
            .map_err(|e| eyre!("Failed to write PID file: {e}"))
    }

    fn read_pid(&self, instance_name: &str) -> Result<Option<u32>> {
        let pid_file = self.pid_file(instance_name);
        if !pid_file.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&pid_file)
            .map_err(|e| eyre!("Failed to read PID file: {e}"))?;
        let pid = content
            .trim()
            .parse::<u32>()
            .map_err(|e| eyre!("Invalid PID in file: {e}"))?;
        Ok(Some(pid))
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

    fn stop_process(&self, instance_name: &str) -> Result<bool> {
        let Some(pid) = self.read_pid(instance_name)? else {
            return Ok(false);
        };

        if !Self::is_process_running(pid) {
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

        let child = cmd
            .spawn()
            .map_err(|e| eyre!("Failed to start native process: {e}"))?;

        self.save_pid(instance_name, child.id())?;

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

        let mut wait = Box::pin(child.wait());
        tokio::select! {
            status = &mut wait => {
                let status = status?;
                if !status.success() {
                    return Err(eyre!("Native process exited with status {status}"));
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                crate::output::info("Stopping foreground local Helix instance");
                match tokio::time::timeout(Duration::from_secs(10), &mut wait).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return Err(eyre!("Failed to wait for process to stop: {e}")),
                    Err(_) => return Err(eyre!("Timed out waiting for process to stop")),
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
        let Some(pid) = self.read_pid(instance_name)? else {
            return Ok(None);
        };

        if Self::is_process_running(pid) {
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
            Ok(Some(pid)) => format!("native process {pid}"),
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
}
