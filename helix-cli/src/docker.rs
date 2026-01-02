//! Container management using Docker-compatible runtimes (Docker/Podman), here Docker is more in a semantic sense.
//!
//! Despite the module name, this works with both Docker and Podman as they
//! share the same CLI interface and support standard Dockerfile formats.

use crate::config::{BuildMode, ContainerRuntime, InstanceInfo};
use crate::project::ProjectContext;
use crate::utils::{print_confirm, print_info, print_status, print_warning};
use eyre::{Result, eyre};
use std::fmt;
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

/// Error type for Docker build failures that may be Rust compilation errors.
#[derive(Debug)]
pub enum DockerBuildError {
    /// Rust compilation failed during Docker build
    RustCompilation {
        output: String,
        instance_name: String,
    },
}

impl fmt::Display for DockerBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DockerBuildError::RustCompilation { output, instance_name } => {
                write!(f, "Rust compilation failed for instance '{}': {}", instance_name, output)
            }
        }
    }
}

impl std::error::Error for DockerBuildError {}

/// Check if Docker build output indicates a Rust compilation error.
fn is_rust_compilation_error(output: &str) -> bool {
    output.contains("error[E")
        || output.contains("error: could not compile")
        || (output.contains("cargo build") && output.contains("error:"))
}

pub struct DockerManager<'a> {
    project: &'a ProjectContext,
    /// The container runtime to use (Docker or Podman)
    pub(crate) runtime: ContainerRuntime,
}

pub const HELIX_DATA_DIR: &str = "/data";

impl<'a> DockerManager<'a> {
    pub fn new(project: &'a ProjectContext) -> Self {
        Self {
            project,
            runtime: project.config.project.container_runtime,
        }
    }

    // === CENTRALIZED NAMING METHODS ===

    /// Get the compose project name for an instance
    fn compose_project_name(&self, instance_name: &str) -> String {
        format!(
            // has to be `-` instead of `_` because fly doesnt allow underscores in instance names
            // abd image name must match the instance name
            "helix-{}-{}",
            self.project.config.project.name, instance_name
        )
    }

    /// Get the service name (always "app")
    fn service_name() -> &'static str {
        "app"
    }

    /// Get the image name for an instance
    pub(crate) fn image_name(&self, instance_name: &str, build_mode: BuildMode) -> String {
        let tag = match build_mode {
            BuildMode::Debug => "debug",
            BuildMode::Release => "latest",
            BuildMode::Dev => "dev",
        };
        let project_name = self.compose_project_name(instance_name);
        format!("{project_name}:{tag}")
    }

    /// Get environment variables for an instance
    pub(crate) fn environment_variables(&self, instance_name: &str) -> Vec<String> {
        // Load .env from project root first (base configuration)
        let root_env = self.project.root.join(".env");
        if root_env.exists() {
            let _ = dotenvy::from_path(&root_env);
            print_info(&format!(
                "Loading environment from {}",
                root_env.display()
            ));
        }

        // Load .env from db/queries directory (overrides project root)
        let queries_dir = self.project.root.join(&self.project.config.project.queries);
        let db_env = queries_dir.join(".env");
        if db_env.exists() {
            let _ = dotenvy::from_path_override(&db_env);
            print_info(&format!(
                "Overriding environment from {}",
                db_env.display()
            ));
        }

        let mut env_vars = vec![
            {
                let port = self
                    .project
                    .config
                    .get_instance(instance_name)
                    .unwrap()
                    .port()
                    .unwrap_or(6969);
                format!("HELIX_PORT={port}")
            },
            format!("HELIX_DATA_DIR={HELIX_DATA_DIR}"),
            format!("HELIX_INSTANCE={instance_name}"),
            {
                let project_name = &self.project.config.project.name;
                format!("HELIX_PROJECT={project_name}")
            },
        ];

        // Add API keys from environment (which includes .env after dotenv() call)
        if let Ok(openai_key) = std::env::var("OPENAI_API_KEY") {
            env_vars.push(format!("OPENAI_API_KEY={openai_key}"));
        }
        if let Ok(gemini_key) = std::env::var("GEMINI_API_KEY") {
            env_vars.push(format!("GEMINI_API_KEY={gemini_key}"));
        }

        env_vars
    }

    /// Get the container name for an instance
    fn container_name(&self, instance_name: &str) -> String {
        let project_name = self.compose_project_name(instance_name);
        format!("{project_name}_app")
    }

    /// Get the network name for an instance
    fn network_name(&self, instance_name: &str) -> String {
        let project_name = self.compose_project_name(instance_name);
        format!("{project_name}_net")
    }

    /// Format a Docker Compose volume spec, keeping Windows absolute paths safe.
    fn compose_volume_spec(&self, volume_source: &str) -> String {
        if cfg!(windows) && volume_source.contains(':') {
            let normalized = volume_source.replace('\\', "/");
            format!("\"{}:{}\"", normalized, HELIX_DATA_DIR)
        } else {
            format!("{}:{}", volume_source, HELIX_DATA_DIR)
        }
    }

    // === CENTRALIZED DOCKER/PODMAN COMMAND EXECUTION ===

    /// Run a docker/podman command with consistent error handling
    pub fn run_docker_command(&self, args: &[&str]) -> Result<Output> {
        let output = Command::new(self.runtime.binary())
            .args(args)
            .output()
            .map_err(|e| {
                eyre!(
                    "Failed to run {} {}: {e}",
                    self.runtime.binary(),
                    args.join(" ")
                )
            })?;
        Ok(output)
    }

    /// Run a docker/podman compose command with proper project naming
    fn run_compose_command(&self, instance_name: &str, args: Vec<&str>) -> Result<Output> {
        let workspace = self.project.instance_workspace(instance_name);
        let project_name = self.compose_project_name(instance_name);

        let mut full_args = vec!["--project-name", &project_name];
        full_args.extend(args);

        let output = Command::new(self.runtime.binary())
            .arg("compose")
            .args(&full_args)
            .current_dir(&workspace)
            .output()
            .map_err(|e| {
                eyre!(
                    "Failed to run {} compose {}: {e}",
                    self.runtime.binary(),
                    full_args.join(" ")
                )
            })?;
        Ok(output)
    }

    /// Detect the current operating system platform
    fn detect_platform() -> &'static str {
        #[cfg(target_os = "macos")]
        return "macos";

        #[cfg(target_os = "linux")]
        return "linux";

        #[cfg(target_os = "windows")]
        return "windows";

        #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
        return "unknown";
    }

    /// Start the container runtime daemon based on platform and runtime
    fn start_runtime_daemon(runtime: ContainerRuntime) -> Result<()> {
        let platform = Self::detect_platform();

        match (runtime, platform) {
            // Docker on macOS
            (ContainerRuntime::Docker, "macos") => {
                print_status("DOCKER", "Starting Docker Desktop for macOS...");
                Command::new("open")
                    .args(["-a", "Docker"])
                    .output()
                    .map_err(|e| eyre!("Failed to start Docker Desktop: {}", e))?;
            }

            // Podman on macOS
            (ContainerRuntime::Podman, "macos") => {
                print_status("PODMAN", "Starting Podman machine on macOS...");

                // Check if machine exists first
                let list_output = Command::new("podman")
                    .args(["machine", "list", "--format", "{{.Name}}"])
                    .output()
                    .map_err(|e| eyre!("Failed to list Podman machines: {}", e))?;

                let machines = String::from_utf8_lossy(&list_output.stdout);

                if machines.trim().is_empty() {
                    // No machine exists, initialize one
                    print_status("PODMAN", "Initializing Podman machine (first time)...");
                    let init_output = Command::new("podman")
                        .args(["machine", "init"])
                        .output()
                        .map_err(|e| eyre!("Failed to initialize Podman machine: {}", e))?;

                    if !init_output.status.success() {
                        let stderr = String::from_utf8_lossy(&init_output.stderr);
                        return Err(eyre!("Failed to initialize Podman machine: {}", stderr));
                    }
                }

                // Start the machine
                Command::new("podman")
                    .args(["machine", "start"])
                    .output()
                    .map_err(|e| eyre!("Failed to start Podman machine: {}", e))?;
            }

            // Docker on Linux
            (ContainerRuntime::Docker, "linux") => {
                print_status("DOCKER", "Attempting to start Docker daemon on Linux...");
                let systemctl_result = Command::new("systemctl").args(["start", "docker"]).output();

                match systemctl_result {
                    Ok(output) if output.status.success() => {}
                    _ => {
                        let service_result = Command::new("service")
                            .args(["docker", "start"])
                            .output()
                            .map_err(|e| eyre!("Failed to start Docker daemon: {}", e))?;

                        if !service_result.status.success() {
                            let stderr = String::from_utf8_lossy(&service_result.stderr);
                            return Err(eyre!("Failed to start Docker daemon: {}", stderr));
                        }
                    }
                }
            }

            // Podman on Linux
            (ContainerRuntime::Podman, "linux") => {
                print_status("PODMAN", "Starting Podman service on Linux...");

                // Try to start user service (rootless)
                let user_service = Command::new("systemctl")
                    .args(["--user", "start", "podman.socket"])
                    .output();

                // Only skip fallback if command succeeded AND status is success
                if !user_service.is_ok_and(|output| output.status.success()) {
                    // Try system service (rootful) as fallback
                    let system_service = Command::new("systemctl")
                        .args(["start", "podman.socket"])
                        .output();

                    if let Err(e) = system_service {
                        print_warning(&format!("Could not start Podman service: {}", e));
                    }
                }
            }
            // Docker on Windows
            (ContainerRuntime::Docker, "windows") => {
                print_status("DOCKER", "Starting Docker Desktop for Windows...");
                // Try Docker Desktop CLI (4.37+) first
                let cli_result = Command::new("docker").args(["desktop", "start"]).output();

                match cli_result {
                    Ok(output) if output.status.success() => {
                        // Modern Docker Desktop CLI worked
                    }
                    _ => {
                        // Fallback to direct executable path for older versions
                        // Note: Empty string "" is required as window title parameter
                        Command::new("cmd")
                            .args([
                                "/c",
                                "start",
                                "",
                                "\"C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe\"",
                            ])
                            .output()
                            .map_err(|e| eyre!("Failed to start Docker Desktop: {}", e))?;
                    }
                }
            }

            // Podman on Windows
            (ContainerRuntime::Podman, "windows") => {
                print_status("PODMAN", "Starting Podman machine on Windows...");

                // Check if machine exists
                let list_output = Command::new("podman")
                    .args(["machine", "list", "--format", "{{.Name}}"])
                    .output()
                    .map_err(|e| eyre!("Failed to list Podman machines: {}", e))?;

                let machines = String::from_utf8_lossy(&list_output.stdout);

                if machines.trim().is_empty() {
                    // Initialize machine first
                    print_status("PODMAN", "Initializing Podman machine (first time)...");
                    let init_output = Command::new("podman")
                        .args(["machine", "init"])
                        .output()
                        .map_err(|e| eyre!("Failed to initialize Podman machine: {}", e))?;

                    if !init_output.status.success() {
                        let stderr = String::from_utf8_lossy(&init_output.stderr);
                        return Err(eyre!("Failed to initialize Podman machine: {}", stderr));
                    }
                }

                // Start the machine
                let start_output = Command::new("podman")
                    .args(["machine", "start"])
                    .output()
                    .map_err(|e| eyre!("Failed to start Podman machine: {}", e))?;

                if !start_output.status.success() {
                    let stderr = String::from_utf8_lossy(&start_output.stderr);
                    return Err(eyre!("Failed to start Podman machine: {}", stderr));
                }
            }

            (_, platform) => {
                return Err(eyre!(
                    "Unsupported platform '{}' for auto-starting {}",
                    platform,
                    runtime.label()
                ));
            }
        }

        Ok(())
    }

    fn wait_for_runtime(runtime: ContainerRuntime, timeout_secs: u64) -> Result<()> {
        print_status(runtime.label(), "Waiting for daemon to start...");

        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
            let output = Command::new(runtime.binary()).args(["info"]).output();

            if let Ok(output) = output
                && output.status.success()
            {
                print_status(runtime.label(), "Daemon is now running");
                return Ok(());
            }

            thread::sleep(Duration::from_millis(500));
        }

        Err(eyre!(
            "Timeout waiting for {} daemon to start. Please start {} manually and try again.",
            runtime.label(),
            runtime.binary()
        ))
    }

    /// Check if container runtime is installed and running, with auto-start option
    pub fn check_runtime_available(runtime: ContainerRuntime) -> Result<()> {
        let cmd = runtime.binary();

        let output = Command::new(cmd)
            .args(["--version"])
            .output()
            .map_err(|_| eyre!("{} is not installed or not available in PATH", cmd))?;

        if !output.status.success() {
            return Err(eyre!("{} is installed but not working properly", cmd));
        }

        // Check if daemon is running
        let output = Command::new(cmd)
            .args(["info"])
            .output()
            .map_err(|_| eyre!("Failed to check {} daemon status", cmd))?;

        if !output.status.success() {
            // Daemon not running - ask user if they want to start it
            let message = format!(
                "{} daemon is not running. Would you like to start {}?",
                runtime.label(),
                runtime.binary()
            );
            let should_start = print_confirm(&message).unwrap_or(false);

            if should_start {
                // Try to start the runtime
                Self::start_runtime_daemon(runtime)?;

                // Wait for it to be ready
                Self::wait_for_runtime(runtime, 15)?;

                // Verify it's running now
                let verify_output = Command::new(cmd)
                    .args(["info"])
                    .output()
                    .map_err(|_| eyre!("Failed to verify {} daemon status", cmd))?;

                if !verify_output.status.success() {
                    return Err(eyre!(
                        "{} daemon failed to start. Please start {} manually and try again.",
                        runtime.label(),
                        cmd
                    ));
                }
            } else {
                print_warning(&format!(
                    "{} daemon must be running to execute this command.",
                    runtime.label()
                ));
                return Err(eyre!(
                    "{} daemon is not running. Please start {}.",
                    cmd,
                    cmd
                ));
            }
        }

        Ok(())
    }

    /// Generate Dockerfile for an instance
    pub fn generate_dockerfile(
        &self,
        instance_name: &str,
        instance_config: InstanceInfo<'_>,
    ) -> Result<String> {
        let build_flag = match instance_config.build_mode() {
            BuildMode::Debug => "",
            BuildMode::Release => "--release",
            BuildMode::Dev => "--features dev",
        };
        let build_mode = match instance_config.build_mode() {
            BuildMode::Debug => "debug",
            BuildMode::Release => "release",
            BuildMode::Dev => "debug",
        };

        let dockerfile = format!(
            r#"# Generated Dockerfile for Helix instance: {instance_name}
FROM lukemathwalker/cargo-chef:latest-rust-1.88 AS chef
WORKDIR /build

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the cached repo workspace first (contains all dependencies and Cargo.toml files)
COPY helix-repo-copy/ ./

# Then overlay instance-specific files
COPY helix-container/ ./helix-container/

FROM chef AS planner
# Generate the recipe file for dependency caching
RUN cargo chef prepare --recipe-path recipe.json --bin helix-container

FROM chef AS builder
# Copy the recipe file
COPY --from=planner /build/recipe.json recipe.json

# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook {build_flag} --recipe-path recipe.json --bin helix-container

# Copy source code and build the application
COPY helix-repo-copy/ ./
COPY helix-container/ ./helix-container/
RUN cargo build {build_flag} --package helix-container

# Runtime image
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary
COPY --from=builder /build/target/{build_mode}/helix-container /usr/local/bin/helix-container

# Create data directory
RUN mkdir -p /data

# Expose port (will be overridden by docker-compose)
EXPOSE 6969

# Run the application
CMD ["helix-container"]
"#
        );

        Ok(dockerfile)
    }

    /// Generate docker-compose.yml for an instance
    pub fn generate_docker_compose(
        &self,
        instance_name: &str,
        instance_config: InstanceInfo<'_>,
    ) -> Result<String> {
        let port = instance_config.port().unwrap_or(6969);
        let default_volume = self.project.instance_volume(instance_name);
        let volume_source = if instance_config.is_local() {
            let data_dir = self.project.instance_data_dir(instance_name)?;
            if data_dir == default_volume {
                format!("../.volumes/{instance_name}")
            } else {
                data_dir.display().to_string()
            }
        } else {
            format!("../.volumes/{instance_name}")
        };

        let volume_spec = self.compose_volume_spec(&volume_source);

        // Use centralized naming methods
        let service_name = Self::service_name();
        let image_name = self.image_name(instance_name, instance_config.build_mode());
        let container_name = self.container_name(instance_name);
        let network_name = self.network_name(instance_name); // Get all environment variables dynamically
        let env_vars = self.environment_variables(instance_name);
        let env_section = env_vars
            .iter()
            .map(|var| format!("      - {var}"))
            .collect::<Vec<_>>()
            .join("\n");

        let compose = format!(
            r#"# Generated docker-compose.yml for Helix instance: {instance_name}
services:
  {service_name}:
    build:
      context: .
      dockerfile: Dockerfile
      {platform}
    image: {image_name}
    container_name: {container_name}
    ports:
      - "{port}:{port}"
    volumes:
      - {volume_spec}
    environment:
{env_section}
    restart: unless-stopped
    networks:
      - {network_name}

networks:
  {network_name}:
    driver: bridge
"#,
            platform = instance_config
                .docker_build_target()
                .map_or("".to_string(), |p| format!("platforms:\n        - {p}")),
            volume_source = volume_source,
        );

        Ok(compose)
    }

    /// Build Docker/Podman image for an instance
    pub fn build_image(&self, instance_name: &str, _build_target: Option<&str>) -> Result<()> {
        print_status(
            self.runtime.label(),
            &format!("Building image for instance '{instance_name}'..."),
        );
        let output = self.run_compose_command(instance_name, vec!["build"])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let full_output = format!("{}\n{}", stderr, stdout);

            // Check if this is a Rust compilation error
            if is_rust_compilation_error(&full_output) {
                return Err(DockerBuildError::RustCompilation {
                    output: full_output,
                    instance_name: instance_name.to_string(),
                }
                .into());
            }

            return Err(eyre!("{} build failed:\n{stderr}", self.runtime.binary()));
        }
        print_status(self.runtime.label(), "Image built successfully");

        Ok(())
    }

    /// Start instance using docker/podman compose
    pub fn start_instance(&self, instance_name: &str) -> Result<()> {
        print_status(
            self.runtime.label(),
            &format!("Starting instance '{instance_name}'..."),
        );

        let output = self.run_compose_command(instance_name, vec!["up", "-d"])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to start instance:\n{stderr}"));
        }

        print_status(
            self.runtime.label(),
            &format!("Instance '{instance_name}' started successfully"),
        );
        Ok(())
    }

    /// Stop instance using docker/podman compose
    pub fn stop_instance(&self, instance_name: &str) -> Result<()> {
        print_status(
            self.runtime.label(),
            &format!("Stopping instance '{instance_name}'..."),
        );

        let output = self.run_compose_command(instance_name, vec!["down"])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to stop instance:\n{stderr}"));
        }

        print_status(
            self.runtime.label(),
            &format!("Instance '{instance_name}' stopped successfully"),
        );
        Ok(())
    }

    /// Check if an instance container exists (running or stopped)
    pub fn instance_exists(&self, instance_name: &str) -> Result<bool> {
        let statuses = self.get_project_status()?;
        let target_container_name = format!("helix_{instance_name}_app");

        Ok(statuses
            .iter()
            .any(|status| status.container_name.contains(&target_container_name)))
    }

    /// Get status of all Docker/Podman containers for this project
    pub fn get_project_status(&self) -> Result<Vec<ContainerStatus>> {
        let project_name = &self.project.config.project.name;
        let filter = format!("name=helix-{project_name}-");

        let output = self.run_docker_command(&[
            "ps",
            "-a",
            "--format",
            "{{.Names}}\t{{.Status}}\t{{.Ports}}\t{{.Image}}",
            "--filter",
            &filter,
        ])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to get container status:\n{stderr}"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut statuses = Vec::new();

        // Process each line (no header with non-table format)
        for line in stdout.lines() {
            if line.trim().is_empty() {
                continue;
            }

            // Tab-separated output since we removed "table" format
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 3 {
                let name = parts[0].trim();
                let status = parts[1].trim();
                let ports = parts[2].trim();

                // Extract instance name from new container naming scheme: helix-{project}-{instance}-app
                let expected_prefix = format!("helix-{project_name}-");

                let instance_name = if let Some(suffix) = name.strip_prefix(&expected_prefix) {
                    // Remove the trailing "-app" if it exists
                    suffix.strip_suffix("-app").unwrap_or(suffix)
                } else {
                    name
                };

                statuses.push(ContainerStatus {
                    instance_name: instance_name.to_string(),
                    container_name: name.to_string(),
                    status: status.to_string(),
                    ports: ports.to_string(),
                });
            }
        }

        Ok(statuses)
    }

    /// Remove instance containers and optionally volumes
    pub fn prune_instance(&self, instance_name: &str, remove_volumes: bool) -> Result<()> {
        print_status(
            self.runtime.label(),
            &format!("Pruning instance '{instance_name}'..."),
        );

        // Check if workspace exists - if not, there's nothing to prune
        let workspace = self.project.instance_workspace(instance_name);
        if !workspace.exists() {
            print_status(
                self.runtime.label(),
                &format!("No workspace found for instance '{instance_name}', nothing to prune"),
            );
            return Ok(());
        }

        // Check if docker-compose file exists
        let compose_file = workspace.join("docker-compose.yml");
        if !compose_file.exists() {
            print_status(
                self.runtime.label(),
                &format!(
                    "No docker-compose.yml found for instance '{instance_name}', nothing to prune"
                ),
            );
            return Ok(());
        }

        // Stop and remove containers
        let mut args = vec!["down"];
        if remove_volumes {
            args.push("--volumes");
            args.push("--remove-orphans");
        }

        let output = self.run_compose_command(instance_name, args)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Don't fail if containers are already down
            if stderr.contains("No such container") || stderr.contains("not running") {
                print_status(
                    self.runtime.label(),
                    &format!("Instance '{instance_name}' containers already stopped"),
                );
            } else {
                return Err(eyre!("Failed to prune instance:\n{stderr}"));
            }
        } else {
            print_status(
                self.runtime.label(),
                &format!("Instance '{instance_name}' pruned successfully"),
            );
        }

        // Clean up orphaned named volumes from old CLI versions
        // Old volume naming pattern: helix_{project_name}_{instance_name}_data
        if remove_volumes {
            let old_volume_name = format!(
                "helix_{}_{}",
                self.project.config.project.name.replace("-", "_"),
                instance_name.replace("-", "_")
            );

            // Try to remove old-style named volume (ignore errors if it doesn't exist)
            let volume_to_remove = format!("{old_volume_name}_data");
            let _ = self.run_docker_command(&["volume", "rm", &volume_to_remove]);
        }

        Ok(())
    }

    /// Remove Docker/Podman images associated with an instance
    pub fn remove_instance_images(&self, instance_name: &str) -> Result<()> {
        print_status(
            self.runtime.label(),
            &format!("Removing images for instance '{instance_name}'..."),
        );

        // Get image names for both debug and release modes
        let debug_image = self.image_name(instance_name, BuildMode::Debug);
        let release_image = self.image_name(instance_name, BuildMode::Release);

        // Try to remove both images (ignore errors if they don't exist)
        for image in [debug_image, release_image] {
            let output = self.run_docker_command(&["rmi", "-f", &image])?;
            if output.status.success() {
                print_status(self.runtime.label(), &format!("Removed image: {image}"));
            }
        }

        Ok(())
    }

    /// Get all Helix-related images from the system
    pub fn get_helix_images(runtime: ContainerRuntime) -> Result<Vec<String>> {
        let output = Command::new(runtime.binary())
            .args([
                "images",
                "--format",
                "{{.Repository}}:{{.Tag}}",
                "--filter",
                "reference=helix-*",
            ])
            .output()
            .map_err(|e| eyre!("Failed to list {} images: {e}", runtime.binary()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to list images:\n{stderr}"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let images: Vec<String> = stdout
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| line.trim().to_string())
            .collect();

        Ok(images)
    }

    /// Remove all Helix-related images from the system
    pub fn clean_all_helix_images(runtime: ContainerRuntime) -> Result<()> {
        print_status(runtime.label(), "Finding all Helix images on system...");

        let images = Self::get_helix_images(runtime)?;

        if images.is_empty() {
            print_status(runtime.label(), "No Helix images found to clean");
            return Ok(());
        }

        let count = images.len();
        print_status(
            runtime.label(),
            &format!("Found {count} Helix images to remove"),
        );

        for image in images {
            let output = Command::new(runtime.binary())
                .args(["rmi", "-f", &image])
                .output()
                .map_err(|e| eyre!("Failed to remove image {image}: {e}"))?;

            if output.status.success() {
                print_status(runtime.label(), &format!("Removed image: {image}"));
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                print_status(
                    runtime.label(),
                    &format!("Warning: Failed to remove {image}: {stderr}"),
                );
            }
        }

        Ok(())
    }

    pub fn tag(&self, image_name: &str, registry_url: &str) -> Result<()> {
        let registry_image = format!("{registry_url}/{image_name}");
        Command::new(self.runtime.binary())
            .arg("tag")
            .arg(image_name)
            .arg(&registry_image)
            .output()?;

        Ok(())
    }

    pub fn push(&self, image_name: &str, registry_url: &str) -> Result<()> {
        let registry_image = format!("{registry_url}/{image_name}");
        print_status(
            self.runtime.label(),
            &format!("Pushing image: {registry_image}"),
        );
        let output = Command::new(self.runtime.binary())
            .arg("push")
            .arg(&registry_image)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to push image: {}", stderr));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ContainerStatus {
    pub instance_name: String,
    pub container_name: String,
    pub status: String,
    pub ports: String,
}
