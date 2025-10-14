use crate::config::{BuildMode, InstanceInfo};
use crate::env_utils::load_env_variables;
use crate::project::ProjectContext;
use crate::utils::print_status;
use eyre::{Result, eyre};
use std::process::{Command, Output};

pub struct DockerManager<'a> {
    project: &'a ProjectContext,
}

pub const HELIX_DATA_DIR: &str = "/data";

impl<'a> DockerManager<'a> {
    pub fn new(project: &'a ProjectContext) -> Self {
        Self { project }
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
    pub(crate) fn environment_variables(&self, instance_name: &str) -> Result<Vec<String>> {
        let instance = self.project.config.get_instance(instance_name)?;

        // Start with Helix built-in variables (highest priority)
        let mut env_vars = vec![
            format!("HELIX_PORT={}", instance.port().unwrap_or(6969)),
            format!("HELIX_DATA_DIR={HELIX_DATA_DIR}"),
            format!("HELIX_INSTANCE={instance_name}"),
            format!("HELIX_PROJECT={}", self.project.config.project.name),
        ];

        // Load user-defined environment variables if this is a local instance
        if let InstanceInfo::Local(local_config) = instance
            && let Some(env_file) = &local_config.env_file
        {
            let user_vars = load_env_variables(Some(env_file.as_path()), &self.project.root)?;

            // Add user variables that don't conflict with Helix built-ins
            for (key, value) in user_vars {
                if !key.starts_with("HELIX_") {
                    env_vars.push(format!("{key}={value}"));
                }
            }
        }

        Ok(env_vars)
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

    // === CENTRALIZED DOCKER COMMAND EXECUTION ===

    /// Run a docker command with consistent error handling
    pub fn run_docker_command(&self, args: &[&str]) -> Result<Output> {
        let output = Command::new("docker")
            .args(args)
            .output()
            .map_err(|e| eyre!("Failed to run docker {}: {e}", args.join(" ")))?;
        Ok(output)
    }

    /// Run a docker compose command with proper project naming
    fn run_compose_command(&self, instance_name: &str, args: Vec<&str>) -> Result<Output> {
        let workspace = self.project.instance_workspace(instance_name);
        let project_name = self.compose_project_name(instance_name);

        let mut full_args = vec!["--project-name", &project_name];
        full_args.extend(args);

        let output = Command::new("docker")
            .arg("compose")
            .args(&full_args)
            .current_dir(&workspace)
            .output()
            .map_err(|e| eyre!("Failed to run docker compose {}: {e}", full_args.join(" ")))?;
        Ok(output)
    }

    /// Check if Docker is installed and running
    pub fn check_docker_available() -> Result<()> {
        let output = Command::new("docker")
            .args(["--version"])
            .output()
            .map_err(|_| eyre!("Docker is not installed or not available in PATH"))?;

        if !output.status.success() {
            return Err(eyre!("Docker is installed but not working properly"));
        }

        // Check if Docker daemon is running
        let output = Command::new("docker")
            .args(["info"])
            .output()
            .map_err(|_| eyre!("Failed to check Docker daemon status"))?;

        if !output.status.success() {
            return Err(eyre!("Docker daemon is not running. Please start Docker."));
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

        // Use centralized naming methods
        let service_name = Self::service_name();
        let image_name = self.image_name(instance_name, instance_config.build_mode());
        let container_name = self.container_name(instance_name);
        let network_name = self.network_name(instance_name);

        // Get environment variables including user-defined ones
        let env_vars = self.environment_variables(instance_name)?;
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
      - ../.volumes/{instance_name}:/data
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
        );

        Ok(compose)
    }

    /// Build Docker image for an instance
    pub fn build_image(&self, instance_name: &str, _build_target: Option<&str>) -> Result<()> {
        print_status(
            "DOCKER",
            &format!("Building image for instance '{instance_name}'..."),
        );

        let output = self.run_compose_command(instance_name, vec!["build"])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Docker build failed:\n{stderr}"));
        }

        print_status("DOCKER", "Image built successfully");
        Ok(())
    }

    /// Start instance using docker compose
    pub fn start_instance(&self, instance_name: &str) -> Result<()> {
        print_status("DOCKER", &format!("Starting instance '{instance_name}'..."));

        let output = self.run_compose_command(instance_name, vec!["up", "-d"])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to start instance:\n{stderr}"));
        }

        print_status(
            "DOCKER",
            &format!("Instance '{instance_name}' started successfully"),
        );
        Ok(())
    }

    /// Stop instance using docker compose
    pub fn stop_instance(&self, instance_name: &str) -> Result<()> {
        print_status("DOCKER", &format!("Stopping instance '{instance_name}'..."));

        let output = self.run_compose_command(instance_name, vec!["down"])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to stop instance:\n{stderr}"));
        }

        print_status(
            "DOCKER",
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

    /// Get status of all Docker containers for this project
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
        print_status("DOCKER", &format!("Pruning instance '{instance_name}'..."));

        // Check if workspace exists - if not, there's nothing to prune
        let workspace = self.project.instance_workspace(instance_name);
        if !workspace.exists() {
            print_status(
                "DOCKER",
                &format!("No workspace found for instance '{instance_name}', nothing to prune"),
            );
            return Ok(());
        }

        // Check if docker-compose file exists
        let compose_file = workspace.join("docker-compose.yml");
        if !compose_file.exists() {
            print_status(
                "DOCKER",
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
                    "DOCKER",
                    &format!("Instance '{instance_name}' containers already stopped"),
                );
            } else {
                return Err(eyre!("Failed to prune instance:\n{stderr}"));
            }
        } else {
            print_status(
                "DOCKER",
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

    /// Remove Docker images associated with an instance
    pub fn remove_instance_images(&self, instance_name: &str) -> Result<()> {
        print_status(
            "DOCKER",
            &format!("Removing images for instance '{instance_name}'..."),
        );

        // Get image names for both debug and release modes
        let debug_image = self.image_name(instance_name, BuildMode::Debug);
        let release_image = self.image_name(instance_name, BuildMode::Release);

        // Try to remove both images (ignore errors if they don't exist)
        for image in [debug_image, release_image] {
            let output = self.run_docker_command(&["rmi", "-f", &image])?;
            if output.status.success() {
                print_status("DOCKER", &format!("Removed image: {image}"));
            }
        }

        Ok(())
    }

    /// Get all Helix-related Docker images from the system
    pub fn get_helix_images() -> Result<Vec<String>> {
        let output = Command::new("docker")
            .args([
                "images",
                "--format",
                "{{.Repository}}:{{.Tag}}",
                "--filter",
                "reference=helix-*",
            ])
            .output()
            .map_err(|e| eyre!("Failed to list Docker images: {e}"))?;

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

    /// Remove all Helix-related Docker images from the system
    pub fn clean_all_helix_images() -> Result<()> {
        print_status("DOCKER", "Finding all Helix images on system...");

        let images = Self::get_helix_images()?;

        if images.is_empty() {
            print_status("DOCKER", "No Helix images found to clean");
            return Ok(());
        }

        let count = images.len();
        print_status("DOCKER", &format!("Found {count} Helix images to remove"));

        for image in images {
            let output = Command::new("docker")
                .args(["rmi", "-f", &image])
                .output()
                .map_err(|e| eyre!("Failed to remove image {image}: {e}"))?;

            if output.status.success() {
                print_status("DOCKER", &format!("Removed image: {image}"));
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                print_status(
                    "DOCKER",
                    &format!("Warning: Failed to remove {image}: {stderr}"),
                );
            }
        }

        Ok(())
    }

    pub fn tag(&self, image_name: &str, registry_url: &str) -> Result<()> {
        let registry_image = format!("{registry_url}/{image_name}");
        Command::new("docker")
            .arg("tag")
            .arg(image_name)
            .arg(&registry_image)
            .output()?;

        Ok(())
    }

    pub fn push(&self, image_name: &str, registry_url: &str) -> Result<()> {
        let registry_image = format!("{registry_url}/{image_name}");
        print_status("DOCKER", &format!("Pushing image: {registry_image}"));
        let output = Command::new("docker")
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
