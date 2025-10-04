use crate::config::default_release_build_mode;
use crate::{
    config::{self, BuildMode},
    docker::DockerManager,
    project::ProjectContext,
    utils::print_status,
};
use eyre::{Result, eyre};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::process::{Command, Output, Stdio};
use tokio::io::AsyncWriteExt;

const FLY_MACHINES_API_URL: &str = "https://api.machines.dev/v1/";
const FLY_REGISTRY_URL: &str = "registry.fly.io";
const INTERNAL_PORT: &str = "6969";

pub struct FlyManager<'a> {
    project: &'a ProjectContext,
    auth: FlyAuth,
}

/// Fly.io authentication method
#[derive(Debug)]
enum FlyAuth {
    ApiKey(String),
    Cli,
}

/// Authentication type selection
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub enum FlyAuthType {
    ApiKey,
    #[default]
    Cli,
}

impl TryFrom<String> for FlyAuthType {
    type Error = eyre::Report;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "api_key" => Ok(Self::ApiKey),
            "cli" => Ok(Self::Cli),
            _ => Err(eyre!(
                "Invalid auth type '{value}'. Valid options: api_key, cli"
            )),
        }
    }
}

/// VM sizes available on Fly.io
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub enum VmSize {
    /// 4 CPU, 1GB RAM
    #[serde(rename = "shared-cpu-4x")]
    SharedCpu4x,
    /// 8 CPU, 2GB RAM
    #[serde(rename = "shared-cpu-8x")]
    SharedCpu8x,
    /// 4 CPU, 8GB RAM
    #[default]
    #[serde(rename = "performance-4x")]
    PerformanceCpu4x,
    /// 8 CPU, 16GB RAM
    #[serde(rename = "performance-8x")]
    PerformanceCpu8x,
    /// 16 CPU, 32GB RAM
    #[serde(rename = "performance-16x")]
    PerformanceCpu16x,
    /// 8 CPU, 32GB RAM, a10 GPU
    #[serde(rename = "a10")]
    A10,
    /// 8 CPU, 32GB RAM, a100 pcie 40GB GPU
    #[serde(rename = "a100-40gb")]
    A10040Gb,
    /// 8 CPU, 32GB RAM, a100 sxm 80GB GPU
    #[serde(rename = "a100-80gb")]
    A10080Gb,
    /// 8 CPU, 32GB RAM, l40s GPU
    #[serde(rename = "l40s")]
    L40s,
}

impl TryFrom<String> for VmSize {
    type Error = eyre::Report;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "shared-cpu-4x" => Ok(Self::SharedCpu4x),
            "shared-cpu-8x" => Ok(Self::SharedCpu8x),
            "performance-4x" => Ok(Self::PerformanceCpu4x),
            "performance-8x" => Ok(Self::PerformanceCpu8x),
            "performance-16x" => Ok(Self::PerformanceCpu16x),
            "a10" => Ok(Self::A10),
            "a100-40gb" => Ok(Self::A10040Gb),
            "a100-80gb" => Ok(Self::A10080Gb),
            "l40s" => Ok(Self::L40s),
            _ => Err(eyre!(
                "Invalid VM size '{value}'. Valid options: shared-cpu-1x, shared-cpu-2x, shared-cpu-4x, shared-cpu-8x, performance-1x, performance-2x, performance-4x, performance-8x, performance-16x, a10, a100-40gb, a100-80gb, l40s"
            )),
        }
    }
}

impl VmSize {
    fn into_command_args(&self) -> [&'static str; 2] {
        let vm_size_arg = match self {
            VmSize::SharedCpu4x => "shared-cpu-4x",
            VmSize::SharedCpu8x => "shared-cpu-8x",
            VmSize::PerformanceCpu4x => "performance-4x",
            VmSize::PerformanceCpu8x => "performance-8x",
            VmSize::PerformanceCpu16x => "performance-16x",
            VmSize::A10 => "a10",
            VmSize::A10040Gb => "a100-40gb",
            VmSize::A10080Gb => "a100-80gb",
            VmSize::L40s => "l40s",
        };
        ["--vm-size", vm_size_arg]
    }
}
/// Configuration for a Fly.io instance
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FlyInstanceConfig {
    #[serde(default = "default_release_build_mode")]
    pub build_mode: BuildMode,
    #[serde(default)]
    pub region: Option<String>,
    pub vm_size: VmSize,
    pub volume: String,
    pub volume_initial_size: u16,
    #[serde(default)]
    pub private: bool,
    pub auth_type: FlyAuthType,
    #[serde(flatten)]
    pub db_config: config::DbConfig,
}

impl<'a> FlyManager<'a> {
    /// Create a new FlyManager
    pub async fn new(project: &'a ProjectContext, auth_type: FlyAuthType) -> Result<Self> {
        let auth = match auth_type {
            FlyAuthType::ApiKey => {
                let env_path = project.helix_dir.join("helix.env");
                let env_content = std::fs::read_to_string(&env_path).map_err(|_| {
                    eyre!(
                        "File {} not found. Create it with your FLY_API_KEY.",
                        env_path.display()
                    )
                })?;

                let api_key = env_content
                    .lines()
                    .find(|line| line.starts_with("FLY_API_KEY="))
                    .and_then(|line| line.split_once('=').map(|x| x.1))
                    .map(|key| key.trim().to_string())
                    .ok_or_else(|| eyre!("FLY_API_KEY not found in {}", env_path.display()))?;

                FlyAuth::ApiKey(api_key)
            }
            FlyAuthType::Cli => {
                Self::check_fly_cli_auth().await?;
                FlyAuth::Cli
            }
        };

        Ok(Self { project, auth })
    }

    // === CENTRALIZED NAMING METHODS ===

    /// Get the Fly.io app name for an instance
    fn app_name(&self, instance_name: &str) -> String {
        format!(
            "helix-{}-{}",
            self.project.config.project.name, instance_name
        )
    }

    /// Get the volume name for an instance
    fn volume_name(&self, instance_name: &str) -> String {
        format!("{}_data", self.app_name(instance_name).replace("-", "_"))
    }

    /// Get the registry image name for an instance
    fn registry_image_name(&self, image_name: &str) -> String {
        format!("{FLY_REGISTRY_URL}/{image_name}")
    }

    // === CENTRALIZED COMMAND EXECUTION ===

    /// Run a flyctl command with consistent error handling
    #[allow(unused)]
    fn run_fly_command(&self, args: &[&str]) -> Result<Output> {
        let output = Command::new("flyctl")
            .args(args)
            .output()
            .map_err(|e| eyre!("Failed to run flyctl {}: {e}", args.join(" ")))?;
        Ok(output)
    }

    /// Run a flyctl command asynchronously with consistent error handling
    async fn run_fly_command_async(&self, args: &[&str]) -> Result<Output> {
        let status = tokio::process::Command::new("flyctl")
            .args(args)
            .output()
            .await
            .map_err(|e| eyre!("Failed to run flyctl {}: {e}", args.join(" ")))?;
        Ok(status)
    }

    /// Get the API client and key (only for API auth)
    fn get_api_client(&self) -> Result<(&reqwest::Client, &str)> {
        match &self.auth {
            FlyAuth::ApiKey(api_key) => {
                // We'll create the client when needed for simplicity
                // In a real implementation, we might cache this
                static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
                let client = CLIENT.get_or_init(reqwest::Client::new);
                Ok((client, api_key))
            }
            FlyAuth::Cli => Err(eyre!(
                "API client not available when using CLI authentication"
            )),
        }
    }

    // === STATIC UTILITY METHODS ===

    /// Check if Fly.io CLI is installed and authenticated
    pub async fn check_fly_cli_available() -> Result<()> {
        let output = Command::new("flyctl")
            .output()
            .map_err(|_| eyre!("flyctl is not installed or not available in PATH. Visit https://fly.io/docs/flyctl/install/"))?;

        if !output.status.success() {
            return Err(eyre!("flyctl is installed but not working properly"));
        }

        Ok(())
    }

    /// Check if Fly.io CLI is authenticated
    async fn check_fly_cli_auth() -> Result<()> {
        Self::check_fly_cli_available().await?;

        println!("Checking Fly.io CLI authentication");
        let mut child = tokio::process::Command::new("flyctl")
            .args(["auth", "whoami"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| eyre!("Failed to check Fly.io authentication: {e}"))?;

        if let Some(stdin) = child.stdin.as_mut() {
            stdin.write_all(b"N\n").await?;
        }

        let status = child.wait().await?;
        if !status.success() {
            return Err(eyre!(
                "Fly.io CLI authentication failed. Run 'flyctl auth login' first."
            ));
        }

        Ok(())
    }

    // === INSTANCE CONFIGURATION ===

    /// Create a Fly.io instance configuration
    pub fn create_instance_config(
        &self,
        _docker: &DockerManager<'_>,
        instance_name: &str,
        volume_initial_size: u16,
        vm_size: VmSize,
        private: bool,
        auth_type: FlyAuthType,
    ) -> FlyInstanceConfig {
        let volume = format!("{}:/data", self.volume_name(instance_name));

        FlyInstanceConfig {
            build_mode: BuildMode::default(),
            region: None,
            vm_size,
            volume,
            volume_initial_size,
            private,
            auth_type,
            db_config: config::DbConfig::default(),
        }
    }

    // === DEPLOYMENT OPERATIONS ===

    /// Initialize a new Fly.io application
    pub async fn init_app(&self, instance_name: &str, config: &FlyInstanceConfig) -> Result<()> {
        let app_name = self.app_name(instance_name);

        print_status("FLY", &format!("Creating Fly.io app '{app_name}'"));

        match &self.auth {
            FlyAuth::ApiKey(api_key) => {
                let (client, _) = self.get_api_client()?;
                let request = json!({
                    "app_name": app_name,
                    "org_slug": "default",
                    "network": "default",
                });

                let response = client
                    .post(format!("{FLY_MACHINES_API_URL}/apps"))
                    .header("Authorization", format!("Bearer {api_key}"))
                    .json(&request)
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(eyre!(
                        "Failed to create Fly.io app '{app_name}': {}",
                        response.status()
                    ));
                }
            }
            FlyAuth::Cli => {
                // Configure app with launch
                let helix_dir_path = self.project.instance_workspace(instance_name);

                let volume_size_str = config.volume_initial_size.to_string();

                let mut launch_args = vec![
                    "launch",
                    "--no-deploy",
                    "--path",
                    helix_dir_path.to_str().ok_or_else(|| {
                        eyre!(
                            "cannot convert helix instance workspace to string: {helix_dir_path:?}"
                        )
                    })?,
                ];

                // Add VM size args
                let vm_args = config.vm_size.into_command_args();
                launch_args.extend_from_slice(&vm_args);

                // Add volume args
                let volume_name = config.volume.replace("-", "_");
                launch_args.extend_from_slice(&["--volume", &volume_name]);
                launch_args.extend_from_slice(&["--volume-initial-size", &volume_size_str]);

                // Add internal port args
                launch_args.extend_from_slice(&["--internal-port", INTERNAL_PORT]);

                // name the app
                launch_args.extend_from_slice(&["--name", &app_name]);

                // Add privacy args
                launch_args.extend_from_slice(&match config.private {
                    true => vec!["--no-public-ips"],
                    false => vec![],
                });

                let launch_status = tokio::process::Command::new("flyctl")
                    .args(&launch_args)
                    .output()
                    .await
                    .map_err(|e| eyre!("Failed to run flyctl launch: {e}"))?;

                if !launch_status.status.success() {
                    return Err(eyre!("Failed to configure Fly.io app '{app_name}'"));
                }
            }
        }

        println!("[FLY] App '{app_name}' created successfully");
        Ok(())
    }

    /// Deploy an image to Fly.io
    pub async fn deploy_image(
        &self,
        docker: &DockerManager<'_>,
        _config: &FlyInstanceConfig,
        instance_name: &str,
        image_name: &str,
    ) -> Result<()> {
        let app_name = self.app_name(instance_name);
        let registry_image = self.registry_image_name(image_name);
        let helix_dir_path = &self
            .project
            .instance_workspace(instance_name)
            .join("fly.toml")
            .display()
            .to_string();

        print_status("FLY", &format!("Deploying '{app_name}' to Fly.io"));
        println!("\tImage: {image_name}");

        match &self.auth {
            FlyAuth::ApiKey(_) => Err(eyre!(
                "API-based deployment not yet implemented. Use CLI authentication instead."
            )),
            FlyAuth::Cli => {
                // Tag image for Fly.io registry
                print_status("FLY", "Tagging image for Fly.io registry");

                // authenticate docker
                let auth_args = vec!["auth", "docker"];
                let auth_status = self.run_fly_command_async(&auth_args).await?;
                if !auth_status.status.success() {
                    return Err(eyre!("Failed to authenticate Docker with Fly.io"));
                }

                docker.tag(image_name, FLY_REGISTRY_URL)?;

                // Push image to registry
                print_status(
                    "FLY",
                    &format!("Pushing image '{image_name}' to Fly.io registry"),
                );
                docker.push(image_name, FLY_REGISTRY_URL)?;

                // Get environment variables first to ensure they live long enough
                let env_vars = docker.environment_variables(instance_name);

                let mut deploy_args = vec![
                    "deploy",
                    "--image",
                    &registry_image,
                    "--config",
                    &helix_dir_path,
                    "-a",
                    &app_name,
                    "--now",
                ];

                // Add environment variables to deploy args
                for env in &env_vars {
                    deploy_args.push("--env");
                    deploy_args.push(env);
                }

                // Deploy image
                print_status("FLY", "Deploying image to Fly.io");
                let deploy_status = self.run_fly_command_async(&deploy_args).await?;

                if !deploy_status.status.success() {
                    return Err(eyre!("Failed to deploy image '{registry_image}'"));
                }

                println!("[FLY] Image '{registry_image}' deployed successfully");
                Ok(())
            }
        }
    }

    /// Stop a Fly.io instance
    pub async fn stop_instance(&self, instance_name: &str) -> Result<()> {
        let app_name = self.app_name(instance_name);
        let stop_status = self
            .run_fly_command_async(&["scale", "count", "0", "-a", &app_name, "-y"])
            .await?;
        if !stop_status.status.success() {
            return Err(eyre!("Failed to stop Fly.io app '{app_name}'"));
        }

        println!("[FLY] App '{app_name}' stopped successfully");
        Ok(())
    }

    /// Start a Fly.io instance
    pub async fn start_instance(&self, instance_name: &str) -> Result<()> {
        let app_name = self.app_name(instance_name);
        let start_status = self
            .run_fly_command_async(&["scale", "count", "1", "-a", &app_name, "-y"])
            .await?;
        if !start_status.status.success() {
            return Err(eyre!("Failed to start Fly.io app '{app_name}'"));
        }

        println!("[FLY] App '{app_name}' started successfully");
        Ok(())
    }

    /// Get the status of Fly.io apps for this project
    #[allow(unused)]
    pub fn get_project_status(&self) -> Result<Vec<FlyAppStatus>> {
        let output = self.run_fly_command(&["apps", "list", "--json"])?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to get Fly.io app status:\n{stderr}"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let apps: serde_json::Value = serde_json::from_str(&stdout)
            .map_err(|e| eyre!("Failed to parse Fly.io apps JSON: {e}"))?;

        let mut statuses = Vec::new();
        let project_prefix = format!("helix-{}-", self.project.config.project.name);

        if let Some(apps_array) = apps.as_array() {
            for app in apps_array {
                if let Some(name) = app.get("name").and_then(|n| n.as_str())
                    && let Some(instance_name) = name.strip_prefix(&project_prefix)
                {
                    let status = app
                        .get("status")
                        .and_then(|s| s.as_str())
                        .unwrap_or("unknown");
                    let region = app
                        .get("primaryRegion")
                        .and_then(|r| r.as_str())
                        .unwrap_or("unknown");

                    statuses.push(FlyAppStatus {
                        instance_name: instance_name.to_string(),
                        app_name: name.to_string(),
                        status: status.to_string(),
                        region: region.to_string(),
                    });
                }
            }
        }

        Ok(statuses)
    }

    /// Delete a Fly.io application
    pub async fn delete_app(&self, instance_name: &str) -> Result<()> {
        let app_name = self.app_name(instance_name);

        print_status("FLY", &format!("Deleting Fly.io app '{app_name}'"));

        let delete_status = self
            .run_fly_command_async(&["apps", "destroy", &app_name, "--yes"])
            .await?;

        if !delete_status.status.success() {
            return Err(eyre!("Failed to delete Fly.io app '{app_name}'"));
        }

        println!("[FLY] App '{app_name}' deleted successfully");
        Ok(())
    }
}

#[derive(Debug)]
#[allow(unused)]
pub struct FlyAppStatus {
    pub instance_name: String,
    pub app_name: String,
    pub status: String,
    pub region: String,
}
