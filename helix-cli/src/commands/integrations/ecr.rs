use crate::config::default_release_build_mode;
use crate::{
    config::{self, BuildMode},
    docker::DockerManager,
    project::ProjectContext,
    utils::print_status,
};
use eyre::{Result, eyre};
use serde::{Deserialize, Serialize};
use std::process::{Command, Output};
use tokio::fs;

const DEFAULT_ECR_REGION: &str = "us-east-1";

pub struct EcrManager<'a> {
    project: &'a ProjectContext,
    #[allow(dead_code)]
    auth: EcrAuth,
}

/// AWS ECR authentication method
#[derive(Debug)]
enum EcrAuth {
    AwsCli,
    // Future: Could add IAM role, API key, etc.
}

/// Authentication type selection
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub enum EcrAuthType {
    #[default]
    AwsCli,
}

impl TryFrom<String> for EcrAuthType {
    type Error = eyre::Report;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "aws_cli" | "cli" => Ok(Self::AwsCli),
            _ => Err(eyre!(
                "Invalid auth type '{}'. Valid options: aws_cli",
                value
            )),
        }
    }
}

/// Configuration for an ECR repository
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EcrConfig {
    pub repository_name: String,
    pub region: String,
    pub registry_url: Option<String>,
    pub auth_type: EcrAuthType,
    #[serde(default = "default_release_build_mode")]
    pub build_mode: BuildMode,
    #[serde(flatten)]
    pub db_config: config::DbConfig,
}

impl Default for EcrConfig {
    fn default() -> Self {
        Self {
            repository_name: String::new(),
            region: DEFAULT_ECR_REGION.to_string(),
            registry_url: None,
            auth_type: EcrAuthType::default(),
            build_mode: default_release_build_mode(),
            db_config: config::DbConfig::default(),
        }
    }
}

impl<'a> EcrManager<'a> {
    /// Create a new EcrManager
    pub async fn new(project: &'a ProjectContext, auth_type: EcrAuthType) -> Result<Self> {
        let auth = match auth_type {
            EcrAuthType::AwsCli => {
                Self::check_aws_cli_auth().await?;
                EcrAuth::AwsCli
            }
        };

        Ok(Self { project, auth })
    }

    // === CENTRALIZED NAMING METHODS ===

    /// Get the ECR repository name for an instance
    fn repository_name(&self, instance_name: &str) -> String {
        format!("helix-{}-{instance_name}", self.project.config.project.name)
    }

    fn image_name(&self, repository_name: &str, build_mode: BuildMode) -> String {
        let tag = match build_mode {
            BuildMode::Debug => "debug",
            BuildMode::Release => "latest",
        };
        format!("{repository_name}:{tag}")
    }

    // === CENTRALIZED COMMAND EXECUTION ===

    /// Run an AWS CLI command with consistent error handling
    #[allow(dead_code)]
    fn run_aws_command(&self, args: &[&str]) -> Result<Output> {
        let output = Command::new("aws")
            .args(args)
            .output()
            .map_err(|e| eyre!("Failed to run aws {}: {e}", args.join(" ")))?;
        Ok(output)
    }

    /// Run an AWS CLI command asynchronously with consistent error handling
    async fn run_aws_command_async(&self, args: &[&str]) -> Result<Output> {
        let status = tokio::process::Command::new("aws")
            .args(args)
            .output()
            .await
            .map_err(|e| eyre!("Failed to run aws {}: {e}", args.join(" ")))?;
        Ok(status)
    }

    // === STATIC UTILITY METHODS ===

    /// Check if AWS CLI is installed and authenticated
    pub async fn check_aws_cli_available() -> Result<()> {
        let output = Command::new("aws")
            .args(["--version"])
            .output()
            .map_err(|_| eyre!("AWS CLI is not installed or not available in PATH. Visit https://aws.amazon.com/cli/"))?;

        if !output.status.success() {
            return Err(eyre!("AWS CLI is installed but not working properly"));
        }

        Ok(())
    }

    /// Check if AWS CLI is authenticated
    async fn check_aws_cli_auth() -> Result<()> {
        Self::check_aws_cli_available().await?;

        print_status("ECR", "Checking AWS CLI authentication");
        let output = tokio::process::Command::new("aws")
            .args(["sts", "get-caller-identity"])
            .output()
            .await
            .map_err(|e| eyre!("Failed to check AWS authentication: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!(
                "AWS CLI authentication failed. Configure your credentials with 'aws configure' first.\nError: {}",
                stderr
            ));
        }

        Ok(())
    }

    /// Get the AWS account ID
    async fn get_account_id(&self) -> Result<String> {
        let output = self
            .run_aws_command_async(&[
                "sts",
                "get-caller-identity",
                "--query",
                "Account",
                "--output",
                "text",
            ])
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to get AWS account ID: {stderr}"));
        }

        let account_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(account_id)
    }

    /// Get the ECR registry URL for the current account and region
    async fn get_registry_url(&self, region: &str) -> Result<String> {
        let account_id = self.get_account_id().await?;
        Ok(format!("{account_id}.dkr.ecr.{region}.amazonaws.com"))
    }

    // === CONFIGURATION MANAGEMENT ===

    /// Create an ECR configuration
    pub async fn create_ecr_config(
        &self,
        _instance_name: &str,
        region: Option<String>,
        auth_type: EcrAuthType,
    ) -> Result<EcrConfig> {
        let repository_name = self.repository_name(_instance_name);
        let region = region.unwrap_or_else(|| DEFAULT_ECR_REGION.to_string());
        let registry_url = Some(self.get_registry_url(&region).await?);

        Ok(EcrConfig {
            repository_name,
            region,
            registry_url,
            auth_type,
            build_mode: BuildMode::default(),
            db_config: config::DbConfig::default(),
        })
    }

    /// Save ECR configuration to file
    pub async fn save_config(&self, instance_name: &str, config: &EcrConfig) -> Result<()> {
        let config_path = self
            .project
            .instance_workspace(instance_name)
            .join("ecr.toml");

        // Ensure the directory exists
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let toml_content = toml::to_string_pretty(config)?;
        fs::write(&config_path, toml_content).await?;

        println!("[ECR] Configuration saved to {}", config_path.display());
        Ok(())
    }

    /// Load ECR configuration from file
    pub async fn load_config(&self, instance_name: &str) -> Result<EcrConfig> {
        let config_path = self
            .project
            .instance_workspace(instance_name)
            .join("ecr.toml");

        if !config_path.exists() {
            return Err(eyre!(
                "ECR configuration not found at {}. Run 'helix init ecr' first.",
                config_path.display()
            ));
        }

        let toml_content = fs::read_to_string(&config_path).await?;
        let config: EcrConfig = toml::from_str(&toml_content)?;

        Ok(config)
    }

    // === REPOSITORY OPERATIONS ===

    /// Initialize a new ECR repository
    pub async fn init_repository(&self, _instance_name: &str, config: &EcrConfig) -> Result<()> {
        let repository_name = &config.repository_name;
        let region = &config.region;

        print_status(
            "ECR",
            &format!("Creating ECR repository '{repository_name}'"),
        );

        // Check if repository already exists
        let check_output = self
            .run_aws_command_async(&[
                "ecr",
                "describe-repositories",
                "--repository-names",
                repository_name,
                "--region",
                region,
            ])
            .await?;

        if check_output.status.success() {
            println!("[ECR] Repository '{repository_name}' already exists");
            return Ok(());
        }

        // Create the repository
        let create_output = self
            .run_aws_command_async(&[
                "ecr",
                "create-repository",
                "--repository-name",
                repository_name,
                "--region",
                region,
                "--image-scanning-configuration",
                "scanOnPush=true",
            ])
            .await?;

        if !create_output.status.success() {
            let stderr = String::from_utf8_lossy(&create_output.stderr);
            return Err(eyre!(
                "Failed to create ECR repository '{repository_name}': {stderr}"
            ));
        }

        println!("[ECR] Repository '{repository_name}' created successfully");
        Ok(())
    }

    /// Deploy an image to ECR
    pub async fn deploy_image(
        &self,
        docker: &DockerManager<'_>,
        config: &EcrConfig,
        _instance_name: &str,
        image_name: &str,
    ) -> Result<()> {
        let tag = "latest";
        let registry_url = config
            .registry_url
            .as_ref()
            .ok_or_else(|| eyre!("Registry URL not found in configuration"))?;
        let repository_name = &config.repository_name;
        let region = &config.region;

        print_status("ECR", &format!("Deploying '{image_name}' to ECR"));
        println!("\tRepository: {repository_name}");
        println!("\tRegion: {region}");
        println!("\tTag: {tag}");

        // Authenticate Docker with ECR
        print_status("ECR", "Authenticating Docker with ECR");
        let auth_output = self
            .run_aws_command_async(&["ecr", "get-login-password", "--region", region])
            .await?;

        if !auth_output.status.success() {
            let stderr = String::from_utf8_lossy(&auth_output.stderr);
            return Err(eyre!("Failed to get ECR login password: {stderr}"));
        }

        let password = String::from_utf8_lossy(&auth_output.stdout)
            .trim()
            .to_string();

        use tokio::io::AsyncWriteExt;
        let mut login_cmd = tokio::process::Command::new("docker");
        login_cmd.args([
            "login",
            "--username",
            "AWS",
            "--password-stdin",
            registry_url,
        ]);
        login_cmd.stdin(std::process::Stdio::piped());
        let mut child = login_cmd.spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(password.as_bytes()).await?;
        }
        let output = child.wait_with_output().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to login to ECR: {}", stderr));
        }
        // Tag image for ECR
        print_status("ECR", "Tagging image for ECR");
        let image_name = self.image_name(repository_name, config.build_mode);
        docker.tag(&image_name, registry_url)?;

        // Push image to ECR
        print_status("ECR", &format!("Pushing image '{image_name}' to ECR"));
        docker.push(&image_name, registry_url)?;

        println!("[ECR] Image '{image_name}' deployed successfully to {registry_url}");
        Ok(())
    }

    /// Delete an ECR repository
    pub async fn delete_repository(&self, instance_name: &str) -> Result<()> {
        let config = self.load_config(instance_name).await?;
        let repository_name = &config.repository_name;
        let region = &config.region;

        print_status(
            "ECR",
            &format!("Deleting ECR repository '{repository_name}'"),
        );

        let delete_output = self
            .run_aws_command_async(&[
                "ecr",
                "delete-repository",
                "--repository-name",
                repository_name,
                "--region",
                region,
                "--force", // Force delete even if repository contains images
            ])
            .await?;

        if !delete_output.status.success() {
            let stderr = String::from_utf8_lossy(&delete_output.stderr);
            // Check if repository doesn't exist
            if stderr.contains("RepositoryNotFoundException") {
                println!("[ECR] Repository '{repository_name}' does not exist");
                return Ok(());
            }
            return Err(eyre!(
                "Failed to delete ECR repository '{repository_name}': {stderr}"
            ));
        }

        println!("[ECR] Repository '{repository_name}' deleted successfully");
        Ok(())
    }

    /// Get the status of ECR repositories for this project
    #[allow(dead_code)]
    pub async fn get_project_status(&self) -> Result<Vec<EcrRepositoryStatus>> {
        let _account_id = self.get_account_id().await?;
        let project_prefix = format!("helix-{}-", self.project.config.project.name);

        // List all repositories
        let output = self
            .run_aws_command_async(&[
                "ecr",
                "describe-repositories",
                "--query",
                &format!("repositories[?starts_with(repositoryName, '{project_prefix}')]"),
                "--output",
                "json",
            ])
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("Failed to list ECR repositories: {stderr}"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let repositories: serde_json::Value = serde_json::from_str(&stdout)
            .map_err(|e| eyre!("Failed to parse ECR repositories JSON: {e}"))?;

        let mut statuses = Vec::new();

        if let Some(repos_array) = repositories.as_array() {
            for repo in repos_array {
                if let Some(name) = repo.get("repositoryName").and_then(|n| n.as_str())
                    && let Some(instance_name) = name.strip_prefix(&project_prefix)
                {
                    let uri = repo
                        .get("repositoryUri")
                        .and_then(|u| u.as_str())
                        .unwrap_or("unknown");
                    let created_at = repo
                        .get("createdAt")
                        .and_then(|c| c.as_str())
                        .unwrap_or("unknown");

                    statuses.push(EcrRepositoryStatus {
                        instance_name: instance_name.to_string(),
                        repository_name: name.to_string(),
                        repository_uri: uri.to_string(),
                        created_at: created_at.to_string(),
                    });
                }
            }
        }

        Ok(statuses)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct EcrRepositoryStatus {
    pub instance_name: String,
    pub repository_name: String,
    pub repository_uri: String,
    pub created_at: String,
}
