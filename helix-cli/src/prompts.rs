//! Interactive prompts for the Helix CLI using cliclack.
//!
//! This module provides a consistent, user-friendly interactive experience
//! for commands like `init` and `add` when flags are not provided.

use crate::CloudDeploymentTypeCommand;
use crate::commands::auth::require_auth;
use crate::commands::feedback::FeedbackType;
use crate::commands::integrations::fly::VmSize;
use eyre::Result;

/// Deployment type options for interactive selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentType {
    Local,
    HelixCloud,
    Ecr,
    Fly,
}

/// AWS/Helix Cloud region options
/// More regions coming soon!
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Region {
    UsEast1,
}

impl Region {
    pub fn as_str(&self) -> &'static str {
        match self {
            Region::UsEast1 => "us-east-1",
        }
    }
}

/// Show the intro banner for interactive mode
pub fn intro(title: &str, subheader: Option<&str>) -> Result<()> {
    match subheader {
        Some(sub) => cliclack::note(title, sub)?,
        None => cliclack::intro(title.to_string())?,
    }
    Ok(())
}

/// Show note banner
#[allow(unused)]
pub fn note(message: &str) -> Result<()> {
    cliclack::log::remark(message)?;
    Ok(())
}

/// Show warning banner
#[allow(unused)]
pub fn warning(message: &str) -> Result<()> {
    cliclack::log::warning(message)?;
    Ok(())
}

/// Show the outro banner when interactive mode completes
#[allow(dead_code)]
pub fn outro(message: &str) -> Result<()> {
    cliclack::outro(message.to_string())?;
    Ok(())
}

/// Prompt user to select a deployment type with descriptions
pub fn select_deployment_type() -> Result<DeploymentType> {
    let selected: DeploymentType = cliclack::select("Where would you like to deploy?")
        .item(
            DeploymentType::Local,
            "Local",
            "Run Helix locally in Docker. Best for development.",
        )
        .item(
            DeploymentType::HelixCloud,
            "Helix Cloud",
            "Managed hosting with automatic scaling. One-click deployment.",
        )
        .item(
            DeploymentType::Ecr,
            "AWS ECR",
            "Push to your own AWS Elastic Container Registry.",
        )
        .item(
            DeploymentType::Fly,
            "Fly.io",
            "Deploy globally on Fly.io edge infrastructure.",
        )
        .interact()?;

    Ok(selected)
}

/// Prompt user to select a cloud region
pub fn select_region() -> Result<String> {
    // More regions coming soon!
    let selected: Region = cliclack::select("Select a region")
        .item(Region::UsEast1, "us-east-1", "More regions coming soon!")
        .interact()?;

    Ok(selected.as_str().to_string())
}

/// Prompt user to select a Fly.io VM size
pub fn select_fly_vm_size() -> Result<VmSize> {
    let selected: VmSize = cliclack::select("Select VM size")
        .item(
            VmSize::SharedCpu4x,
            "shared-cpu-4x",
            "4 shared CPUs, 1GB RAM - Development & small workloads",
        )
        .item(
            VmSize::SharedCpu8x,
            "shared-cpu-8x",
            "8 shared CPUs, 2GB RAM - Medium workloads",
        )
        .item(
            VmSize::PerformanceCpu4x,
            "performance-4x",
            "4 dedicated CPUs, 8GB RAM - Production (Recommended)",
        )
        .item(
            VmSize::PerformanceCpu8x,
            "performance-8x",
            "8 dedicated CPUs, 16GB RAM - High performance",
        )
        .interact()?;

    Ok(selected)
}

/// Prompt user to enter Fly.io volume size in GB
pub fn input_fly_volume_size() -> Result<u16> {
    let size: String = cliclack::input("Volume size in GB")
        .default_input("20")
        .placeholder("20")
        .validate(|input: &String| match input.parse::<u16>() {
            Ok(n) if (1..=500).contains(&n) => Ok(()),
            Ok(_) => Err("Volume size must be between 1 and 500 GB"),
            Err(_) => Err("Please enter a valid number"),
        })
        .interact()?;

    Ok(size.parse().unwrap_or(20))
}

/// Prompt user for a yes/no confirmation
pub fn confirm(message: &str) -> Result<bool> {
    let result = cliclack::confirm(message).interact()?;
    Ok(result)
}

/// Prompt user to enter an instance name
pub fn input_instance_name(default: &str) -> Result<String> {
    let name: String = cliclack::input("Instance name")
        .default_input(default)
        .placeholder(default)
        .validate(|input: &String| {
            if input.is_empty() {
                Err("Instance name cannot be empty")
            } else if input.len() > 32 {
                Err("Instance name must be 32 characters or less")
            } else if !input
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            {
                Err("Instance name can only contain letters, numbers, hyphens, and underscores")
            } else {
                Ok(())
            }
        })
        .interact()?;

    Ok(name)
}

/// Build a CloudDeploymentTypeCommand from interactive selections
///
/// This is the main entry point for interactive mode for `helix add`.
/// It prompts the user through all necessary options including instance name,
/// and returns a fully configured command.
pub async fn build_deployment_command(
    default_name: &str,
) -> Result<Option<CloudDeploymentTypeCommand>> {
    let deployment_type = select_deployment_type()?;

    // Check auth early for Helix Cloud instances before prompting for more details
    if matches!(deployment_type, DeploymentType::HelixCloud) {
        require_auth().await?;
    }

    // Prompt for instance name with project name as default
    let instance_name = input_instance_name(default_name)?;

    match deployment_type {
        DeploymentType::Local => Ok(Some(CloudDeploymentTypeCommand::Local {
            name: Some(instance_name),
        })),
        DeploymentType::HelixCloud => {
            // Check auth early for Helix Cloud instances
            let region = select_region()?;
            Ok(Some(CloudDeploymentTypeCommand::Helix {
                region: Some(region),
                name: Some(instance_name),
            }))
        }
        DeploymentType::Ecr => Ok(Some(CloudDeploymentTypeCommand::Ecr {
            name: Some(instance_name),
        })),
        DeploymentType::Fly => {
            let vm_size = select_fly_vm_size()?;
            let volume_size = input_fly_volume_size()?;
            let private = confirm("Make deployment private (internal network only)?")?;

            Ok(Some(CloudDeploymentTypeCommand::Fly {
                auth: "cli".to_string(),
                volume_size,
                vm_size: vm_size.as_str().to_string(),
                private,
                name: Some(instance_name),
            }))
        }
    }
}

/// Build a CloudDeploymentTypeCommand for the init command
/// Returns None for local deployment (the default)
pub async fn build_init_deployment_command() -> Result<Option<CloudDeploymentTypeCommand>> {
    let deployment_type = select_deployment_type()?;

    if matches!(deployment_type, DeploymentType::HelixCloud) {
        require_auth().await?;
    }

    match deployment_type {
        DeploymentType::Local => {
            // Local is the default for init, return None to use default behavior
            Ok(None)
        }
        DeploymentType::HelixCloud => {
            let region = select_region()?;
            Ok(Some(CloudDeploymentTypeCommand::Helix {
                region: Some(region),
                name: None,
            }))
        }
        DeploymentType::Ecr => Ok(Some(CloudDeploymentTypeCommand::Ecr { name: None })),
        DeploymentType::Fly => {
            let vm_size = select_fly_vm_size()?;
            let volume_size = input_fly_volume_size()?;
            let private = confirm("Make deployment private (internal network only)?")?;

            Ok(Some(CloudDeploymentTypeCommand::Fly {
                auth: "cli".to_string(),
                volume_size,
                vm_size: vm_size.as_str().to_string(),
                private,
                name: None,
            }))
        }
    }
}

/// Check if we're running in an interactive terminal
pub fn is_interactive() -> bool {
    use std::io::IsTerminal;
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

/// Prompt user to select an instance from available instances
///
/// Takes a slice of (name, type_hint) tuples to show instance types.
/// If only one instance exists, it will be auto-selected without prompting.
/// If no instances exist, returns an error.
pub fn select_instance(instances: &[(&String, &str)]) -> Result<String> {
    if instances.is_empty() {
        return Err(eyre::eyre!(
            "No instances found in helix.toml. Run 'helix init' to create a project first."
        ));
    }

    // Auto-select if only one instance
    if instances.len() == 1 {
        return Ok(instances[0].0.clone());
    }

    let mut select = cliclack::select("Select an instance");
    for (name, type_hint) in instances {
        select = select.item((*name).clone(), name.as_str(), *type_hint);
    }
    let selected = select.interact()?;
    Ok(selected)
}

/// Prompt user to select a feedback type
pub fn select_feedback_type() -> Result<FeedbackType> {
    let selected: FeedbackType = cliclack::select("What type of feedback would you like to send?")
        .item(
            FeedbackType::Bug,
            "Bug Report",
            "Report a bug or issue you've encountered",
        )
        .item(
            FeedbackType::FeatureRequest,
            "Feature Request",
            "Suggest a new feature or improvement",
        )
        .item(
            FeedbackType::General,
            "General Feedback",
            "Share general thoughts or comments",
        )
        .interact()?;

    Ok(selected)
}

/// Prompt user to enter their feedback message
pub fn input_feedback_message() -> Result<String> {
    let message: String = cliclack::input("Enter your feedback")
        .placeholder("Describe your feedback here...")
        .validate(|input: &String| {
            if input.trim().is_empty() {
                Err("Feedback message cannot be empty")
            } else if input.len() < 10 {
                Err("Please provide more detail (at least 10 characters)")
            } else {
                Ok(())
            }
        })
        .interact()?;

    Ok(message)
}

/// Prompt user to select a workspace from a list
pub fn select_workspace(workspaces: &[crate::commands::sync::CliWorkspace]) -> Result<String> {
    if workspaces.len() == 1 {
        return Ok(workspaces[0].id.clone());
    }

    let mut select = cliclack::select("Select a workspace");
    for ws in workspaces {
        select = select.item(ws.id.clone(), ws.name.as_str(), "");
    }
    let selected = select.interact()?;
    Ok(selected)
}

/// Prompt user to select a cluster from a list
pub fn select_cluster(clusters: &[crate::commands::sync::CliCluster]) -> Result<String> {
    if clusters.len() == 1 {
        return Ok(clusters[0].cluster_id.clone());
    }

    let mut select = cliclack::select("Select a cluster");
    for c in clusters {
        select = select.item(
            c.cluster_id.clone(),
            c.cluster_name.as_str(),
            c.project_name.as_str(),
        );
    }
    let selected = select.interact()?;
    Ok(selected)
}
