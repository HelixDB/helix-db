//! Interactive prompts for the Helix CLI using cliclack.
//!
//! This module provides a consistent, user-friendly interactive experience
//! for commands like `init` and `add` when flags are not provided.

use crate::CloudDeploymentTypeCommand;
use crate::commands::auth::require_auth;
use crate::commands::feedback::FeedbackType;
use crate::commands::integrations::fly::VmSize;
use eyre::Result;
use std::path::Path;

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


pub fn confirm_overwrite(path: &Path) -> Result<bool> {
    confirm(&format!("File '{}' exists. Overwrite it?", path.display()))
}

pub fn confirm_generate_ai_setup() -> Result<bool> {
    confirm("Generate AI setup files now?")
}

pub fn confirm_copy_ai_prompt() -> Result<bool> {
    confirm("Copy AI setup prompt to clipboard?")
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

/// Prompt user to select a project from a list
pub fn select_project(projects: &[(String, String)]) -> Result<String> {
    if projects.is_empty() {
        return Err(eyre::eyre!("No projects found in this workspace"));
    }

    if projects.len() == 1 {
        return Ok(projects[0].0.clone());
    }

    let mut select = cliclack::select("Which project would you like to use?");
    for (id, name) in projects {
        let short_id = if id.len() > 8 { &id[..8] } else { id.as_str() };
        let hint = format!("id: {}", short_id);
        select = select.item(id.clone(), name.as_str(), hint.as_str());
    }

    let selected = select.interact()?;
    Ok(selected)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingProjectChoice {
    Create,
    ChooseExisting,
}

pub fn select_missing_project_choice(project_name: &str) -> Result<MissingProjectChoice> {
    let prompt = format!(
        "Project '{}' was not found. Would you like to create it or choose an existing project?",
        project_name
    );

    let selected: &str = cliclack::select(prompt)
        .item("create", "create", "Create a new project")
        .item(
            "choose_existing",
            "choose existing",
            "Pick an existing project",
        )
        .interact()?;

    Ok(match selected {
        "create" => MissingProjectChoice::Create,
        _ => MissingProjectChoice::ChooseExisting,
    })
}

/// Prompt user to input a project name
pub fn input_project_name(default: &str) -> Result<String> {
    let name: String = cliclack::input("Project name")
        .default_input(default)
        .placeholder(default)
        .validate(|input: &String| {
            if input.is_empty() {
                Err("Project name cannot be empty")
            } else if input.len() > 64 {
                Err("Project name must be 64 characters or less")
            } else {
                Ok(())
            }
        })
        .interact()?;
    Ok(name)
}

/// Prompt user to input a cluster name
pub fn input_cluster_name(default: &str) -> Result<String> {
    let name: String = cliclack::input("Cluster name")
        .default_input(default)
        .placeholder(default)
        .validate(|input: &String| {
            if input.is_empty() {
                Err("Cluster name cannot be empty")
            } else if input.len() > 32 {
                Err("Cluster name must be 32 characters or less")
            } else if !input
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            {
                Err("Cluster name can only contain letters, numbers, hyphens, and underscores")
            } else {
                Ok(())
            }
        })
        .interact()?;
    Ok(name)
}

/// Prompt user to select cluster type (standard vs enterprise)
pub fn select_cluster_type() -> Result<&'static str> {
    let selected: &str = cliclack::select("Select cluster type")
        .item(
            "standard",
            "Standard",
            "Railway-based cluster for .hx queries",
        )
        .item(
            "enterprise",
            "Object Storage (Enterprise)",
            "Enterprise cluster for .rs source files",
        )
        .interact()?;
    Ok(selected)
}

/// Prompt user to select build mode
pub fn select_build_mode() -> Result<crate::config::BuildMode> {
    let selected: crate::config::BuildMode = cliclack::select("Select build mode")
        .item(
            crate::config::BuildMode::Dev,
            "dev",
            "Fast builds, no optimizations",
        )
        .item(
            crate::config::BuildMode::Release,
            "release",
            "Optimized builds for production",
        )
        .interact()?;
    Ok(selected)
}

/// Prompt user to select availability mode for enterprise clusters
pub fn select_availability_mode() -> Result<crate::config::AvailabilityMode> {
    let selected: crate::config::AvailabilityMode = cliclack::select("Select availability mode")
        .item(
            crate::config::AvailabilityMode::Dev,
            "Dev",
            "Single instance, lower cost",
        )
        .item(
            crate::config::AvailabilityMode::Ha,
            "HA (High Availability)",
            "Multi-instance, auto-scaling",
        )
        .interact()?;
    Ok(selected)
}

/// Prompt user to select gateway node type
pub fn select_gateway_node_type(is_ha: bool) -> Result<String> {
    if is_ha {
        let selected: String = cliclack::select("Select gateway node type")
            .item("GW-40".to_string(), "GW-40", "40 GB RAM")
            .item("GW-80".to_string(), "GW-80", "80 GB RAM")
            .item("GW-160".to_string(), "GW-160", "160 GB RAM")
            .interact()?;
        Ok(selected)
    } else {
        let selected: String = cliclack::select("Select gateway node type")
            .item("GW-20".to_string(), "GW-20", "20 GB RAM")
            .item("GW-80".to_string(), "GW-80", "80 GB RAM")
            .interact()?;
        Ok(selected)
    }
}

/// Prompt user to select DB node type
pub fn select_db_node_type(is_ha: bool) -> Result<String> {
    if is_ha {
        let selected: String = cliclack::select("Select DB node type")
            .item("HLX-160".to_string(), "HLX-160", "160 GB")
            .item("HLX-320".to_string(), "HLX-320", "320 GB")
            .item("HLX-640".to_string(), "HLX-640", "640 GB")
            .item("HLX-1280".to_string(), "HLX-1280", "1280 GB")
            .interact()?;
        Ok(selected)
    } else {
        let selected: String = cliclack::select("Select DB node type")
            .item("HLX-40".to_string(), "HLX-40", "40 GB")
            .item("HLX-80".to_string(), "HLX-80", "80 GB")
            .item("HLX-160".to_string(), "HLX-160", "160 GB")
            .item("HLX-320".to_string(), "HLX-320", "320 GB")
            .interact()?;
        Ok(selected)
    }
}

/// Prompt user to input min instances for HA mode
pub fn input_min_instances() -> Result<u64> {
    let val: String = cliclack::input("Minimum instances")
        .default_input("3")
        .placeholder("3")
        .validate(|input: &String| match input.parse::<u64>() {
            Ok(n) if (3..=100).contains(&n) => Ok(()),
            Ok(_) => Err("Must be between 3 and 100"),
            Err(_) => Err("Please enter a valid number"),
        })
        .interact()?;
    Ok(val.parse().unwrap_or(3))
}

/// Prompt user to input max instances for HA mode
pub fn input_max_instances(min: u64) -> Result<u64> {
    let default = min.to_string();
    let val: String = cliclack::input("Maximum instances")
        .default_input(&default)
        .placeholder(&default)
        .validate(move |input: &String| match input.parse::<u64>() {
            Ok(n) if n >= min && n <= 100 => Ok(()),
            Ok(_) => Err("Must be between min instances and 100"),
            Err(_) => Err("Please enter a valid number"),
        })
        .interact()?;
    Ok(val.parse().unwrap_or(min))
}

/// Prompt user to select a cluster from a list (standard or enterprise)
pub fn select_cluster_from_workspace(
    standard: &[(String, String, String)],
    enterprise: &[(String, String, String)],
) -> Result<(String, bool)> {
    // Build combined list: (cluster_id, display_name, is_enterprise)
    let mut items: Vec<(String, String, String, bool)> = Vec::new();

    for (id, name, project) in standard {
        items.push((
            id.clone(),
            name.clone(),
            format!("{} (Standard)", project),
            false,
        ));
    }
    for (id, name, project) in enterprise {
        items.push((
            id.clone(),
            name.clone(),
            format!("{} (Enterprise)", project),
            true,
        ));
    }

    if items.is_empty() {
        return Err(eyre::eyre!("No clusters found"));
    }

    if items.len() == 1 {
        return Ok((items[0].0.clone(), items[0].3));
    }

    // Use index as value since we need to return both cluster_id and is_enterprise
    let mut select = cliclack::select("Select a cluster");
    for (i, (_, name, hint, _)) in items.iter().enumerate() {
        select = select.item(i, name.as_str(), hint.as_str());
    }
    let idx = select.interact()?;
    Ok((items[idx].0.clone(), items[idx].3))
}
