use crate::CloudDeploymentTypeCommand;
use crate::cleanup::CleanupTracker;
use crate::commands::integrations::ecr::{EcrAuthType, EcrManager};
use crate::commands::integrations::fly::{FlyAuthType, FlyManager, VmSize};
use crate::commands::integrations::helix::HelixManager;
use crate::config::{BuildMode, CloudConfig, DbConfig, LocalInstanceConfig};
use crate::docker::DockerManager;
use crate::errors::project_error;
use crate::output::{Operation, Step};
use crate::project::ProjectContext;
use crate::prompts;
use crate::utils::print_instructions;
use eyre::Result;
use std::env;

pub async fn run(deployment_type: Option<CloudDeploymentTypeCommand>) -> Result<()> {
    let mut cleanup_tracker = CleanupTracker::new();

    // Load project context first to get the project name for interactive prompts
    let cwd = env::current_dir()?;
    let project_context = ProjectContext::find_and_load(Some(&cwd))?;
    let project_name = &project_context.config.project.name;

    // If no deployment type provided and we're in an interactive terminal, prompt the user
    let deployment_type = match deployment_type {
        Some(dt) => dt,
        None if prompts::is_interactive() => {
            prompts::intro(
                "helix add",
                Some(
                    "This will add a new instance to the Helix project.\nYou can configure the instance type, name and other settings below.\n",
                ),
            )?;
            match prompts::build_deployment_command(project_name).await? {
                Some(dt) => dt,
                None => {
                    // User selected Local but didn't provide a name
                    CloudDeploymentTypeCommand::Local { name: None }
                }
            }
        }
        None => {
            return Err(eyre::eyre!(
                "No deployment type specified. Run 'helix add' in an interactive terminal or specify a deployment type:\n  \
                helix add local\n  \
                helix add cloud\n  \
                helix add ecr\n  \
                helix add fly"
            ));
        }
    };

    // Execute the add logic, capturing any errors
    let result = run_add_inner(deployment_type, project_context, &mut cleanup_tracker).await;

    // If there was an error, perform cleanup
    if let Err(ref e) = result
        && cleanup_tracker.has_tracked_resources()
    {
        eprintln!("Add failed, performing cleanup: {}", e);
        let summary = cleanup_tracker.cleanup();
        summary.log_summary();
    }

    result
}

async fn run_add_inner(
    deployment_type: CloudDeploymentTypeCommand,
    mut project_context: ProjectContext,
    cleanup_tracker: &mut CleanupTracker,
) -> Result<()> {
    let instance_name = deployment_type
        .name()
        .unwrap_or(project_context.config.project.name.clone());

    // Check if instance already exists
    if project_context.config.local.contains_key(&instance_name)
        || project_context.config.cloud.contains_key(&instance_name)
    {
        return Err(project_error(format!(
            "Instance '{instance_name}' already exists in helix.toml"
        ))
        .with_hint("use a different instance name or remove the existing instance")
        .into());
    }

    let op = Operation::new("Adding", &instance_name);

    // Backup the original config before any modifications
    let config_path = project_context.root.join("helix.toml");
    cleanup_tracker.backup_config(&project_context.config, config_path.clone());

    // Determine instance type

    match deployment_type {
        CloudDeploymentTypeCommand::Helix { region, .. } => {
            // Add Helix cloud instance
            let helix_manager = HelixManager::new(&project_context);

            // Create cloud instance configuration (without cluster_id yet)
            let cloud_config = helix_manager
                .create_instance_config(&instance_name, region.clone())
                .await?;

            // Insert into project configuration
            project_context.config.cloud.insert(
                instance_name.clone(),
                CloudConfig::Helix(cloud_config.clone()),
            );

            // Save config first
            let config_path = project_context.root.join("helix.toml");
            project_context.config.save_to_file(&config_path)?;

            Step::verbose_substep("Helix cloud instance configuration added");

            // Prompt user to create cluster now
            println!();
            println!("This will open Stripe for payment and provision your cluster.");

            let should_create = if prompts::is_interactive() {
                prompts::confirm("Create cluster now?")?
            } else {
                // Fallback to raw stdin for non-interactive terminals
                use std::io::{self, Write};
                print!("Create cluster now? [Y/n]: ");
                io::stdout().flush()?;
                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                let input = input.trim().to_lowercase();
                input.is_empty() || input == "y" || input == "yes"
            };

            if should_create {
                // Run create-cluster flow
                crate::commands::create_cluster::run(&instance_name, region).await?;

                // create_cluster::run() already saved the updated config with the real cluster_id
                // Return early to avoid overwriting it with the stale in-memory config
                op.success();

                print_instructions(
                    "Next steps:",
                    &[
                        &format!(
                            "Run 'helix build {instance_name}' to compile your project for this instance"
                        ),
                        &format!(
                            "Run 'helix push {instance_name}' to start the '{instance_name}' instance"
                        ),
                    ],
                );

                return Ok(());
            } else {
                println!();
                crate::output::info(&format!(
                    "Cluster creation skipped. Run 'helix create-cluster {}' when ready.",
                    instance_name
                ));
            }
        }
        CloudDeploymentTypeCommand::Ecr { .. } => {
            // Add ECR instance
            // Create ECR manager
            let ecr_manager = EcrManager::new(&project_context, EcrAuthType::AwsCli).await?;

            // Create ECR configuration
            let ecr_config = ecr_manager
                .create_ecr_config(
                    &instance_name,
                    None, // Use default region
                    EcrAuthType::AwsCli,
                )
                .await?;

            // Initialize the ECR repository
            ecr_manager
                .init_repository(&instance_name, &ecr_config)
                .await?;

            // Save configuration to ecr.toml
            ecr_manager.save_config(&instance_name, &ecr_config).await?;

            // Update helix.toml with cloud config
            project_context
                .config
                .cloud
                .insert(instance_name.clone(), CloudConfig::Ecr(ecr_config.clone()));

            Step::verbose_substep("AWS ECR repository initialized successfully");
        }
        CloudDeploymentTypeCommand::Fly {
            auth,
            volume_size,
            vm_size,
            private,
            ..
        } => {
            let docker = DockerManager::new(&project_context);

            // Parse configuration with proper error handling
            let auth_type = FlyAuthType::try_from(auth)?;
            let vm_size_parsed = VmSize::try_from(vm_size)?;

            // Create Fly.io manager
            let fly_manager = FlyManager::new(&project_context, auth_type.clone()).await?;

            // Create instance configuration
            let instance_config = fly_manager.create_instance_config(
                &docker,
                &instance_name,
                volume_size,
                vm_size_parsed,
                private,
                auth_type,
            );

            // Initialize the Fly.io app
            fly_manager
                .init_app(&instance_name, &instance_config)
                .await?;

            project_context.config.cloud.insert(
                instance_name.clone(),
                CloudConfig::FlyIo(instance_config.clone()),
            );
        }
        _ => {
            // Add local instance with default configuration
            let local_config = LocalInstanceConfig {
                port: None, // Let the system assign a port
                build_mode: BuildMode::Dev,
                db_config: DbConfig::default(),
            };

            project_context
                .config
                .local
                .insert(instance_name.clone(), local_config);
            Step::verbose_substep("Local instance configuration added");
        }
    }

    // Save the updated configuration
    let config_path = project_context.root.join("helix.toml");
    project_context.config.save_to_file(&config_path)?;

    op.success();

    print_instructions(
        "Next steps:",
        &[
            &format!("Run 'helix build {instance_name}' to compile your project for this instance"),
            &format!("Run 'helix push {instance_name}' to start the '{instance_name}' instance"),
        ],
    );

    Ok(())
}
