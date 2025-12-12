use crate::cleanup::CleanupTracker;
use crate::CloudDeploymentTypeCommand;
use crate::commands::integrations::ecr::{EcrAuthType, EcrManager};
use crate::commands::integrations::fly::{FlyAuthType, FlyManager, VmSize};
use crate::commands::integrations::helix::HelixManager;
use crate::config::{BuildMode, CloudConfig, DbConfig, LocalInstanceConfig};
use crate::docker::DockerManager;
use crate::errors::project_error;
use crate::project::ProjectContext;
use crate::utils::{print_instructions, print_status, print_success};
use eyre::Result;
use std::env;

pub async fn run(deployment_type: CloudDeploymentTypeCommand) -> Result<()> {
    let mut cleanup_tracker = CleanupTracker::new();

    // Execute the add logic, capturing any errors
    let result = run_add_inner(deployment_type, &mut cleanup_tracker).await;

    // If there was an error, perform cleanup
    if let Err(ref e) = result
        && cleanup_tracker.has_tracked_resources() {
            eprintln!("Add failed, performing cleanup: {}", e);
            let summary = cleanup_tracker.cleanup();
            summary.log_summary();
        }

    result
}

async fn run_add_inner(
    deployment_type: CloudDeploymentTypeCommand,
    cleanup_tracker: &mut CleanupTracker,
) -> Result<()> {
    let cwd = env::current_dir()?;
    let mut project_context = ProjectContext::find_and_load(Some(&cwd))?;

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

    print_status(
        "ADD",
        &format!("Adding instance '{instance_name}' to Helix project"),
    );

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

            print_status("CLOUD", "Helix cloud instance configuration added");

            // Prompt user to create cluster now
            println!();
            println!("\nWould you like to create the cluster now?");
            println!("This will open Stripe for payment and provision your cluster.");
            println!();
            print!("Create cluster now? [Y/n]: ");
            use std::io::{self, Write};
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim().to_lowercase();

            if input.is_empty() || input == "y" || input == "yes" {
                // Run create-cluster flow
                crate::commands::create_cluster::run(&instance_name, region).await?;

                // create_cluster::run() already saved the updated config with the real cluster_id
                // Return early to avoid overwriting it with the stale in-memory config
                print_success(&format!(
                    "Instance '{instance_name}' added to Helix project"
                ));

                print_instructions(
                    "Next steps:",
                    &[
                        &format!("Run 'helix build {instance_name}' to compile your project for this instance"),
                        &format!("Run 'helix push {instance_name}' to start the '{instance_name}' instance"),
                    ],
                );

                return Ok(());
            } else {
                println!();
                print_status(
                    "INFO",
                    &format!("Cluster creation skipped. Run 'helix create-cluster {}' when ready.", instance_name)
                );
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

            print_status("ECR", "AWS ECR repository initialized successfully");
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
                build_mode: BuildMode::Debug,
                db_config: DbConfig::default(),
                queries: None,
            };

            project_context
                .config
                .local
                .insert(instance_name.clone(), local_config);
            print_status("LOCAL", "Local instance configuration added");
        }
    }

    // Save the updated configuration
    let config_path = project_context.root.join("helix.toml");
    project_context.config.save_to_file(&config_path)?;

    print_success(&format!(
        "Instance '{instance_name}' added to Helix project"
    ));

    print_instructions(
        "Next steps:",
        &[
            &format!("Run 'helix build {instance_name}' to compile your project for this instance"),
            &format!("Run 'helix push {instance_name}' to start the '{instance_name}' instance"),
        ],
    );

    Ok(())
}
