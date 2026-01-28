use crate::CloudDeploymentTypeCommand;
use crate::cleanup::CleanupTracker;
use crate::commands::integrations::ecr::{EcrAuthType, EcrManager};
use crate::commands::integrations::fly::{FlyAuthType, FlyManager, VmSize};
use crate::commands::integrations::helix::HelixManager;
use crate::config::{CloudConfig, HelixConfig};
use crate::docker::DockerManager;
use crate::errors::project_error;
use crate::project::ProjectContext;
use crate::prompts;
use crate::utils::{print_instructions, print_status, print_success};
use eyre::Result;
use std::env;
use std::fs;
use std::path::Path;

pub async fn run(
    path: Option<String>,
    _template: String,
    queries_path: String,
    deployment_type: Option<CloudDeploymentTypeCommand>,
) -> Result<()> {
    let mut cleanup_tracker = CleanupTracker::new();

    // Execute the init logic, capturing any errors
    let result = run_init_inner(
        path,
        _template,
        queries_path,
        deployment_type,
        &mut cleanup_tracker,
    )
    .await;

    // If there was an error, perform cleanup
    if let Err(ref e) = result
        && cleanup_tracker.has_tracked_resources()
    {
        eprintln!("Init failed, performing cleanup: {}", e);
        let summary = cleanup_tracker.cleanup();
        summary.log_summary();
    }

    result
}

async fn run_init_inner(
    path: Option<String>,
    _template: String,
    queries_path: String,
    deployment_type: Option<CloudDeploymentTypeCommand>,
    cleanup_tracker: &mut CleanupTracker,
) -> Result<()> {
    let project_dir = match path {
        Some(p) => std::path::PathBuf::from(p),
        None => env::current_dir()?,
    };

    let project_name = project_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("helix-project");

    let config_path = project_dir.join("helix.toml");

    if config_path.exists() {
        return Err(project_error(format!(
            "helix.toml already exists in {}",
            project_dir.display()
        ))
        .with_hint("use 'helix add <instance_name>' to add a new instance to the existing project")
        .into());
    }

    print_status(
        "INIT",
        &format!("Initializing Helix project: {project_name}"),
    );

    // Create project directory if it doesn't exist
    let project_dir_existed = project_dir.exists();
    fs::create_dir_all(&project_dir)?;
    if !project_dir_existed {
        cleanup_tracker.track_dir(project_dir.clone());
    }

    // Create default helix.toml with custom queries path
    let mut config = HelixConfig::default_config(project_name);
    config.project.queries = std::path::PathBuf::from(&queries_path);

    // Save initial config and track it
    config.save_to_file(&config_path)?;
    cleanup_tracker.track_file(config_path.clone());

    // Create project structure
    create_project_structure(&project_dir, &queries_path, cleanup_tracker)?;

    // Initialize deployment type based on flags or interactive selection
    // If no deployment type provided and we're in an interactive terminal, prompt the user
    let deployment_type = if deployment_type.is_none() && prompts::is_interactive() {
        prompts::intro(
            "helix init",
            Some(
                "This will create a new Helix project in the current directory.\nYou can configure the project type, name and other settings below.",
            ),
        )?;

        prompts::build_init_deployment_command().await?
    } else {
        deployment_type
    };

    match deployment_type {
        Some(deployment) => {
            match deployment {
                CloudDeploymentTypeCommand::Helix { region, .. } => {
                    // Initialize Helix deployment
                    let cwd = env::current_dir()?;
                    let project_context = ProjectContext::find_and_load(Some(&cwd))?;

                    // Create Helix manager
                    let helix_manager = HelixManager::new(&project_context);

                    // Create cloud instance configuration (without cluster_id yet)
                    let cloud_config = helix_manager
                        .create_instance_config(project_name, region.clone())
                        .await?;

                    // Insert into config first
                    config.cloud.insert(
                        project_name.to_string(),
                        CloudConfig::Helix(cloud_config.clone()),
                    );

                    // Backup config before saving
                    cleanup_tracker.backup_config(&config, config_path.clone());

                    // Save config
                    config.save_to_file(&config_path)?;

                    // Prompt user to create cluster now
                    println!();
                    print_status("CLUSTER", "Helix Cloud instance configuration saved");
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
                        crate::commands::create_cluster::run(project_name, region).await?;
                    } else {
                        println!();
                        print_status(
                            "INFO",
                            &format!(
                                "Cluster creation skipped. Run 'helix create-cluster {}' when ready.",
                                project_name
                            ),
                        );
                    }
                }
                CloudDeploymentTypeCommand::Ecr { .. } => {
                    let cwd = env::current_dir()?;
                    let project_context = ProjectContext::find_and_load(Some(&cwd))?;

                    // Create ECR manager
                    let ecr_manager =
                        EcrManager::new(&project_context, EcrAuthType::AwsCli).await?;

                    // Create ECR configuration
                    let ecr_config = ecr_manager
                        .create_ecr_config(
                            project_name,
                            None, // Use default region
                            EcrAuthType::AwsCli,
                        )
                        .await?;

                    // Initialize the ECR repository
                    ecr_manager
                        .init_repository(project_name, &ecr_config)
                        .await?;

                    // Save configuration to ecr.toml
                    ecr_manager.save_config(project_name, &ecr_config).await?;

                    // Update helix.toml with cloud config
                    config.cloud.insert(
                        project_name.to_string(),
                        CloudConfig::Ecr(ecr_config.clone()),
                    );

                    // Backup config before saving
                    cleanup_tracker.backup_config(&config, config_path.clone());

                    config.save_to_file(&config_path)?;

                    print_status("ECR", "AWS ECR repository initialized successfully");
                }
                CloudDeploymentTypeCommand::Fly {
                    auth,
                    volume_size,
                    vm_size,
                    private,
                    ..
                } => {
                    let cwd = env::current_dir()?;
                    let project_context = ProjectContext::find_and_load(Some(&cwd))?;
                    let docker = DockerManager::new(&project_context);

                    // Parse configuration with proper error handling
                    let auth_type = FlyAuthType::try_from(auth)?;

                    // Parse vm_size directly using match statement to avoid trait conflicts
                    let vm_size_parsed = VmSize::try_from(vm_size)?;

                    // Create Fly.io manager
                    let fly_manager = FlyManager::new(&project_context, auth_type.clone()).await?;
                    // Create instance configuration
                    let instance_config = fly_manager.create_instance_config(
                        &docker,
                        project_name, // Use "default" as the instance name for init
                        volume_size,
                        vm_size_parsed,
                        private,
                        auth_type,
                    );

                    // Initialize the Fly.io app
                    fly_manager.init_app(project_name, &instance_config).await?;

                    config.cloud.insert(
                        project_name.to_string(),
                        CloudConfig::FlyIo(instance_config.clone()),
                    );

                    // Backup config before saving
                    cleanup_tracker.backup_config(&config, config_path.clone());

                    config.save_to_file(&config_path)?;
                }
                _ => {}
            }
        }
        None => {
            // Local instance is the default, config already saved above
        }
    }

    print_success(&format!(
        "Helix project initialized in {}",
        project_dir.display()
    ));
    let queries_path_clean = queries_path.trim_end_matches('/');
    print_instructions(
        "Next steps:",
        &[
            &format!("Edit {queries_path_clean}/schema.hx to define your data model"),
            &format!("Add queries to {queries_path_clean}/queries.hx"),
            "Run 'helix push dev' to start your development instance",
        ],
    );

    Ok(())
}

fn create_project_structure(
    project_dir: &Path,
    queries_path: &str,
    cleanup_tracker: &mut CleanupTracker,
) -> Result<()> {
    // Create directories
    let helix_dir = project_dir.join(".helix");
    fs::create_dir_all(&helix_dir)?;
    cleanup_tracker.track_dir(helix_dir);

    let queries_dir = project_dir.join(queries_path);
    fs::create_dir_all(&queries_dir)?;
    cleanup_tracker.track_dir(queries_dir);

    // Create default schema.hx with proper Helix syntax
    let default_schema = r#"// Start building your schema here.
//
// The schema is used to to ensure a level of type safety in your queries.
//
// The schema is made up of Node types, denoted by N::,
// and Edge types, denoted by E::
//
// Under the Node types you can define fields that
// will be stored in the database.
//
// Under the Edge types you can define what type of node
// the edge will connect to and from, and also the
// properties that you want to store on the edge.
//
// Example:
//
// N::User {
//     Name: String,
//     Label: String,
//     Age: I64,
//     IsAdmin: Boolean,
// }
//
// E::Knows {
//     From: User,
//     To: User,
//     Properties: {
//         Since: I64,
//     }
// }
"#;
    let schema_path = project_dir.join(queries_path).join("schema.hx");
    fs::write(&schema_path, default_schema)?;
    cleanup_tracker.track_file(schema_path);

    // Create default queries.hx with proper Helix query syntax in the queries directory
    let default_queries = r#"// Start writing your queries here.
//
// You can use the schema to help you write your queries.
//
// Queries take the form:
//     QUERY {query name}({input name}: {input type}) =>
//         {variable} <- {traversal}
//         RETURN {variable}
//
// Example:
//     QUERY GetUserFriends(user_id: String) =>
//         friends <- N<User>(user_id)::Out<Knows>
//         RETURN friends
//
//
// For more information on how to write queries,
// see the documentation at https://docs.helix-db.com
// or checkout our GitHub at https://github.com/himanalot/helix-db
"#;
    let queries_path_file = project_dir.join(queries_path).join("queries.hx");
    fs::write(&queries_path_file, default_queries)?;
    cleanup_tracker.track_file(queries_path_file);

    // Create .gitignore
    let gitignore = r#".helix/
target/
*.log
"#;
    let gitignore_path = project_dir.join(".gitignore");
    fs::write(&gitignore_path, gitignore)?;
    cleanup_tracker.track_file(gitignore_path);

    Ok(())
}
