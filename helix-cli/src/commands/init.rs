use crate::CloudDeploymentTypeCommand;
use crate::cleanup::CleanupTracker;
use crate::commands::integrations::ecr::{EcrAuthType, EcrManager};
use crate::commands::integrations::fly::{FlyAuthType, FlyManager, VmSize};
use crate::commands::workspace_flow::{self, ClusterResult};
use crate::config::{
    CloudConfig, CloudInstanceConfig, DbConfig, EnterpriseInstanceConfig, HelixConfig,
    LocalInstanceConfig,
};
use crate::docker::DockerManager;
use crate::errors::project_error;
use crate::output::{Operation, Step};
use crate::project::ProjectContext;
use crate::prompts;
use crate::utils::print_instructions;
use eyre::Result;
use std::collections::HashMap;
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

    let op = Operation::new("Initializing", project_name);

    // Create project directory if it doesn't exist
    let project_dir_existed = project_dir.exists();
    fs::create_dir_all(&project_dir)?;
    if !project_dir_existed {
        cleanup_tracker.track_dir(project_dir.clone());
    }

    let interactive = prompts::is_interactive();

    // Create default helix.toml with custom queries path
    let mut config = HelixConfig::default_config(project_name);
    config.project.queries = std::path::PathBuf::from(&queries_path);

    let mut local_instance_name = "dev".to_string();
    let mut deployment_instance_name: Option<String> = None;
    let mut is_remote_init = false;

    // Save initial config and track it
    config.save_to_file(&config_path)?;
    cleanup_tracker.track_file(config_path.clone());

    // Create project structure
    create_project_structure(&project_dir, &queries_path, interactive, cleanup_tracker)?;

    // Initialize deployment type based on flags or interactive selection
    // If no deployment type provided and we're in an interactive terminal, prompt the user
    let deployment_type = if deployment_type.is_none() && interactive {
        prompts::intro(
            "helix init",
            Some(
                "This will create a new Helix project in the current directory.\nYou can configure the project type, name and other settings below.",
            ),
        )?;

        prompts::build_init_deployment_command(project_name).await?
    } else {
        deployment_type
    };

    match deployment_type.clone() {
        Some(deployment) => {
            match deployment {
                CloudDeploymentTypeCommand::Helix { name, .. } => {
                    is_remote_init = true;

                    // Authenticate and run workspace/project/cluster flow
                    let credentials = crate::commands::auth::require_auth().await?;
                    let result = workspace_flow::run_workspace_project_cluster_flow(
                        project_name,
                        config.project.id.as_deref(),
                        &credentials,
                        name.as_deref(),
                    )
                    .await?;

                    config.project.name = result.resolved_project_name;
                    config.project.id = Some(result.resolved_project_id);

                    // Backup config before saving
                    cleanup_tracker.backup_config(&config, config_path.clone());

                    match result.cluster {
                        ClusterResult::Standard(std_result) => {
                            deployment_instance_name = Some(std_result.instance_name.clone());
                            let cloud_config = CloudInstanceConfig {
                                cluster_id: std_result.cluster_id,
                                region: Some("us-east-1".to_string()),
                                build_mode: std_result.build_mode,
                                env_vars: HashMap::new(),
                                db_config: DbConfig::default(),
                            };
                            config
                                .cloud
                                .insert(std_result.instance_name, CloudConfig::Helix(cloud_config));
                        }
                        ClusterResult::Enterprise(ent_result) => {
                            deployment_instance_name = Some(ent_result.instance_name.clone());
                            let enterprise_config = EnterpriseInstanceConfig {
                                cluster_id: ent_result.cluster_id,
                                availability_mode: ent_result.availability_mode,
                                gateway_node_type: ent_result.gateway_node_type,
                                db_node_type: ent_result.db_node_type,
                                min_instances: ent_result.min_instances,
                                max_instances: ent_result.max_instances,
                                db_config: DbConfig::default(),
                            };
                            config
                                .enterprise
                                .insert(ent_result.instance_name, enterprise_config);
                        }
                    }

                    config.save_to_file(&config_path)?;
                    Step::verbose_substep("Helix Cloud configuration saved to helix.toml");
                }
                CloudDeploymentTypeCommand::Ecr { name } => {
                    is_remote_init = true;
                    let instance_name = name.unwrap_or_else(|| project_name.to_string());
                    deployment_instance_name = Some(instance_name.clone());

                    let project_context = ProjectContext::find_and_load(Some(&project_dir))?;

                    // Create ECR manager
                    let ecr_manager =
                        EcrManager::new(&project_context, EcrAuthType::AwsCli).await?;

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
                    config
                        .cloud
                        .insert(instance_name, CloudConfig::Ecr(ecr_config.clone()));

                    // Backup config before saving
                    cleanup_tracker.backup_config(&config, config_path.clone());

                    config.save_to_file(&config_path)?;

                    Step::verbose_substep("AWS ECR repository initialized successfully");
                }
                CloudDeploymentTypeCommand::Fly {
                    auth,
                    volume_size,
                    vm_size,
                    private,
                    name,
                } => {
                    is_remote_init = true;
                    let instance_name = name.unwrap_or_else(|| project_name.to_string());
                    deployment_instance_name = Some(instance_name.clone());

                    let project_context = ProjectContext::find_and_load(Some(&project_dir))?;
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

                    config
                        .cloud
                        .insert(instance_name, CloudConfig::FlyIo(instance_config.clone()));

                    // Backup config before saving
                    cleanup_tracker.backup_config(&config, config_path.clone());

                    config.save_to_file(&config_path)?;
                }
                CloudDeploymentTypeCommand::Local { name } => {
                    local_instance_name = name.unwrap_or_else(|| "dev".to_string());

                    if local_instance_name != "dev" {
                        let local_cfg = config.local.remove("dev").unwrap_or(LocalInstanceConfig {
                            port: Some(6969),
                            build_mode: crate::config::BuildMode::Dev,
                            db_config: DbConfig::default(),
                        });
                        config.local.insert(local_instance_name.clone(), local_cfg);

                        config.save_to_file(&config_path)?;
                    }
                }
            }
        }
        None => {
            // Local instance is the default, config already saved above
        }
    }

    op.success();
    let queries_path_clean = queries_path.trim_end_matches('/');

    let target_instance = deployment_instance_name
        .clone()
        .unwrap_or_else(|| local_instance_name.clone());

    let mut next_steps = vec![
        format!("Edit {queries_path_clean}/schema.hx to define your data model"),
        format!("Add queries to {queries_path_clean}/queries.hx"),
        format!(
            "Run 'helix push {target_instance}' to {}",
            if is_remote_init {
                "deploy your configured instance"
            } else {
                "start your development instance"
            }
        ),
    ];

    if is_remote_init {
        next_steps.push(format!(
            "Use 'helix logs {target_instance}' to verify deployment output"
        ));
    }

    let next_step_refs: Vec<&str> = next_steps.iter().map(String::as_str).collect();
    print_instructions("Next steps:", &next_step_refs);

    Ok(())
}

fn create_project_structure(
    project_dir: &Path,
    queries_path: &str,
    interactive: bool,
    cleanup_tracker: &mut CleanupTracker,
) -> Result<()> {
    // Create directories
    let helix_dir = project_dir.join(".helix");
    let helix_dir_existed = helix_dir.exists();
    fs::create_dir_all(&helix_dir)?;
    if !helix_dir_existed {
        cleanup_tracker.track_dir(helix_dir);
    }

    let queries_dir = project_dir.join(queries_path);
    let queries_dir_existed = queries_dir.exists();
    fs::create_dir_all(&queries_dir)?;
    if !queries_dir_existed {
        cleanup_tracker.track_dir(queries_dir);
    }

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
    write_starter_file(&schema_path, default_schema, interactive, cleanup_tracker)?;

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
// or checkout our GitHub at https://github.com/HelixDB/helix-db
"#;
    let queries_path_file = project_dir.join(queries_path).join("queries.hx");
    write_starter_file(
        &queries_path_file,
        default_queries,
        interactive,
        cleanup_tracker,
    )?;

    // Create .gitignore
    let gitignore = r#".helix/
target/
*.log
"#;
    let gitignore_path = project_dir.join(".gitignore");
    write_starter_file(&gitignore_path, gitignore, interactive, cleanup_tracker)?;

    Ok(())
}

fn write_starter_file(
    path: &Path,
    content: &str,
    interactive: bool,
    cleanup_tracker: &mut CleanupTracker,
) -> Result<()> {
    if path.exists() {
        let should_overwrite = if interactive {
            prompts::confirm_overwrite(path)?
        } else {
            false
        };

        if !should_overwrite {
            crate::output::warning(&format!("Skipping existing file: {}", path.display()));
            return Ok(());
        }
    }

    let should_track = !path.exists();
    fs::write(path, content)?;
    if should_track {
        cleanup_tracker.track_file(path.to_path_buf());
    }
    Ok(())
}
