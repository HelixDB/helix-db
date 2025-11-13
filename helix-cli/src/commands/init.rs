use crate::CloudDeploymentTypeCommand;
use crate::commands::integrations::ecr::{EcrAuthType, EcrManager};
use crate::commands::integrations::fly::{FlyAuthType, FlyManager, VmSize};
use crate::commands::integrations::helix::HelixManager;
use crate::commands::templates::{TemplateFetcher, TemplateProcessor, TemplateSource};
use crate::config::{CloudConfig, HelixConfig};
use crate::docker::DockerManager;
use crate::errors::project_error;
use crate::project::ProjectContext;
use crate::utils::{print_instructions, print_status, print_success};
use eyre::Result;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

pub async fn run(
    path: Option<String>,
    template: Option<String>,
    queries_path: String,
    deployment_type: Option<CloudDeploymentTypeCommand>,
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
    fs::create_dir_all(&project_dir)?;

    // Process template if provided
    if let Some(template_spec) = template {
        process_template(&template_spec, &project_dir, project_name)?;

        print_success(&format!(
            "Helix project successfully initialized from template: `{template_spec}`",
        ));

        print_instructions(
            "Next steps:",
            &[
                &format!(
                    "1. Explore your project files in `{}`",
                    project_dir.display()
                ),
                "2. Run `helix build dev` to compile your project",
                "3. Run `helix push dev` to start your development instance",
            ],
        );

        return Ok(());
    }

    // Create default helix.toml with custom queries path
    let mut config = HelixConfig::default_config(project_name);
    config.project.queries = std::path::PathBuf::from(&queries_path);
    config.save_to_file(&config_path)?;
    // Create project structure
    create_project_structure(&project_dir, &queries_path)?;

    // Initialize deployment type based on flags
    match deployment_type {
        Some(deployment) => {
            match deployment {
                CloudDeploymentTypeCommand::Helix { region, .. } => {
                    // Initialize Helix deployment
                    let cwd = env::current_dir()?;
                    let project_context = ProjectContext::find_and_load(Some(&cwd))?;

                    // Create Helix manager
                    let helix_manager = HelixManager::new(&project_context);

                    // Create cloud instance configuration
                    let cloud_config = helix_manager
                        .create_instance_config(project_name, region)
                        .await?;

                    // Initialize the cloud cluster
                    helix_manager
                        .init_cluster(project_name, &cloud_config)
                        .await?;

                    // Insert into config
                    config.cloud.insert(
                        project_name.to_string(),
                        CloudConfig::Helix(cloud_config.clone()),
                    );

                    // save config
                    config.save_to_file(&config_path)?;
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
            "Run 'helix build dev' to compile your project",
            "Run 'helix push dev' to start your development instance",
        ],
    );

    Ok(())
}

fn process_template(
    template_str: &str,
    project_dir: &Path,
    project_name: &str,
) -> eyre::Result<()> {
    // Parse template source
    let template_source = TemplateSource::parse(template_str)?;

    // Prepare template variables for rendering
    let mut variables = HashMap::new();
    variables.insert("project_name".to_string(), project_name.to_string());

    // Fetch raw template from git (with caching)
    print_status("TEMPLATE", &format!("Resolving template: {}", template_str));
    let raw_template_path = TemplateFetcher::fetch(&template_source)?;

    // Render and apply template to project directory.
    print_status("TEMPLATE", "Rendering template...");
    TemplateProcessor::render_to_dir(&raw_template_path, project_dir, &variables)?;

    Ok(())
}

fn create_project_structure(project_dir: &Path, queries_path: &str) -> Result<()> {
    // Create directories
    fs::create_dir_all(project_dir.join(".helix"))?;
    fs::create_dir_all(project_dir.join(queries_path))?;

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
    fs::write(
        project_dir.join(queries_path).join("schema.hx"),
        default_schema,
    )?;

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
    fs::write(
        project_dir.join(queries_path).join("queries.hx"),
        default_queries,
    )?;

    // Create .gitignore
    let gitignore = r#".helix/
target/
*.log
"#;
    fs::write(project_dir.join(".gitignore"), gitignore)?;

    Ok(())
}
