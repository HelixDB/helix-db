use crate::CloudDeploymentTypeCommand;
use crate::cleanup::CleanupTracker;
use crate::commands::integrations::ecr::{EcrAuthType, EcrManager};
use crate::commands::integrations::fly::{FlyAuthType, FlyManager, VmSize};
use crate::commands::integrations::helix::HelixManager;
use crate::config::{CloudConfig, HelixConfig};
use crate::docker::DockerManager;
use crate::errors::project_error;
use crate::output::{Operation, Step};
use crate::project::ProjectContext;
use crate::prompts;
use crate::utils::print_instructions;
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

    let op = Operation::new("Initializing", project_name);

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
                    Step::verbose_substep("Helix Cloud instance configuration saved");
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
                        crate::output::info(&format!(
                            "Cluster creation skipped. Run 'helix create-cluster {}' when ready.",
                            project_name
                        ));
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

                    Step::verbose_substep("AWS ECR repository initialized successfully");
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

    op.success();
    let queries_path_clean = queries_path.trim_end_matches('/');
    print_instructions(
        "Next steps:",
        &[
            &format!("Customize {queries_path_clean}/schema.hx with your data model"),
            &format!("Edit {queries_path_clean}/queries.hx or add your own queries"),
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

    // Create default schema.hx with starter types
    let default_schema = r#"// ─── Node types ────────────────────────────────────────────
// Nodes represent entities in your graph.
// Use INDEX for fields you want to query by.

N::User {
    INDEX name: String,
    email: String,
    age: I32,
    created_at: Date DEFAULT NOW,
}

// ─── Vector types ──────────────────────────────────────────
// Vectors store embeddings for similarity search.
// Fields here are metadata stored alongside the vector.

V::Document {
    title: String,
    content: String,
    source: String,
}

// ─── Edge types ────────────────────────────────────────────
// Edges connect nodes and can carry properties.

E::Authored {
    From: User,
    To: Document,
    Properties: {
        role: String,
    }
}
"#;
    let schema_path = project_dir.join(queries_path).join("schema.hx");
    fs::write(&schema_path, default_schema)?;
    cleanup_tracker.track_file(schema_path);

    // Create default queries.hx with starter queries matching the schema
    let default_queries = r#"// ─── Node queries ──────────────────────────────────────────

QUERY GetAllUsers() =>
    users <- N<User>
    RETURN users

QUERY GetUser(name: String) =>
    user <- N<User>({name: name})
    RETURN user

QUERY CreateUser(name: String, email: String, age: I32) =>
    user <- AddN<User>({name: name, email: email, age: age})
    RETURN user

QUERY UpdateUser(name: String, email: String) =>
    user <- N<User>({name: name})::UPDATE({email: email})
    RETURN user

QUERY DeleteUser(name: String) =>
    DROP N<User>({name: name})
    RETURN "success"

// ─── Vector queries ────────────────────────────────────────

QUERY AddDocument(title: String, content: String, source: String) =>
    doc <- AddV<Document>(
        Embed(content),
        {title: title, content: content, source: source}
    )
    RETURN doc

QUERY SearchDocuments(query: String, k: I32) =>
    results <- SearchV<Document>(Embed(query), k)
    RETURN results

// ─── Edge queries ──────────────────────────────────────────

QUERY LinkUserToDocument(name: String, doc_id: ID, role: String) =>
    user <- N<User>({name: name})
    doc <- V<Document>(doc_id)
    edge <- AddE<Authored>({role: role})::From(user)::To(doc)
    RETURN edge

QUERY GetUserDocuments(name: String) =>
    user <- N<User>({name: name})
    docs <- user::Out<Authored>
    RETURN docs

// For more on HelixQL, see https://docs.helix-db.com
// or visit https://github.com/HelixDB/helix-db
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
