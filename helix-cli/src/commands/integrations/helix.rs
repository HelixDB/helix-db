use crate::commands::auth::require_auth;
use crate::config::{BuildMode, CloudInstanceConfig, DbConfig, InstanceInfo};
use crate::project::ProjectContext;
use crate::sse_client::{SseEvent, SseProgressHandler};
use crate::utils::helixc_utils::{collect_hx_files, generate_content};
use crate::utils::{print_error, print_status, print_success};
use eyre::{Result, eyre};
use helix_db::helix_engine::traversal_core::config::Config;
use reqwest_eventsource::RequestBuilderExt;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::LazyLock;
// use uuid::Uuid;

const DEFAULT_CLOUD_AUTHORITY: &str = "cloud.helix-db.com";
pub static CLOUD_AUTHORITY: LazyLock<String> = LazyLock::new(|| {
    std::env::var("CLOUD_AUTHORITY").unwrap_or_else(|_| {
        if cfg!(debug_assertions) {
            "localhost:3000".to_string()
        } else {
            DEFAULT_CLOUD_AUTHORITY.to_string()
        }
    })
});

pub struct HelixManager<'a> {
    project: &'a ProjectContext,
}

impl<'a> HelixManager<'a> {
    pub fn new(project: &'a ProjectContext) -> Self {
        Self { project }
    }

    pub async fn create_instance_config(
        &self,
        _instance_name: &str,
        region: Option<String>,
    ) -> Result<CloudInstanceConfig> {
        // Generate unique cluster ID
        // let cluster_id = format!("helix-{}-{}", instance_name, Uuid::new_v4());
        let cluster_id = "YOUR_CLUSTER_ID".to_string();

        // Use provided region or default to us-east-1
        let region = region.or_else(|| Some("us-east-1".to_string()));

        Ok(CloudInstanceConfig {
            cluster_id,
            region,
            build_mode: BuildMode::Release,
            env_vars: HashMap::new(),
            db_config: DbConfig::default(),
        })
    }

    #[allow(dead_code)]
    pub async fn init_cluster(
        &self,
        instance_name: &str,
        config: &CloudInstanceConfig,
    ) -> Result<()> {
        // Check authentication first
        require_auth().await?;

        print_status(
            "CLOUD",
            &format!("Initializing Helix cloud cluster: {}", config.cluster_id),
        );
        print_status(
            "INFO",
            "Note: Cluster provisioning API is not yet implemented",
        );
        print_status(
            "INFO",
            "This will create the configuration locally and provision the cluster when the API is ready",
        );

        // TODO: When the backend API is ready, implement actual cluster creation
        // let credentials = Credentials::read_from_file(&self.credentials_path());
        // let create_request = json!({
        //     "name": instance_name,
        //     "cluster_id": config.cluster_id,
        //     "region": config.region,
        //     "instance_type": "small",
        //     "user_id": credentials.user_id
        // });

        // let client = reqwest::Client::new();
        // let cloud_url = format!("http://{}/clusters/create", *CLOUD_AUTHORITY);

        // let response = client
        //     .post(cloud_url)
        //     .header("x-api-key", &credentials.helix_admin_key)
        //     .header("Content-Type", "application/json")
        //     .json(&create_request)
        //     .send()
        //     .await?;

        // match response.status() {
        //     reqwest::StatusCode::CREATED => {
        //         print_success("Cluster creation initiated");
        //         self.wait_for_cluster_ready(&config.cluster_id).await?;
        //     }
        //     reqwest::StatusCode::CONFLICT => {
        //         return Err(eyre!("Cluster name '{}' already exists", instance_name));
        //     }
        //     reqwest::StatusCode::UNAUTHORIZED => {
        //         return Err(eyre!("Authentication failed. Run 'helix auth login'"));
        //     }
        //     _ => {
        //         let error_text = response.text().await.unwrap_or_default();
        //         return Err(eyre!("Failed to create cluster: {}", error_text));
        //     }
        // }

        print_success(format!("Cloud instance '{instance_name}' configuration created").as_str());
        print_status(
            "NEXT",
            "Run 'helix build <instance>' to compile your project for this instance",
        );

        Ok(())
    }

    pub(crate) async fn deploy(
        &self,
        path: Option<String>,
        cluster_name: String,
        build_mode: BuildMode,
    ) -> Result<()> {
        let credentials = require_auth().await?;
        let path = match get_path_or_cwd(path.as_ref()) {
            Ok(path) => path,
            Err(e) => {
                return Err(eyre!("Error: failed to get path: {e}"));
            }
        };
        let files =
            collect_hx_files(&path, &self.project.config.project.queries).unwrap_or_default();

        let content = match generate_content(&files) {
            Ok(content) => content,
            Err(e) => {
                return Err(eyre!("Error: failed to generate content: {e}"));
            }
        };

        // Optionally load config from helix.toml or legacy config.hx.json
        let helix_toml_path = path.join("helix.toml");
        let config_hx_path = path.join("config.hx.json");
        let schema_path = path.join("schema.hx");

        let _config: Option<Config> = if helix_toml_path.exists() {
            // v2 format: helix.toml (config is already loaded in self.project)
            None
        } else if config_hx_path.exists() {
            // v1 backward compatibility: config.hx.json
            if schema_path.exists() {
                Config::from_files(config_hx_path, schema_path).ok()
            } else {
                Config::from_file(config_hx_path).ok()
            }
        } else {
            None
        };

        // get cluster information from helix.toml
        let cluster_info = match self.project.config.get_instance(&cluster_name)? {
            InstanceInfo::Helix(config) => config,
            _ => {
                return Err(eyre!("Error: cluster is not a cloud instance"));
            }
        };

        // Separate schema from query files
        let mut schema_content = String::new();
        let mut queries_map: HashMap<String, String> = HashMap::new();

        for file in &content.files {
            if file.name.ends_with("schema.hx") {
                schema_content = file.content.clone();
            } else {
                queries_map.insert(file.name.clone(), file.content.clone());
            }
        }

        let dev_profile = build_mode == BuildMode::Dev;

        // Prepare deployment payload
        let payload = json!({
            "schema": schema_content,
            "queries": queries_map,
            "env_vars": cluster_info.env_vars,
            "instance_name": cluster_name,
            "dev_profile": dev_profile
        });

        // Initiate deployment with SSE streaming
        let client = reqwest::Client::new();
        let deploy_url = format!("https://{}/deploy", *CLOUD_AUTHORITY);

        let mut event_source = client
            .post(&deploy_url)
            .header("x-api-key", &credentials.helix_admin_key)
            .header("x-cluster-id", &cluster_info.cluster_id)
            .header("Content-Type", "application/json")
            .json(&payload)
            .eventsource()?;

        let progress = SseProgressHandler::new("Deploying queries...");
        let mut deployment_success = false;

        // Process SSE events
        use futures_util::StreamExt;

        while let Some(event) = event_source.next().await {
            match event {
                Ok(reqwest_eventsource::Event::Open) => {
                    // Connection opened
                }
                Ok(reqwest_eventsource::Event::Message(message)) => {
                    // Parse the SSE event
                    let sse_event: SseEvent = match serde_json::from_str(&message.data) {
                        Ok(event) => event,
                        Err(e) => {
                            progress.println(&format!("Failed to parse event: {}", e));
                            continue;
                        }
                    };

                    match sse_event {
                        SseEvent::Progress {
                            percentage,
                            message,
                        } => {
                            progress.set_progress(percentage);
                            if let Some(msg) = message {
                                progress.set_message(&msg);
                            }
                        }
                        SseEvent::Log { message, .. } => {
                            progress.println(&message);
                        }
                        SseEvent::StatusTransition { to, message, .. } => {
                            let msg = message.unwrap_or_else(|| format!("Status: {}", to));
                            progress.println(&msg);
                        }
                        SseEvent::Success { .. } => {
                            deployment_success = true;
                            progress.finish("Deployment completed successfully!");
                            event_source.close();
                            break;
                        }
                        SseEvent::Error { error } => {
                            progress.finish_error(&format!("Error: {}", error));
                            event_source.close();
                            return Err(eyre!("Deployment failed: {}", error));
                        }
                        // Deploy-specific events
                        SseEvent::ValidatingQueries => {
                            progress.set_message("Validating queries...");
                        }
                        SseEvent::Building {
                            estimated_percentage,
                        } => {
                            progress.set_progress(estimated_percentage as f64);
                            progress.set_message("Building...");
                        }
                        SseEvent::Deploying => {
                            progress.set_message("Deploying to infrastructure...");
                        }
                        SseEvent::Deployed { url, auth_key } => {
                            deployment_success = true;
                            progress.finish("Deployment completed!");
                            print_success(&format!("Deployed to: {}", url));
                            print_status("AUTH_KEY", &format!("Your auth key: {}", auth_key));

                            // Prompt user for .env handling
                            println!();
                            println!("Would you like to save connection details to a .env file?");
                            println!("  1. Add to .env in project root (Recommended)");
                            println!("  2. Don't add");
                            println!("  3. Specify custom path");
                            print!("\nChoice [1]: ");

                            use std::io::{self, Write};
                            io::stdout().flush().ok();

                            let mut input = String::new();
                            if io::stdin().read_line(&mut input).is_ok() {
                                let choice = input.trim();
                                match choice {
                                    "1" | "" => {
                                        let env_path = self.project.root.join(".env");
                                        let comment = format!(
                                            "# HelixDB Cloud URL for instance: {}",
                                            cluster_name
                                        );
                                        if let Err(e) = crate::utils::add_env_var_with_comment(
                                            &env_path,
                                            "HELIX_CLOUD_URL",
                                            &url,
                                            Some(&comment),
                                        ) {
                                            print_error(&format!("Failed to write .env: {}", e));
                                        }
                                        match crate::utils::add_env_var_to_file(
                                            &env_path,
                                            "HELIX_API_KEY",
                                            &auth_key,
                                        ) {
                                            Ok(_) => print_success(&format!(
                                                "Added HELIX_CLOUD_URL and HELIX_API_KEY to {}",
                                                env_path.display()
                                            )),
                                            Err(e) => {
                                                print_error(&format!("Failed to write .env: {}", e))
                                            }
                                        }
                                    }
                                    "2" => {
                                        print_status("INFO", "Skipped saving to .env");
                                    }
                                    "3" => {
                                        print!("Enter path: ");
                                        io::stdout().flush().ok();
                                        let mut path_input = String::new();
                                        if io::stdin().read_line(&mut path_input).is_ok() {
                                            let custom_path = PathBuf::from(path_input.trim());
                                            let comment = format!(
                                                "# HelixDB Cloud URL for instance: {}",
                                                cluster_name
                                            );
                                            if let Err(e) = crate::utils::add_env_var_with_comment(
                                                &custom_path,
                                                "HELIX_CLOUD_URL",
                                                &url,
                                                Some(&comment),
                                            ) {
                                                print_error(&format!(
                                                    "Failed to write .env: {}",
                                                    e
                                                ));
                                            }
                                            match crate::utils::add_env_var_to_file(
                                                &custom_path,
                                                "HELIX_API_KEY",
                                                &auth_key,
                                            ) {
                                                Ok(_) => print_success(&format!(
                                                    "Added HELIX_CLOUD_URL and HELIX_API_KEY to {}",
                                                    custom_path.display()
                                                )),
                                                Err(e) => print_error(&format!(
                                                    "Failed to write .env: {}",
                                                    e
                                                )),
                                            }
                                        }
                                    }
                                    _ => {
                                        print_status(
                                            "INFO",
                                            "Invalid choice, skipped saving to .env",
                                        );
                                    }
                                }
                            }

                            event_source.close();
                            break;
                        }
                        SseEvent::Redeployed { url } => {
                            deployment_success = true;
                            progress.finish("Redeployment completed!");
                            print_success(&format!("Redeployed to: {}", url));
                            event_source.close();
                            break;
                        }
                        SseEvent::BadRequest { error } => {
                            progress.finish_error(&format!("Bad request: {}", error));
                            event_source.close();
                            return Err(eyre!("Bad request: {}", error));
                        }
                        SseEvent::QueryValidationError { error } => {
                            progress.finish_error(&format!("Query validation failed: {}", error));
                            event_source.close();
                            return Err(eyre!("Query validation error: {}", error));
                        }
                        _ => {
                            // Ignore other event types
                        }
                    }
                }
                Err(err) => {
                    progress.finish_error(&format!("Stream error: {}", err));
                    return Err(eyre!("Deployment stream error: {}", err));
                }
            }
        }

        if !deployment_success {
            return Err(eyre!("Deployment did not complete successfully"));
        }

        print_success("Queries deployed successfully");
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn redeploy(
        &self,
        path: Option<String>,
        cluster_name: String,
        build_mode: BuildMode,
    ) -> Result<()> {
        // Redeploy is similar to deploy but may have different backend handling
        // For now, we'll use the same implementation with a different status message
        print_status(
            "REDEPLOY",
            &format!("Redeploying to cluster: {}", cluster_name),
        );

        // Call deploy with the same logic
        // In the future, this could use a different endpoint or add a "redeploy" flag
        self.deploy(path, cluster_name, build_mode).await
    }
}

/// Returns the path or the current working directory if no path is provided
pub fn get_path_or_cwd(path: Option<&String>) -> Result<PathBuf> {
    match path {
        Some(p) => Ok(PathBuf::from(p)),
        None => Ok(env::current_dir()?),
    }
}
