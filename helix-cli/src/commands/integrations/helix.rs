use crate::commands::auth::require_auth;
use crate::config::{BuildMode, CloudConfig, CloudInstanceConfig, DbConfig, InstanceInfo};
use crate::output;
use crate::project::ProjectContext;
use crate::sse_client::{SseEvent, SseProgressHandler, parse_sse_event};
use crate::utils::helixc_utils::{collect_hx_files, generate_content};
use crate::utils::print_error;
use base64::prelude::{BASE64_STANDARD, Engine as _};
use eyre::{Result, eyre};
use helix_db::helix_engine::traversal_core::config::Config;
use reqwest_eventsource::RequestBuilderExt;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
// use uuid::Uuid;

const DEFAULT_CLOUD_AUTHORITY: &str = "cloud.helix-db.com";
pub static CLOUD_AUTHORITY: LazyLock<String> = LazyLock::new(|| {
    std::env::var("CLOUD_AUTHORITY").unwrap_or_else(|_| {
        if cfg!(debug_assertions) {
            "localhost:8080".to_string()
        } else {
            DEFAULT_CLOUD_AUTHORITY.to_string()
        }
    })
});

pub fn cloud_base_url() -> String {
    let authority = CLOUD_AUTHORITY.as_str();

    if authority.starts_with("http://") || authority.starts_with("https://") {
        authority.to_string()
    } else if authority.starts_with("localhost") || authority.starts_with("127.0.0.1") {
        format!("http://{authority}")
    } else {
        format!("https://{authority}")
    }
}

pub struct HelixManager<'a> {
    project: &'a ProjectContext,
}

const QUERY_BIN_VERSION: u32 = 1;

#[derive(serde::Serialize)]
struct QueryBinEnvelope {
    version: u32,
    schema: String,
    queries: HashMap<String, String>,
}

impl<'a> HelixManager<'a> {
    pub fn new(project: &'a ProjectContext) -> Self {
        Self { project }
    }

    #[allow(dead_code)]
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

        output::info(&format!(
            "Initializing Helix cloud cluster: {}",
            config.cluster_id
        ));
        output::info("Note: Cluster provisioning API is not yet implemented");
        output::info(
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

        output::success(&format!(
            "Cloud instance '{instance_name}' configuration created"
        ));
        output::info("Run 'helix build <instance>' to compile your project for this instance");

        Ok(())
    }

    pub(crate) async fn deploy(
        &self,
        path: Option<String>,
        cluster_name: String,
        build_mode_override: Option<BuildMode>,
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

        let queries_root = path
            .join(&self.project.config.project.queries)
            .canonicalize()
            .unwrap_or_else(|_| path.join(&self.project.config.project.queries));

        for file in &content.files {
            let file_path = Path::new(&file.name);
            let relative_name = file_path
                .strip_prefix(&queries_root)
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .unwrap_or_else(|_| {
                    file_path
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string())
                        .unwrap_or_else(|| file.name.clone())
                });

            if relative_name.ends_with("schema.hx") {
                schema_content = file.content.clone();
            } else {
                queries_map.insert(relative_name, file.content.clone());
            }
        }

        let query_bin_payload = QueryBinEnvelope {
            version: QUERY_BIN_VERSION,
            schema: schema_content.clone(),
            queries: queries_map.clone(),
        };
        let query_bin_bytes = bincode::serialize(&query_bin_payload)
            .map_err(|e| eyre!("Failed to serialize queries.bin payload: {e}"))?;
        let query_bin_b64 = BASE64_STANDARD.encode(&query_bin_bytes);

        // Build a pruned HelixConfig containing only [project] and the deployed [cloud.<instance>]
        let helix_toml_content = {
            use crate::config::HelixConfig;
            let pruned = HelixConfig {
                project: self.project.config.project.clone(),
                local: HashMap::new(),
                cloud: {
                    let mut m = HashMap::new();
                    m.insert(
                        cluster_name.clone(),
                        crate::config::CloudConfig::from(
                            self.project.config.get_instance(&cluster_name)?,
                        ),
                    );
                    m
                },
                enterprise: HashMap::new(),
            };
            match toml::to_string_pretty(&pruned) {
                Ok(s) => Some(s),
                Err(e) => {
                    output::warning(&format!("Failed to serialize pruned helix.toml: {}", e));
                    None
                }
            }
        };

        // Prepare deployment payload
        let build_mode_override = build_mode_override
            .map(|mode| match mode {
                BuildMode::Dev => Ok("dev"),
                BuildMode::Release => Ok("release"),
                BuildMode::Debug => {
                    Err(eyre!("debug build mode is not supported for cloud deploys"))
                }
            })
            .transpose()?;

        let payload = json!({
            "schema": schema_content,
            "queries": queries_map,
            "queries_bin_b64": query_bin_b64,
            "env_vars": cluster_info.env_vars,
            "instance_name": cluster_name,
            "helix_toml": helix_toml_content,
            "build_mode_override": build_mode_override
        });

        // Initiate deployment with SSE streaming
        let client = reqwest::Client::new();
        let deploy_url = format!(
            "{}/api/cli/clusters/{}/deploy",
            cloud_base_url(),
            cluster_info.cluster_id
        );

        let mut event_source = client
            .post(&deploy_url)
            .header("x-api-key", &credentials.helix_admin_key)
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
                    let sse_event: SseEvent = match parse_sse_event(&message.data) {
                        Ok(event) => event,
                        Err(e) => {
                            output::verbose(&format!(
                                "Ignoring unrecognized deploy SSE payload: {}",
                                e
                            ));
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
                            output::success(&format!("Deployed to: {}", url));
                            output::info(&format!("Your auth key: {}", auth_key));

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
                                            Ok(_) => output::success(&format!(
                                                "Added HELIX_CLOUD_URL and HELIX_API_KEY to {}",
                                                env_path.display()
                                            )),
                                            Err(e) => {
                                                print_error(&format!("Failed to write .env: {}", e))
                                            }
                                        }
                                    }
                                    "2" => {
                                        output::info("Skipped saving to .env");
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
                                                Ok(_) => output::success(&format!(
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
                                        output::info("Invalid choice, skipped saving to .env");
                                    }
                                }
                            }

                            event_source.close();
                            break;
                        }
                        SseEvent::Redeployed { url } => {
                            deployment_success = true;
                            progress.finish("Redeployment completed!");
                            output::success(&format!("Redeployed to: {}", url));
                            event_source.close();
                            break;
                        }
                        SseEvent::Done { url, auth_key } => {
                            deployment_success = true;

                            if let Some(auth_key) = auth_key {
                                progress.finish("Deployment completed!");
                                output::success(&format!("Deployed to: {}", url));
                                output::info(&format!("Your auth key: {}", auth_key));
                            } else {
                                progress.finish("Redeployment completed!");
                                output::success(&format!("Redeployed to: {}", url));
                            }

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

        output::success("Queries deployed successfully");
        Ok(())
    }

    pub(crate) async fn deploy_by_cluster_id(
        &self,
        path: Option<String>,
        cluster_id: &str,
        cluster_name_hint: &str,
        build_mode_override: Option<BuildMode>,
    ) -> Result<()> {
        if let Some(instance_name) =
            self.project
                .config
                .cloud
                .iter()
                .find_map(|(instance_name, cloud_config)| match cloud_config {
                    CloudConfig::Helix(config) if config.cluster_id == cluster_id => {
                        Some(instance_name.clone())
                    }
                    _ => None,
                })
        {
            return self.deploy(path, instance_name, build_mode_override).await;
        }

        Err(eyre!(
            "Cluster '{}' is not configured in helix.toml. Run 'helix sync' to refresh cluster metadata, then retry syncing cluster '{}'.",
            cluster_id,
            cluster_name_hint
        ))
    }

    /// Deploy .rs files to an enterprise cluster
    pub(crate) async fn deploy_enterprise(
        &self,
        path: Option<String>,
        cluster_name: String,
        config: &crate::config::EnterpriseInstanceConfig,
    ) -> Result<()> {
        let credentials = require_auth().await?;
        let path = match get_path_or_cwd(path.as_ref()) {
            Ok(path) => path,
            Err(e) => {
                return Err(eyre!("Error: failed to get path: {e}"));
            }
        };

        // Collect .rs files from queries directory
        let queries_dir = path.join(&self.project.config.project.queries);
        let mut rs_files: HashMap<String, String> = HashMap::new();

        if queries_dir.exists() {
            for entry in std::fs::read_dir(&queries_dir)? {
                let entry = entry?;
                let file_path = entry.path();
                if file_path.is_file() && file_path.extension().map(|e| e == "rs").unwrap_or(false)
                {
                    let filename = file_path.file_name().unwrap().to_string_lossy().to_string();
                    let content = std::fs::read_to_string(&file_path)
                        .map_err(|e| eyre!("Failed to read {}: {}", filename, e))?;
                    rs_files.insert(filename, content);
                }
            }
        }

        if rs_files.is_empty() {
            return Err(eyre!(
                "No .rs files found in queries directory: {}",
                queries_dir.display()
            ));
        }

        // Build pruned helix.toml
        let helix_toml_content = {
            use crate::config::HelixConfig;
            let pruned = HelixConfig {
                project: self.project.config.project.clone(),
                local: HashMap::new(),
                cloud: HashMap::new(),
                enterprise: {
                    let mut m = HashMap::new();
                    m.insert(cluster_name.clone(), config.clone());
                    m
                },
            };
            toml::to_string_pretty(&pruned).ok()
        };

        let payload = json!({
            "rs_files": rs_files,
            "instance_name": cluster_name,
            "helix_toml": helix_toml_content,
        });

        // Send to enterprise deploy endpoint
        let client = reqwest::Client::new();
        let deploy_url = format!(
            "{}/api/cli/enterprise-clusters/{}/deploy",
            cloud_base_url(),
            config.cluster_id
        );

        let mut event_source = client
            .post(&deploy_url)
            .header("x-api-key", &credentials.helix_admin_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .eventsource()?;

        let progress = SseProgressHandler::new("Deploying enterprise cluster...");
        let mut deployment_success = false;

        use futures_util::StreamExt;

        while let Some(event) = event_source.next().await {
            match event {
                Ok(reqwest_eventsource::Event::Open) => {}
                Ok(reqwest_eventsource::Event::Message(message)) => {
                    let sse_event: SseEvent = match parse_sse_event(&message.data) {
                        Ok(event) => event,
                        Err(e) => {
                            output::verbose(&format!(
                                "Ignoring unrecognized deploy SSE payload: {}",
                                e
                            ));
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
                        SseEvent::Success { .. } => {
                            deployment_success = true;
                            progress.finish("Enterprise deployment completed!");
                            event_source.close();
                            break;
                        }
                        SseEvent::Error { error } => {
                            progress.finish_error(&format!("Error: {}", error));
                            event_source.close();
                            return Err(eyre!("Enterprise deployment failed: {}", error));
                        }
                        SseEvent::Deployed { url, auth_key } => {
                            deployment_success = true;
                            progress.finish("Enterprise deployment completed!");
                            output::success(&format!("Deployed to: {}", url));
                            output::info(&format!("Your auth key: {}", auth_key));
                            event_source.close();
                            break;
                        }
                        _ => {}
                    }
                }
                Err(err) => {
                    progress.finish_error(&format!("Stream error: {}", err));
                    return Err(eyre!("Enterprise deployment stream error: {}", err));
                }
            }
        }

        if !deployment_success {
            return Err(eyre!("Enterprise deployment did not complete successfully"));
        }

        output::success("Enterprise cluster deployed successfully");
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
        output::info(&format!("Redeploying to cluster: {}", cluster_name));

        // Call deploy with the same logic
        // In the future, this could use a different endpoint or add a "redeploy" flag
        self.deploy(path, cluster_name, Some(build_mode)).await
    }
}

/// Returns the path or the current working directory if no path is provided
pub fn get_path_or_cwd(path: Option<&String>) -> Result<PathBuf> {
    match path {
        Some(p) => Ok(PathBuf::from(p)),
        None => Ok(env::current_dir()?),
    }
}
