use crate::commands::auth::Credentials;
use crate::config::{BuildMode, CloudInstanceConfig, DbConfig, InstanceInfo};
use crate::project::ProjectContext;
use crate::utils::helixc_utils::{collect_hx_files, generate_content};
use crate::utils::{print_error_with_hint, print_status, print_success};
use eyre::{OptionExt, Result, eyre};
use helix_db::helix_engine::traversal_core::config::Config;
use helix_db::utils::styled_string::StyledString;
use serde_json::json;
use std::env;
use std::path::PathBuf;
use std::sync::LazyLock;
// use uuid::Uuid;

const DEFAULT_CLOUD_AUTHORITY: &str = "ec2-184-72-27-116.us-west-1.compute.amazonaws.com:3000";
pub static CLOUD_AUTHORITY: LazyLock<String> = LazyLock::new(|| {
    std::env::var("CLOUD_AUTHORITY").unwrap_or(DEFAULT_CLOUD_AUTHORITY.to_string())
});

pub struct HelixManager<'a> {
    project: &'a ProjectContext,
}

impl<'a> HelixManager<'a> {
    pub fn new(project: &'a ProjectContext) -> Self {
        Self { project }
    }

    fn credentials_path(&self) -> Result<PathBuf> {
        // get home directory
        let home = dirs::home_dir().ok_or_eyre("Cannot find home directory")?;
        Ok(home.join(".helix").join("credentials"))
    }

    fn check_auth(&self) -> Result<()> {
        let credentials_path = self.credentials_path()?;
        if !credentials_path.exists() {
            print_error_with_hint(
                "Credentials file not found",
                "Run 'helix auth login' to authenticate with Helix Cloud",
            );
            return Err(eyre!(""));
        }

        let credentials = Credentials::read_from_file(&credentials_path);
        if !credentials.is_authenticated() {
            return Err(eyre!("Credentials file is not authenticated"));
        }

        Ok(())
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

        print_status(
            "CONFIG",
            &format!("Creating cloud configuration for cluster: {cluster_id}"),
        );

        Ok(CloudInstanceConfig {
            cluster_id,
            region,
            build_mode: BuildMode::Release,
            db_config: DbConfig::default(),
        })
    }

    pub async fn init_cluster(
        &self,
        instance_name: &str,
        config: &CloudInstanceConfig,
    ) -> Result<()> {
        // Check authentication first
        self.check_auth()?;

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

    pub(crate) async fn deploy(&self, path: Option<String>, cluster_name: String) -> Result<()> {
        self.check_auth()?;
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

        // get credentials - already validated by check_auth()
        let credentials = Credentials::read_from_file(&self.credentials_path()?);

        // read config.hx.json
        let config_path = path.join("config.hx.json");
        let schema_path = path.join("schema.hx");

        // Use from_files if schema.hx exists (backward compatibility), otherwise use from_file
        let config = if schema_path.exists() {
            match Config::from_files(config_path, schema_path) {
                Ok(config) => config,
                Err(e) => {
                    return Err(eyre!("Error: failed to load config: {e}"));
                }
            }
        } else {
            match Config::from_file(config_path) {
                Ok(config) => config,
                Err(e) => {
                    return Err(eyre!("Error: failed to load config: {e}"));
                }
            }
        };

        // get cluster information from helix.toml
        let cluster_info = match self.project.config.get_instance(&cluster_name)? {
            InstanceInfo::Helix(config) => config,
            _ => {
                return Err(eyre!("Error: cluster is not a cloud instance"));
            }
        };

        // upload queries to central server
        let payload = json!({
            "user_id": credentials.user_id,
            "queries": content.files,
            "cluster_id": cluster_info.cluster_id,
            "version": "0.1.0",
            "helix_config": config.to_json()
        });
        let client = reqwest::Client::new();

        let cloud_url = format!("http://{}/clusters/deploy-queries", *CLOUD_AUTHORITY);

        match client
            .post(cloud_url)
            .header("x-api-key", &credentials.helix_admin_key) // used to verify user
            .header("x-cluster-id", &cluster_info.cluster_id) // used to verify instance with user
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&payload).unwrap())
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    println!("{}", "Queries uploaded to remote db".green().bold());
                } else {
                    return Err(eyre!("Error uploading queries to remote db"));
                }
            }
            Err(e) => {
                return Err(eyre!("Error uploading queries to remote db: {e}"));
            }
        };

        Ok(())
    }
}

/// Returns the path or the current working directory if no path is provided
pub fn get_path_or_cwd(path: Option<&String>) -> Result<PathBuf> {
    match path {
        Some(p) => Ok(PathBuf::from(p)),
        None => Ok(env::current_dir()?),
    }
}
