use crate::commands::auth::Credentials;
use crate::commands::build::{collect_hx_files, generate_content};
use crate::config::InstanceInfo;
use crate::project::ProjectContext;
use eyre::{Result, eyre};
use helix_db::helix_engine::traversal_core::config::Config;
use helix_db::utils::styled_string::StyledString;
use serde_json::json;
use std::env;
use std::path::PathBuf;
use std::sync::LazyLock;

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

    fn credentials_path(&self) -> PathBuf {
        self.project.helix_dir.join("credentials")
    }

    fn check_auth(&self) -> Result<()> {
        let credentials_path = self.credentials_path();
        if !credentials_path.exists() {
            return Err(eyre!("Credentials file not found"));
        }

        let credentials = Credentials::read_from_file(&credentials_path);
        if !credentials.is_authenticated() {
            return Err(eyre!("Credentials file is not authenticated"));
        }

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
        let files = collect_hx_files(&path).unwrap_or_default();

        let content = match generate_content(&files) {
            Ok(content) => content,
            Err(e) => {
                return Err(eyre!("Error: failed to generate content: {e}"));
            }
        };

        // get credentials - already validated by check_auth()
        let credentials = Credentials::read_from_file(&self.credentials_path());

        // read config.hx.json
        let config = match Config::from_files(path.join("config.hx.json"), path.join("schema.hx")) {
            Ok(config) => config,
            Err(e) => {
                return Err(eyre!("Error: failed to load config: {e}"));
            }
        };

        // get cluster information from helix.toml
        let cluster_info = match self.project.config.get_instance(&cluster_name)? {
            InstanceInfo::HelixCloud(config) => config,
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
