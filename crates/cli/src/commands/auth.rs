use crate::{
    enterprise_cloud::cloud_base_url,
    metrics_sender::{load_metrics_config, save_metrics_config},
    output,
    sse_client::{SseClient, SseEvent},
    AuthAction,
};
use color_eyre::owo_colors::OwoColorize;
use eyre::{eyre, OptionExt, Result};
use serde::Deserialize;
use std::{
    fs::{self, File},
    path::PathBuf,
};

pub async fn run(action: AuthAction) -> Result<()> {
    match action {
        AuthAction::Login => login().await,
        AuthAction::Logout => logout().await,
        AuthAction::CreateKey { cluster } => create_key(&cluster).await,
    }
}

async fn login() -> Result<()> {
    output::info("Logging into Helix Cloud");

    let home = dirs::home_dir().ok_or_eyre("Cannot find home directory")?;
    let config_path = home.join(".helix");
    let cred_path = config_path.join("credentials");

    if !config_path.exists() {
        fs::create_dir_all(&config_path)?;
    }
    if !cred_path.exists() {
        File::create(&cred_path)?;
    }

    // not needed?
    if Credentials::try_read_from_file(&cred_path).is_some() {
        println!(
            "You already have saved credentials. Running login rotates your user key and revokes previous user keys."
        );
    }

    let (key, user_id) = github_login().await?;

    // write credentials
    let credentials = Credentials {
        user_id: user_id.clone(),
        helix_admin_key: key,
    };
    credentials.write_to_file(&cred_path);

    // write metics.toml
    let mut metrics = load_metrics_config()?;
    metrics.user_id = Some(user_id.leak());
    save_metrics_config(&metrics)?;

    output::success("Logged in successfully");
    output::info("Your credentials are stored in ~/.helix/credentials");

    Ok(())
}

async fn logout() -> Result<()> {
    output::info("Logging out of Helix Cloud");

    // Remove credentials file
    let home = dirs::home_dir().ok_or_eyre("Cannot find home directory")?;
    let credentials_path = home.join(".helix").join("credentials");

    if credentials_path.exists() {
        fs::remove_file(&credentials_path)?;
        output::success("Logged out successfully");
    } else {
        output::info("Not currently logged in");
    }

    Ok(())
}

async fn create_key(cluster: &str) -> Result<()> {
    #[derive(Deserialize)]
    struct CreateKeyResponse {
        key: String,
        warning: Option<String>,
    }

    #[derive(Deserialize)]
    struct ErrorResponse {
        error: String,
    }

    output::info(&format!("Rotating API key for cluster: {cluster}"));

    let credentials = require_auth().await?;
    let url = format!(
        "{}/api/cli/enterprise-clusters/{cluster}/key",
        cloud_base_url()
    );

    let response = reqwest::Client::new()
        .post(&url)
        .header("x-api-key", &credentials.helix_admin_key)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let error_body = response.text().await.unwrap_or_default();
        let error_message = serde_json::from_str::<ErrorResponse>(&error_body)
            .map(|error| error.error)
            .unwrap_or_else(|_| {
                if error_body.is_empty() {
                    format!("request failed with status {status}")
                } else {
                    error_body
                }
            });

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(eyre!(
                "Authentication failed. Run 'helix auth login' to re-authenticate."
            ));
        }

        return Err(eyre!("Failed to rotate API key: {error_message}"));
    }

    let body: CreateKeyResponse = response.json().await?;

    output::success("Cluster API key refresh completed");
    if let Some(warning) = body.warning.as_deref() {
        output::warning(warning);
    } else {
        output::info("Previous cluster keys were revoked after successful redeploy.");
    }
    println!();
    println!("Cluster: {}", cluster.bold());
    println!("New API key (shown once): {}", body.key.bold());

    output::info("Update HELIX_API_KEY in your environment before running queries.");

    Ok(())
}

#[derive(Debug)]
pub struct Credentials {
    pub(crate) user_id: String,
    pub(crate) helix_admin_key: String,
}

impl Credentials {
    pub fn is_authenticated(&self) -> bool {
        !self.user_id.is_empty() && !self.helix_admin_key.is_empty()
    }

    #[allow(unused)]
    pub(crate) fn read_from_file(path: &PathBuf) -> Self {
        let content = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("Failed to read credentials file at {path:?}: {e}"));
        Self::parse_key_value_format(&content)
            .unwrap_or_else(|e| panic!("Failed to parse credentials file at {path:?}: {e}"))
    }

    pub(crate) fn try_read_from_file(path: &PathBuf) -> Option<Self> {
        let content = fs::read_to_string(path).ok()?;
        Self::parse_key_value_format(&content).ok()
    }

    pub(crate) fn write_to_file(&self, path: &PathBuf) {
        let content = format!(
            "helix_user_id={}\nhelix_user_key={}",
            self.user_id, self.helix_admin_key
        );
        fs::write(path, content)
            .unwrap_or_else(|e| panic!("Failed to write credentials file to {path:?}: {e}"));
    }

    #[allow(unused)]
    pub(crate) fn try_write_to_file(&self, path: &PathBuf) -> Option<()> {
        let content = format!(
            "helix_user_id={}\nhelix_user_key={}",
            self.user_id, self.helix_admin_key
        );
        fs::write(path, content).ok()?;
        Some(())
    }

    fn parse_key_value_format(content: &str) -> Result<Self> {
        let mut user_id = None;
        let mut helix_admin_key = None;

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once('=') {
                match key.trim() {
                    "helix_user_id" => user_id = Some(value.trim().to_string()),
                    "helix_user_key" => helix_admin_key = Some(value.trim().to_string()),
                    _ => {} // Ignore unknown keys
                }
            }
        }

        Ok(Credentials {
            user_id: user_id.ok_or_eyre("Missing helix_user_id in credentials file")?,
            helix_admin_key: helix_admin_key
                .ok_or_eyre("Missing helix_user_key in credentials file")?,
        })
    }
}

/// Check that the user is authenticated with Helix Cloud.
/// If not authenticated, prompts the user to login interactively.
/// Returns credentials if authenticated (or after successful login).
pub async fn require_auth() -> Result<Credentials> {
    let home = dirs::home_dir().ok_or_eyre("Cannot find home directory")?;
    let credentials_path = home.join(".helix").join("credentials");

    // Check if we have valid credentials
    if let Some(credentials) = Credentials::try_read_from_file(&credentials_path)
        && credentials.is_authenticated()
    {
        return Ok(credentials);
    }

    output::warning("Not authenticated with Helix Cloud");
    Err(eyre!(
        "Authentication required. Run 'helix auth login' first."
    ))
}

/// Ensure the user has Helix Cloud credentials, running the existing GitHub
/// device login flow inline when credentials are missing or invalid.
pub async fn ensure_auth_or_login() -> Result<Credentials> {
    let home = dirs::home_dir().ok_or_eyre("Cannot find home directory")?;
    let config_path = home.join(".helix");
    let credentials_path = config_path.join("credentials");

    if let Some(credentials) = Credentials::try_read_from_file(&credentials_path)
        && credentials.is_authenticated()
    {
        return Ok(credentials);
    }

    fs::create_dir_all(&config_path)?;
    let (key, user_id) = github_login().await?;
    let credentials = Credentials {
        user_id: user_id.clone(),
        helix_admin_key: key,
    };
    credentials.write_to_file(&credentials_path);

    let mut metrics = load_metrics_config()?;
    metrics.user_id = Some(user_id.leak());
    save_metrics_config(&metrics)?;

    Ok(credentials)
}

pub async fn github_login() -> Result<(String, String)> {
    let url = format!("{}/github-login", cloud_base_url());
    let client = SseClient::new(url).post();

    let mut api_key: Option<String> = None;
    let mut user_id: Option<String> = None;

    client
        .connect(|event| {
            match event {
                SseEvent::UserVerification {
                    user_code,
                    verification_uri,
                    ..
                } => {
                    println!(
                        "To Login please go \x1b]8;;{}\x1b\\here\x1b]8;;\x1b\\({}),\nand enter the code: {}",
                        verification_uri,
                        verification_uri,
                        user_code.bold()
                    );
                    Ok(true) // Continue processing events
                }
                SseEvent::Success { data } => {
                    // Extract API key and user_id from success event
                    if let Some(key) = data.get("key").and_then(|v| v.as_str()) {
                        api_key = Some(key.to_string());
                    }
                    if let Some(id) = data.get("user_id").and_then(|v| v.as_str()) {
                        user_id = Some(id.to_string());
                    }
                    Ok(false) // Stop processing - login complete
                }
                SseEvent::DeviceCodeTimeout { message } => {
                    Err(eyre!("Login timeout: {}. Please try again.", message))
                }
                SseEvent::Error { error } => {
                    Err(eyre!("Login error: {}", error))
                }
                _ => {
                    // Ignore other event types during login
                    Ok(true)
                }
            }
        })
        .await?;

    match (api_key, user_id) {
        (Some(key), Some(id)) => Ok((key, id)),
        _ => Err(eyre!("Login completed but credentials were not received")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_parse_comments_unknown_fields_and_round_trip() {
        let credentials = Credentials::parse_key_value_format(
            "# generated\nunknown=value\nhelix_user_id = user-1\nhelix_user_key = key-1\n",
        )
        .unwrap();
        assert!(credentials.is_authenticated());
        assert_eq!(credentials.user_id, "user-1");
        assert_eq!(credentials.helix_admin_key, "key-1");

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials");
        credentials.write_to_file(&path);
        let restored = Credentials::try_read_from_file(&path).unwrap();
        assert_eq!(restored.user_id, "user-1");
        assert_eq!(restored.helix_admin_key, "key-1");
    }

    #[test]
    fn credentials_reject_missing_or_empty_required_values() {
        assert!(Credentials::parse_key_value_format("helix_user_id=user").is_err());
        assert!(Credentials::parse_key_value_format("helix_user_key=key").is_err());
        let empty =
            Credentials::parse_key_value_format("helix_user_id=\nhelix_user_key=\n").unwrap();
        assert!(!empty.is_authenticated());
    }
}
