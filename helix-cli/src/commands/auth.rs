use crate::{
    AuthAction,
    commands::integrations::helix::CLOUD_AUTHORITY,
    metrics_sender::{load_metrics_config, save_metrics_config},
    utils::{print_info, print_line, print_status, print_success, print_warning},
};
use color_eyre::owo_colors::OwoColorize;
use eyre::{OptionExt, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use std::{
    fs::{self, File},
    path::PathBuf,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};

pub async fn run(action: AuthAction) -> Result<()> {
    match action {
        AuthAction::Login => login().await,
        AuthAction::Logout => logout().await,
        AuthAction::CreateKey { cluster } => create_key(&cluster).await,
    }
}

async fn login() -> Result<()> {
    print_status("LOGIN", "Logging into Helix Cloud");

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
            "You have an existing key which may be valid, only continue if it doesn't work or you want to switch accounts. (Key checking is WIP)"
        );
    }

    let (key, user_id) = github_login().await.unwrap();

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

    print_success("Logged in successfully");
    print_info("Your credentials are stored in ~/.helix/credentials");

    Ok(())
}

async fn logout() -> Result<()> {
    print_status("LOGOUT", "Logging out of Helix Cloud");

    // Remove credentials file
    let home = dirs::home_dir().ok_or_eyre("Cannot find home directory")?;
    let credentials_path = home.join(".helix").join("credentials");

    if credentials_path.exists() {
        fs::remove_file(&credentials_path)?;
        print_success("Logged out successfully");
    } else {
        print_info("Not currently logged in");
    }

    Ok(())
}

async fn create_key(cluster: &str) -> Result<()> {
    print_status(
        "API_KEY",
        &format!("Creating API key for cluster: {cluster}"),
    );

    // TODO: Implement API key creation
    // This would:
    // 1. Authenticate with cloud
    // 2. Create new API key for specified cluster
    // 3. Display the key to the user

    print_warning("API key creation not yet implemented");
    print_line(&format!(
        "  This will create a new API key for cluster: {cluster}"
    ));

    Ok(())
}

#[derive(Debug)]
pub struct Credentials {
    pub(crate) user_id: String,
    pub(crate) helix_admin_key: String,
}

impl Credentials {
    pub(crate) fn is_authenticated(&self) -> bool {
        !self.user_id.is_empty() && !self.helix_admin_key.is_empty()
    }

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

#[derive(Deserialize)]
struct UserCodeMsg {
    user_code: String,
    verification_uri: String,
}

#[derive(Deserialize)]
struct ApiKeyMsg {
    user_id: String,
    key: String,
}
pub async fn github_login() -> Result<(String, String)> {
    let url = format!("ws://{}/login", *CLOUD_AUTHORITY);
    let (mut ws_stream, _) = connect_async(url).await?;

    let init_msg: UserCodeMsg = match ws_stream.next().await {
        Some(Ok(Message::Text(payload))) => serde_json::from_str(&payload)?,
        Some(Ok(m)) => return Err(eyre::eyre!("Unexpected message: {m:?}")),
        Some(Err(e)) => return Err(e.into()),
        None => return Err(eyre::eyre!("Connection Closed Unexpectedly")),
    };

    println!(
        "To Login please go \x1b]8;;{}\x1b\\here\x1b]8;;\x1b\\({}),\nand enter the code: {}",
        init_msg.verification_uri,
        init_msg.verification_uri,
        init_msg.user_code.bold()
    );

    let msg: ApiKeyMsg = match ws_stream.next().await {
        Some(Ok(Message::Text(payload))) => serde_json::from_str(&payload)?,
        Some(Ok(Message::Close(Some(CloseFrame {
            code: CloseCode::Error,
            reason,
        })))) => return Err(eyre::eyre!("Error: {reason}")),
        Some(Ok(m)) => return Err(eyre::eyre!("Unexpected message: {m:?}")),
        Some(Err(e)) => return Err(e.into()),
        None => return Err(eyre::eyre!("Connection Closed Unexpectedly")),
    };

    Ok((msg.key, msg.user_id))
}
