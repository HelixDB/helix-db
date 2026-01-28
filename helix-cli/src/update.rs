use dirs::home_dir;
use eyre::{Result, eyre};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const GITHUB_API_URL: &str = "https://api.github.com/repos/himanalot/helix-db/releases/latest";
const UPDATE_CHECK_INTERVAL: u64 = 24 * 60 * 60; // 24 hours in seconds

#[derive(Deserialize)]
#[allow(unused)]
struct GitHubRelease {
    tag_name: String,
    name: String,
    html_url: String,
}

#[derive(Serialize, Deserialize)]
struct UpdateCache {
    last_check: u64,
    latest_version: Option<String>,
}

fn get_update_cache_path() -> Result<PathBuf> {
    let home = home_dir().ok_or_else(|| eyre!("Cannot find home directory"))?;
    let helix_dir = home.join(".helix");

    // Ensure .helix directory exists
    fs::create_dir_all(&helix_dir)?;

    Ok(helix_dir.join("update_cache.toml"))
}

async fn fetch_latest_version() -> Result<String> {
    let client = Client::builder()
        .user_agent(format!("helix-cli/{CURRENT_VERSION}"))
        .timeout(Duration::from_secs(10))
        .build()?;

    let response = client.get(GITHUB_API_URL).send().await?;

    if !response.status().is_success() {
        return Err(eyre!(
            "Failed to fetch latest version: HTTP {}",
            response.status()
        ));
    }

    let release: GitHubRelease = response.json().await?;

    // Remove 'v' prefix if present
    let version = release
        .tag_name
        .strip_prefix('v')
        .unwrap_or(&release.tag_name);
    Ok(version.to_string())
}

fn should_check_for_updates() -> Result<bool> {
    let cache_path = get_update_cache_path()?;

    if !cache_path.exists() {
        return Ok(true);
    }

    let cache_content = fs::read_to_string(&cache_path)?;
    let update_cache: UpdateCache = toml::from_str(&cache_content)?;

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let time_since_check = now.saturating_sub(update_cache.last_check);

    Ok(time_since_check >= UPDATE_CHECK_INTERVAL)
}

fn save_update_check(latest_version: Option<String>) -> Result<()> {
    let cache_path = get_update_cache_path()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let update_cache = UpdateCache {
        last_check: now,
        latest_version,
    };

    let cache_content = toml::to_string_pretty(&update_cache)?;
    fs::write(&cache_path, cache_content)?;

    Ok(())
}

fn is_newer_version(current: &str, latest: &str) -> bool {
    // Simple version comparison - assumes semantic versioning but is robust against missing zeros
    let current_parts = current
        .split('.')
        .filter_map(|s| s.parse().ok())
        .chain([0].into_iter().cycle())
        .take(3);
    let latest_parts = latest
        .split('.')
        .filter_map(|s| s.parse().ok())
        .chain([0].into_iter().cycle())
        .take(3);

    for (current_part, latest_part) in current_parts.zip(latest_parts) {
        match latest_part.cmp(&current_part) {
            std::cmp::Ordering::Greater => return true,
            std::cmp::Ordering::Less => return false,
            std::cmp::Ordering::Equal => continue,
        }
    }

    false
}

pub async fn check_for_updates() -> Result<()> {
    // Skip update check if not needed (to avoid slowing down every command)
    if !should_check_for_updates().unwrap_or(true) {
        // Still check cache for any previously found updates
        let cache_path = get_update_cache_path()?;
        let cache_content = fs::read_to_string(&cache_path)?;
        let update_cache: UpdateCache = toml::from_str(&cache_content)?;

        let Some(latest) = update_cache.latest_version else {
            return Ok(());
        };

        if is_newer_version(CURRENT_VERSION, &latest) {
            print_update_available(&latest);
        }
        return Ok(());
    }

    // Perform actual update check
    match fetch_latest_version().await {
        Ok(latest_version) => {
            if is_newer_version(CURRENT_VERSION, &latest_version) {
                print_update_available(&latest_version);
                save_update_check(Some(latest_version))?;
            } else {
                save_update_check(Some(latest_version))?;
            }
        }
        Err(_) => {
            // Silently fail - don't block CLI usage due to network issues
            save_update_check(None)?;
        }
    }

    Ok(())
}

fn print_update_available(latest_version: &str) {
    eprintln!("[UPDATE] New version available: v{CURRENT_VERSION} -> v{latest_version}",);
    eprintln!("         Run 'helix update' to upgrade");
    eprintln!();
}
