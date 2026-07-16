use dirs::home_dir;
use eyre::{eyre, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const UPDATE_CHECK_INTERVAL: u64 = 24 * 60 * 60; // 24 hours in seconds

/// Latest commit on the default branch of the skills repo. `skills add` pulls
/// the branch HEAD, so a new commit here means installed skills are stale.
/// Source identifier recorded in the skills lockfile for the Helix skill pack.
const HELIX_SKILLS_SOURCE: &str = "HelixDB/skills";

/// Returns true when the user has opted out of the background update check via
/// `HELIX_NO_UPDATE_CHECK` or `HELIX_DISABLE_UPDATE_CHECK`. Lets sandboxes, CI,
/// and restricted-network environments skip the GitHub API call (and its
/// up-to-10s timeout) on the first command of a fresh machine.
fn update_check_disabled() -> bool {
    env_disables_update_check(
        std::env::var_os("HELIX_NO_UPDATE_CHECK"),
        std::env::var_os("HELIX_DISABLE_UPDATE_CHECK"),
    )
}

/// Pure core of [`update_check_disabled`] so the opt-out logic can be unit
/// tested without mutating process-global environment state.
fn env_disables_update_check(no_update_check: Option<OsString>, disable: Option<OsString>) -> bool {
    no_update_check.is_some() || disable.is_some()
}

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

    let response = client
        .get(crate::service_endpoints::url(
            crate::service_endpoints::ServiceEndpoint::GitHubRelease,
        ))
        .send()
        .await?;

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

/// Check for updates and return the latest version if an update is available.
/// Returns `Some(latest_version)` when an update is available, `None` otherwise.
pub async fn check_for_updates() -> Result<Option<String>> {
    // Honor the opt-out before touching the cache or the network, so restricted
    // environments never block on the GitHub API.
    if update_check_disabled() {
        return Ok(None);
    }

    // Skip update check if not needed (to avoid slowing down every command)
    if !should_check_for_updates().unwrap_or(true) {
        // Still check cache for any previously found updates
        let cache_path = get_update_cache_path()?;
        let cache_content = fs::read_to_string(&cache_path)?;
        let update_cache: UpdateCache = toml::from_str(&cache_content)?;

        let Some(latest) = update_cache.latest_version else {
            return Ok(None);
        };

        if is_newer_version(CURRENT_VERSION, &latest) {
            return Ok(Some(latest));
        }
        return Ok(None);
    }

    // Perform actual update check
    let latest_version = match fetch_latest_version().await {
        Ok(latest_version) => latest_version,
        Err(_) => {
            // Silently fail - don't block CLI usage due to network issues
            save_update_check(None)?;
            return Ok(None);
        }
    };

    if is_newer_version(CURRENT_VERSION, &latest_version) {
        save_update_check(Some(latest_version.clone()))?;
        return Ok(Some(latest_version));
    }

    save_update_check(Some(latest_version))?;

    Ok(None)
}

/// Get the current version of the CLI.
pub const fn current_version() -> &'static str {
    CURRENT_VERSION
}

// --- Skills update check -----------------------------------------------------
//
// The Helix agent skills are installed out-of-band by the `skills` CLI
// (`npx skills add HelixDB/skills`). There is no reliable read-only "is an
// update available" command (`skills check` actually mutates), so we detect
// staleness ourselves: compare the skills repo's latest commit SHA against the
// SHA that was current the last time the user refreshed. This reuses the same
// 24h cache + opt-out as the binary update check and never shells out to npx.

#[derive(Serialize, Deserialize, Default)]
struct SkillsCache {
    last_check: u64,
    /// Repo HEAD SHA at the last refresh; `None` until we establish a baseline.
    applied_sha: Option<String>,
    /// Cached verdict served between 24h network checks.
    update_available: bool,
}

#[derive(Deserialize)]
struct GitHubCommit {
    sha: String,
}

fn get_skills_cache_path() -> Result<PathBuf> {
    let home = home_dir().ok_or_else(|| eyre!("Cannot find home directory"))?;
    let helix_dir = home.join(".helix");
    fs::create_dir_all(&helix_dir)?;
    Ok(helix_dir.join("skills_cache.toml"))
}

/// Paths the `skills` CLI may use for its global lockfile.
fn skills_lockfile_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(home) = home_dir() {
        paths.push(home.join(".agents").join(".skill-lock.json"));
    }
    if let Some(state) = std::env::var_os("XDG_STATE_HOME") {
        paths.push(PathBuf::from(state).join("skills").join(".skill-lock.json"));
    }
    paths
}

/// True when the Helix skill pack appears in the global skills lockfile. A plain
/// substring scan — fast enough to run on every invocation, no subprocess.
pub fn skills_installed() -> bool {
    skills_lockfile_paths().iter().any(|path| {
        fs::read_to_string(path)
            .map(|contents| contents.contains(HELIX_SKILLS_SOURCE))
            .unwrap_or(false)
    })
}

async fn fetch_latest_skills_sha() -> Result<String> {
    let client = Client::builder()
        .user_agent(format!("helix-cli/{CURRENT_VERSION}"))
        .timeout(Duration::from_secs(10))
        .build()?;

    let response = client
        .get(crate::service_endpoints::url(
            crate::service_endpoints::ServiceEndpoint::SkillsCommits,
        ))
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(eyre!(
            "Failed to fetch latest skills version: HTTP {}",
            response.status()
        ));
    }

    let commits: Vec<GitHubCommit> = response.json().await?;
    commits
        .into_iter()
        .next()
        .map(|c| c.sha)
        .ok_or_else(|| eyre!("skills repo returned no commits"))
}

fn read_skills_cache() -> SkillsCache {
    get_skills_cache_path()
        .and_then(|path| Ok(fs::read_to_string(path)?))
        .and_then(|contents| Ok(toml::from_str(&contents)?))
        .unwrap_or_default()
}

fn save_skills_cache(cache: &SkillsCache) -> Result<()> {
    let path = get_skills_cache_path()?;
    fs::write(path, toml::to_string_pretty(cache)?)?;
    Ok(())
}

/// Check whether the installed Helix skills are out of date. Returns `true` only
/// when skills are installed and the repo HEAD has moved past the recorded
/// baseline. Honors the `HELIX_NO_UPDATE_CHECK` opt-out and caches for 24h so it
/// never blocks more than once a day. Notify-only — never mutates skills.
pub async fn check_skills_update() -> bool {
    if update_check_disabled() || !skills_installed() {
        return false;
    }

    let mut cache = read_skills_cache();

    // Serve the cached verdict if we checked within the interval.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if cache.last_check != 0 && now.saturating_sub(cache.last_check) < UPDATE_CHECK_INTERVAL {
        return cache.update_available;
    }

    let latest = match fetch_latest_skills_sha().await {
        Ok(latest) => latest,
        Err(_) => {
            // Throttle retries but don't surface a notice on network failure.
            cache.last_check = now;
            cache.update_available = false;
            let _ = save_skills_cache(&cache);
            return false;
        }
    };

    cache.last_check = now;
    cache.update_available = match &cache.applied_sha {
        // First observation: treat the current HEAD as the baseline so we never
        // false-positive on a user who just installed.
        None => {
            cache.applied_sha = Some(latest);
            false
        }
        Some(applied) => *applied != latest,
    };

    let verdict = cache.update_available;
    let _ = save_skills_cache(&cache);
    verdict
}

/// Reset the skills baseline after a refresh so the notice clears. Best-effort:
/// removing the cache makes the next [`check_skills_update`] re-baseline to the
/// current repo HEAD.
pub fn record_skills_refreshed() {
    if let Ok(path) = get_skills_cache_path()
        && path.exists()
    {
        let _ = fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_check_enabled_when_no_env_vars_set() {
        assert!(!env_disables_update_check(None, None));
    }

    #[test]
    fn update_check_disabled_by_either_env_var() {
        assert!(env_disables_update_check(Some(OsString::from("1")), None));
        assert!(env_disables_update_check(None, Some(OsString::from("1"))));
        assert!(env_disables_update_check(
            Some(OsString::from("1")),
            Some(OsString::from("1"))
        ));
    }

    #[test]
    fn update_check_disabled_even_when_value_is_empty() {
        // Presence is what matters (`HELIX_NO_UPDATE_CHECK=` still opts out),
        // matching how `var_os` reports a set-but-empty variable.
        assert!(env_disables_update_check(Some(OsString::new()), None));
    }

    #[test]
    fn semantic_version_comparison_handles_prefixes_and_missing_components() {
        assert!(is_newer_version("3.0.8", "3.0.9"));
        assert!(is_newer_version("3.0", "3.1.0"));
        assert!(is_newer_version("2.9.9", "3.0.0"));
        assert!(!is_newer_version("3.0.8", "3.0.8"));
        assert!(!is_newer_version("3.1.0", "3.0.9"));
        assert!(!is_newer_version("3.0.8", "not-a-version"));
    }

    #[test]
    fn update_cache_round_trips() {
        let cache = UpdateCache {
            last_check: 42,
            latest_version: Some("4.0.0".to_string()),
        };
        let encoded = toml::to_string(&cache).unwrap();
        let decoded: UpdateCache = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded.last_check, 42);
        assert_eq!(decoded.latest_version.as_deref(), Some("4.0.0"));
    }
}
