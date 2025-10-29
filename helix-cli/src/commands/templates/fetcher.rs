use super::{TemplateProcessor, TemplateSource};
use crate::project::get_helix_cache_dir;
use crate::utils::print_status;
use eyre::Result;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Result of cache validation check
enum CacheStatus {
    /// Cache is valid and matches upstream
    Valid(PathBuf),
    /// Cache is stale or doesn't exist
    Invalid,
    /// Network error occurred, but cache is available
    NetworkError(PathBuf),
    /// Network error and no cache available
    NetworkErrorNoCache,
}

/// Manages fetching and caching of templates from Git repositories
pub struct TemplateFetcher;

impl TemplateFetcher {
    /// Fetch a template from the given source, using cache when available
    /// Returns a path to a fully rendered template ready to copy
    pub fn fetch(source: &TemplateSource, variables: &HashMap<String, String>) -> Result<PathBuf> {
        Self::check_git_available()?;

        let cache_status = Self::check_cache_validity(source)?;

        match cache_status {
            CacheStatus::Valid(path) => {
                print_status("TEMPLATE", "Using cached template (up to date)");
                Ok(path)
            }
            CacheStatus::Invalid => {
                print_status("TEMPLATE", "Fetching template from git...");
                Self::fetch_and_render(source, variables)
            }
            CacheStatus::NetworkError(path) => {
                print_status(
                    "WARNING",
                    "Network error, using cached template (may be outdated)",
                );
                Ok(path)
            }
            CacheStatus::NetworkErrorNoCache => Err(eyre::eyre!(
                "Cannot fetch template: network error and no cache available. \
                     Please check your internet connection."
            )),
        }
    }

    /// Check if cache is valid by comparing with upstream commit hash
    fn check_cache_validity(source: &TemplateSource) -> Result<CacheStatus> {
        let git_url = source.to_git_url();
        let url_hash = Self::hash_url(&git_url);
        let cache_base = get_helix_cache_dir()?.join("templates").join(&url_hash);

        match Self::resolve_commit_hash(source) {
            Ok(Some(latest_commit)) => {
                let cache_path = cache_base.join(&latest_commit);

                if cache_path.exists() {
                    return Ok(CacheStatus::Valid(cache_path));
                }

                Ok(CacheStatus::Invalid)
            }
            Ok(None) => {
                if let Some(cached_commit) = Self::get_latest_cached_commit(&cache_base)? {
                    let cache_path = cache_base.join(&cached_commit);
                    return Ok(CacheStatus::NetworkError(cache_path));
                }

                Ok(CacheStatus::NetworkErrorNoCache)
            }
            Err(e) => Err(e),
        }
    }

    fn resolve_commit_hash(source: &TemplateSource) -> Result<Option<String>> {
        let git_url = source.to_git_url();
        let git_ref = source.git_ref().unwrap_or("HEAD");

        let output = Command::new("git")
            .env("GIT_TERMINAL_PROMPT", "0")
            .arg("ls-remote")
            .arg(&git_url)
            .arg(git_ref)
            .output()
            .map_err(|e| eyre::eyre!("Failed to execute git ls-remote: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Self::parse_git_error(&stderr, &git_url);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let commit_hash = stdout
            .split_whitespace()
            .next()
            .ok_or_else(|| eyre::eyre!("Invalid git ls-remote output"))?
            .to_string();

        Ok(Some(commit_hash))
    }

    /// Fetch template, render it, and cache the rendered version
    fn fetch_and_render(
        source: &TemplateSource,
        variables: &HashMap<String, String>,
    ) -> Result<PathBuf> {
        let git_url = source.to_git_url();

        let commit_hash = Self::resolve_commit_hash(source)?
            .ok_or_else(|| eyre::eyre!("Network error: cannot fetch template"))?;

        let temp_dir = Self::create_temp_dir()?;

        Self::clone_to_temp(source, &temp_dir)?;

        Self::validate_template(&temp_dir)?;

        let cache_path = Self::get_cache_path_for_commit(&git_url, &commit_hash)?;

        print_status("TEMPLATE", "Rendering template...");
        TemplateProcessor::render_to_cache(&temp_dir, &cache_path, variables)?;

        std::fs::remove_dir_all(&temp_dir).ok();

        Ok(cache_path)
    }

    fn clone_to_temp(source: &TemplateSource, temp_dir: &Path) -> Result<()> {
        let git_url = source.to_git_url();
        let mut cmd = Command::new("git");
        cmd.env("GIT_TERMINAL_PROMPT", "0")
            .arg("clone")
            .arg("--depth")
            .arg("1");

        if let Some(git_ref) = source.git_ref() {
            cmd.arg("--branch").arg(git_ref);
        }

        cmd.arg(&git_url).arg(temp_dir);

        let output = cmd
            .output()
            .map_err(|e| eyre::eyre!("Failed to execute git clone: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Self::parse_git_error(&stderr, &git_url).map(|_| ());
        }

        Ok(())
    }

    fn validate_template(template_path: &Path) -> Result<()> {
        if !template_path.join("helix.toml").exists() {
            return Err(eyre::eyre!("Invalid template: missing helix.toml"));
        }
        Ok(())
    }

    /// Get cache path for a specific commit hash
    fn get_cache_path_for_commit(url: &str, commit_hash: &str) -> Result<PathBuf> {
        let cache_base = get_helix_cache_dir()?;
        let templates_dir = cache_base.join("templates");
        let url_hash = Self::hash_url(url);

        let cache_path = templates_dir.join(url_hash).join(commit_hash);

        Ok(cache_path)
    }

    /// Hash a URL to create a directory name
    fn hash_url(url: &str) -> String {
        let mut hasher = DefaultHasher::new();
        url.hash(&mut hasher);
        let hash = hasher.finish();
        format!("{:x}", hash)
    }

    /// Get the most recent cached commit for a URL
    fn get_latest_cached_commit(url_cache_dir: &Path) -> Result<Option<String>> {
        if !url_cache_dir.exists() {
            return Ok(None);
        }

        // Find the most recently modified commit directory
        let mut entries: Vec<_> = std::fs::read_dir(url_cache_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();

        entries.sort_by_key(|e| {
            e.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });

        if let Some(latest) = entries.last()
            && let Some(name) = latest.file_name().to_str()
        {
            return Ok(Some(name.to_string()));
        }

        Ok(None)
    }

    /// Create a temporary directory for cloning
    fn create_temp_dir() -> Result<PathBuf> {
        let temp_base = std::env::temp_dir();
        let unique_name = format!("helix-template-{}", uuid::Uuid::new_v4());
        let temp_dir = temp_base.join(unique_name);
        std::fs::create_dir_all(&temp_dir)?;
        Ok(temp_dir)
    }

    fn check_git_available() -> Result<()> {
        let output = Command::new("git")
            .env("GIT_TERMINAL_PROMPT", "0")
            .arg("--version")
            .output()
            .map_err(|_| {
                eyre::eyre!("git command not found. Please install git to use templates.")
            })?;

        if !output.status.success() {
            return Err(eyre::eyre!("git command is not working properly"));
        }

        Ok(())
    }

    fn parse_git_error(stderr: &str, git_url: &str) -> Result<Option<String>> {
        if stderr.contains("Could not resolve host")
            || stderr.contains("Connection timed out")
            || stderr.contains("unable to access")
        {
            return Ok(None);
        }

        if stderr.contains("Repository not found")
            || stderr.contains("not found")
            || stderr.contains("could not read Username")
            || stderr.contains("Authentication failed")
            || stderr.contains("denied")
        {
            return Err(eyre::eyre!("Template '{}' not found or private", git_url));
        }

        Err(eyre::eyre!("Git operation failed: {}", stderr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_hash_consistent() {
        let url = "https://github.com/helix-db/basic";
        assert_eq!(
            TemplateFetcher::hash_url(url),
            TemplateFetcher::hash_url(url)
        );
    }

    #[test]
    fn test_url_hash_unique() {
        let hash1 = TemplateFetcher::hash_url("https://github.com/helix-db/basic");
        let hash2 = TemplateFetcher::hash_url("https://github.com/helix-db/advanced");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_cache_path_structure() {
        let path = TemplateFetcher::get_cache_path_for_commit(
            "https://github.com/helix-db/basic",
            "abc123",
        )
        .unwrap();
        assert!(path.to_string_lossy().contains("templates"));
        assert!(path.to_string_lossy().ends_with("abc123"));
    }
}
