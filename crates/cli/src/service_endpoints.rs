//! Central service endpoint selection for production and black-box CLI tests.
//!
//! Production callers use the public Helix and GitHub endpoints. Integration
//! tests set `HELIX_TEST_HTTP_BASE_URL` on the spawned `helix` process so every
//! outbound request is redirected to one isolated local mock server.

use std::env;

const TEST_HTTP_BASE_URL_ENV: &str = "HELIX_TEST_HTTP_BASE_URL";
const DEFAULT_CLOUD_AUTHORITY: &str = "cloud.helix-db.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceEndpoint {
    Cloud,
    GitHubRelease,
    SkillsCommits,
    Metrics,
}

pub(crate) fn url(endpoint: ServiceEndpoint) -> String {
    endpoint_url(
        endpoint,
        env::var(TEST_HTTP_BASE_URL_ENV).ok().as_deref(),
        env::var("CLOUD_AUTHORITY").ok().as_deref(),
    )
}

fn endpoint_url(
    endpoint: ServiceEndpoint,
    test_base_url: Option<&str>,
    cloud_authority: Option<&str>,
) -> String {
    let Some(base_url) = test_base_url else {
        return match endpoint {
            ServiceEndpoint::Cloud => {
                normalize_cloud_authority(cloud_authority.unwrap_or(DEFAULT_CLOUD_AUTHORITY))
            }
            ServiceEndpoint::GitHubRelease => {
                "https://api.github.com/repos/helixdb/helix-db/releases/latest".to_string()
            }
            ServiceEndpoint::SkillsCommits => {
                "https://api.github.com/repos/HelixDB/skills/commits?per_page=1".to_string()
            }
            ServiceEndpoint::Metrics => "https://logs.helix-db.com/v2".to_string(),
        };
    };
    let base_url = base_url.trim_end_matches('/');

    match endpoint {
        ServiceEndpoint::Cloud => base_url.to_string(),
        ServiceEndpoint::GitHubRelease => {
            format!("{base_url}/__helix_test/github/releases/latest")
        }
        ServiceEndpoint::SkillsCommits => {
            format!("{base_url}/__helix_test/github/skills/commits?per_page=1")
        }
        ServiceEndpoint::Metrics => format!("{base_url}/__helix_test/metrics"),
    }
}

fn normalize_cloud_authority(authority: &str) -> String {
    if authority.starts_with("http://") || authority.starts_with("https://") {
        authority.to_string()
    } else if authority.starts_with("localhost") || authority.starts_with("127.0.0.1") {
        format!("http://{authority}")
    } else {
        format!("https://{authority}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_routes_every_service_to_the_mock_server() {
        let base = Some("http://127.0.0.1:4321/");

        assert_eq!(
            endpoint_url(ServiceEndpoint::Cloud, base, None),
            "http://127.0.0.1:4321"
        );
        assert_eq!(
            endpoint_url(ServiceEndpoint::GitHubRelease, base, None),
            "http://127.0.0.1:4321/__helix_test/github/releases/latest"
        );
        assert_eq!(
            endpoint_url(ServiceEndpoint::SkillsCommits, base, None),
            "http://127.0.0.1:4321/__helix_test/github/skills/commits?per_page=1"
        );
        assert_eq!(
            endpoint_url(ServiceEndpoint::Metrics, base, None),
            "http://127.0.0.1:4321/__helix_test/metrics"
        );
    }

    #[test]
    fn cloud_authority_preserves_existing_scheme_rules() {
        assert_eq!(
            normalize_cloud_authority("cloud.example.com"),
            "https://cloud.example.com"
        );
        assert_eq!(
            normalize_cloud_authority("localhost:3000"),
            "http://localhost:3000"
        );
        assert_eq!(
            normalize_cloud_authority("http://127.0.0.1:3000"),
            "http://127.0.0.1:3000"
        );
    }
}
