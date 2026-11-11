//! Pseudonymous attribution sent with every telemetry envelope.
//!
//! An `installation_id` lives in the Helix home directory, so every fresh
//! container or CI run reports a new one. These values let analysis link those
//! installations back to one project or deployment without collecting IP
//! addresses, repository names or bucket names:
//!
//! - [`ProjectId`]: SHA-256 of the git repository (`host/owner/repo`), under a
//!   fixed domain prefix.
//!   The same repository gives the same ID on a laptop, in CI, and in
//!   containers started by `helix start`, which pass it in [`PROJECT_ID_ENV`].
//! - [`DeploymentId`]: [`DEPLOYMENT_ID_ENV`] as set, or a SHA-256 of an AWS S3
//!   bucket and database path, under another domain prefix.
//! - [`Environment`]: CI provider, Kubernetes, container or host.
//! - session ID: random per telemetry runtime. Query summaries are cumulative
//!   per session, so a session's last summary is its query count.

use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::query::ValidationError;

/// Explicit deployment label, for example `acme-prod`. It replaces any derived
/// deployment ID. Helix's own traffic uses `helix-internal-ci`,
/// `helix-internal-dev` and `helix-internal-cloud`.
pub const DEPLOYMENT_ID_ENV: &str = "HELIX_DEPLOYMENT_ID";
/// Project ID computed by the CLI and passed to containers it starts, which
/// have no git checkout to derive it from.
pub const PROJECT_ID_ENV: &str = "HELIX_TELEMETRY_PROJECT_ID";

const PROJECT_DOMAIN: &[u8] = b"helix-telemetry-project-v1";
const DEPLOYMENT_DOMAIN: &[u8] = b"helix-telemetry-deployment-v1";
const CONTAINER_MARKERS: [&str; 2] = ["/.dockerenv", "/run/.containerenv"];

/// CI service a process runs in, detected from the variable it always sets.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CiProvider {
    GithubActions,
    Gitlab,
    Circleci,
    Buildkite,
    Jenkins,
    AzurePipelines,
    Bitbucket,
    AwsCodebuild,
    Travis,
    Teamcity,
    Drone,
    /// Any other service that sets the conventional `CI` variable.
    Other,
}

impl CiProvider {
    /// Detection order: named providers first, the generic `CI` flag last.
    const ALL: [Self; 12] = [
        Self::GithubActions,
        Self::Gitlab,
        Self::Circleci,
        Self::Buildkite,
        Self::Jenkins,
        Self::AzurePipelines,
        Self::Bitbucket,
        Self::AwsCodebuild,
        Self::Travis,
        Self::Teamcity,
        Self::Drone,
        Self::Other,
    ];

    const fn marker(self) -> &'static str {
        match self {
            Self::GithubActions => "GITHUB_ACTIONS",
            Self::Gitlab => "GITLAB_CI",
            Self::Circleci => "CIRCLECI",
            Self::Buildkite => "BUILDKITE",
            Self::Jenkins => "JENKINS_URL",
            Self::AzurePipelines => "TF_BUILD",
            Self::Bitbucket => "BITBUCKET_BUILD_NUMBER",
            Self::AwsCodebuild => "CODEBUILD_BUILD_ID",
            Self::Travis => "TRAVIS",
            Self::Teamcity => "TEAMCITY_VERSION",
            Self::Drone => "DRONE",
            Self::Other => "CI",
        }
    }

    fn detect(lookup: &impl Fn(&str) -> Option<String>) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|provider| is_enabled(lookup(provider.marker())))
    }

    /// Repository URL the provider exposes, when it has one.
    fn repository_url(self, lookup: &impl Fn(&str) -> Option<String>) -> Option<String> {
        let variable = match self {
            Self::GithubActions => {
                let server =
                    lookup("GITHUB_SERVER_URL").unwrap_or_else(|| "https://github.com".to_owned());
                return lookup("GITHUB_REPOSITORY")
                    .map(|repository| format!("{}/{repository}", server.trim_end_matches('/')));
            }
            Self::Gitlab => "CI_PROJECT_URL",
            Self::Circleci => "CIRCLE_REPOSITORY_URL",
            Self::Buildkite => "BUILDKITE_REPO",
            Self::Jenkins => "GIT_URL",
            Self::AzurePipelines => "BUILD_REPOSITORY_URI",
            Self::Bitbucket => "BITBUCKET_GIT_HTTP_ORIGIN",
            Self::AwsCodebuild => "CODEBUILD_SOURCE_REPO_URL",
            Self::Drone => "DRONE_GIT_HTTP_URL",
            Self::Travis | Self::Teamcity | Self::Other => return None,
        };
        lookup(variable)
    }
}

/// Where a process runs. Serialized flat into the client object as
/// `environment`, plus `ci_provider` for CI.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "environment", rename_all = "snake_case")]
pub enum Environment {
    Ci { ci_provider: CiProvider },
    Kubernetes,
    Container,
    Host,
}

/// Hash of a normalized git repository URL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectId(String);

impl ProjectId {
    /// Hashes a git remote URL. HTTPS, SSH and scp-style URLs of one
    /// repository give the same ID; credentials, ports, case and `.git` are
    /// ignored. Local paths, including Windows drive paths, give `None`.
    ///
    /// ```
    /// use helix_metrics::attribution::ProjectId;
    ///
    /// let https = ProjectId::from_repository_url("https://token@github.com/Owner/Repo.git").unwrap();
    /// let ssh = ProjectId::from_repository_url("git@github.com:owner/repo").unwrap();
    /// assert_eq!(https, ssh);
    /// assert_eq!(https.as_str().len(), 32);
    /// assert!(ProjectId::from_repository_url("/srv/git/repo.git").is_none());
    /// ```
    #[must_use]
    pub fn from_repository_url(url: &str) -> Option<Self> {
        let url = url.trim();
        let (authority, path) = match url.split_once("://") {
            Some((_, rest)) => rest.split_once('/')?,
            // As in git, a one-letter "host" in scp form is a Windows drive.
            None => url
                .split_once(':')
                .filter(|(authority, _)| authority.len() > 1)?,
        };
        let host = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        let host = host.split_once(':').map_or(host, |(host, _)| host);
        let path = path.trim_matches('/');
        let path = path
            .strip_suffix(".git")
            .unwrap_or(path)
            .trim_end_matches('/');
        if host.is_empty() || path.is_empty() || host.contains('/') || path.contains('\\') {
            return None;
        }
        let repository = format!("{host}/{path}").to_ascii_lowercase();
        Some(Self(domain_digest(PROJECT_DOMAIN, [repository.as_str()])))
    }

    fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        crate::query::context_id(value, "project_id").map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable identity of one running deployment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeploymentId(String);

impl DeploymentId {
    /// Validates an explicit deployment label.
    ///
    /// ```
    /// use helix_metrics::attribution::DeploymentId;
    ///
    /// assert_eq!(DeploymentId::new("acme-prod").unwrap().as_str(), "acme-prod");
    /// assert!(DeploymentId::new(" acme").is_err());
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        crate::query::context_id(value, "deployment_id").map(Self)
    }

    /// Derives an ID from AWS S3 storage, whose bucket names are globally
    /// unique. Custom endpoints give `None`: local S3 servers reuse the same
    /// bucket names across unrelated users.
    ///
    /// ```
    /// use helix_metrics::attribution::DeploymentId;
    ///
    /// let id = DeploymentId::for_s3("acme-data", "db/", None).unwrap();
    /// assert_eq!(id, DeploymentId::for_s3("acme-data", "db/", None).unwrap());
    /// assert_ne!(id, DeploymentId::for_s3("acme-data", "other/", None).unwrap());
    /// assert!(DeploymentId::for_s3("helix", "db/", Some("http://seaweedfs:8333")).is_none());
    /// ```
    #[must_use]
    pub fn for_s3(bucket: &str, database: &str, endpoint: Option<&str>) -> Option<Self> {
        endpoint
            .is_none()
            .then(|| Self(domain_digest(DEPLOYMENT_DOMAIN, [bucket, database])))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Attribution of one telemetry runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attribution {
    session_id: Uuid,
    environment: Environment,
    project_id: Option<ProjectId>,
    deployment_id: Option<DeploymentId>,
}

impl Attribution {
    /// Detects attribution from this process's environment variables,
    /// working directory and container markers. An explicit
    /// [`DEPLOYMENT_ID_ENV`] replaces `storage_deployment`.
    #[must_use]
    pub fn detect(storage_deployment: Option<DeploymentId>) -> Self {
        Self::from_sources(
            |name| std::env::var(name).ok(),
            std::env::current_dir().ok().as_deref(),
            CONTAINER_MARKERS
                .iter()
                .any(|marker| Path::new(marker).exists()),
            storage_deployment,
        )
    }

    fn from_sources(
        lookup: impl Fn(&str) -> Option<String>,
        working_directory: Option<&Path>,
        in_container: bool,
        storage_deployment: Option<DeploymentId>,
    ) -> Self {
        let ci_provider = CiProvider::detect(&lookup);
        let environment = match ci_provider {
            Some(ci_provider) => Environment::Ci { ci_provider },
            None if is_enabled(lookup("KUBERNETES_SERVICE_HOST")) => Environment::Kubernetes,
            None if in_container => Environment::Container,
            None => Environment::Host,
        };
        let project_id = explicit(lookup(PROJECT_ID_ENV), ProjectId::new)
            .or_else(|| {
                ci_provider
                    .and_then(|provider| provider.repository_url(&lookup))
                    .and_then(|url| ProjectId::from_repository_url(&url))
            })
            .or_else(|| {
                working_directory
                    .and_then(git_origin_url)
                    .and_then(|url| ProjectId::from_repository_url(&url))
            });
        let deployment_id =
            explicit(lookup(DEPLOYMENT_ID_ENV), DeploymentId::new).or(storage_deployment);
        Self {
            session_id: Uuid::new_v4(),
            environment,
            project_id,
            deployment_id,
        }
    }

    #[must_use]
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    #[must_use]
    pub const fn environment(&self) -> Environment {
        self.environment
    }

    #[must_use]
    pub fn project_id(&self) -> Option<&ProjectId> {
        self.project_id.as_ref()
    }

    #[must_use]
    pub fn deployment_id(&self) -> Option<&DeploymentId> {
        self.deployment_id.as_ref()
    }
}

/// Variables `helix start` copies from the host into the server container so
/// the container reports the same CI provider and deployment label.
pub fn forwarded_env_vars() -> impl Iterator<Item = &'static str> {
    CiProvider::ALL
        .into_iter()
        .map(CiProvider::marker)
        .chain([DEPLOYMENT_ID_ENV])
}

/// Telemetry never fails startup: an invalid explicit value is reported and
/// ignored, which falls back to the derived value.
fn explicit<T>(
    value: Option<String>,
    parse: impl FnOnce(String) -> Result<T, ValidationError>,
) -> Option<T> {
    parse(value?)
        .inspect_err(|error| tracing::warn!(%error, "ignoring telemetry attribution override"))
        .ok()
}

fn is_enabled(value: Option<String>) -> bool {
    value.is_some_and(|value| {
        let value = value.trim();
        !value.is_empty() && !value.eq_ignore_ascii_case("false") && value != "0"
    })
}

fn domain_digest<'a>(domain: &[u8], parts: impl IntoIterator<Item = &'a str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for part in parts {
        hasher.update([0]);
        hasher.update(part.as_bytes());
    }
    let digest: [u8; 32] = hasher.finalize().into();
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Reads `remote.origin.url` of the repository containing `start`, following
/// worktree and submodule `.git` files to the shared config.
fn git_origin_url(start: &Path) -> Option<String> {
    let (directory, dot_git) = start
        .ancestors()
        .map(|directory| (directory, directory.join(".git")))
        .find(|(_, dot_git)| dot_git.exists())?;
    let git_dir = if dot_git.is_dir() {
        dot_git
    } else {
        let pointer = std::fs::read_to_string(&dot_git).ok()?;
        let private_dir = directory.join(pointer.trim().strip_prefix("gitdir:")?.trim());
        match std::fs::read_to_string(private_dir.join("commondir")) {
            Ok(common_dir) => private_dir.join(common_dir.trim()),
            Err(_) => private_dir,
        }
    };
    origin_url(&std::fs::read_to_string(git_dir.join("config")).ok()?)
}

fn origin_url(config: &str) -> Option<String> {
    let mut in_origin = false;
    config.lines().map(str::trim).find_map(|line| {
        if line.starts_with('[') {
            in_origin = line.split_whitespace().eq(["[remote", "\"origin\"]"]);
            return None;
        }
        let (key, value) = line.split_once('=')?;
        (in_origin && key.trim().eq_ignore_ascii_case("url"))
            .then(|| value.trim().trim_matches('"').to_owned())
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn detect(variables: &[(&str, &str)], working_directory: Option<&Path>) -> Attribution {
        let variables: HashMap<String, String> = variables
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        Attribution::from_sources(
            |name| variables.get(name).cloned(),
            working_directory,
            false,
            None,
        )
    }

    fn project(url: &str) -> Option<ProjectId> {
        ProjectId::from_repository_url(url)
    }

    #[test]
    fn every_url_form_of_one_repository_has_one_project_id() {
        let expected = project("github.com:owner/repo").expect("scp-like without user");
        for url in [
            "https://github.com/owner/repo",
            "https://github.com/owner/repo.git",
            "https://github.com/owner/repo/",
            "https://x-access-token:secret@github.com/Owner/Repo.git",
            "http://github.com:443/owner/repo",
            "ssh://git@github.com:22/owner/repo.git",
            "git@github.com:owner/repo.git",
            "  git@GitHub.com:/owner/repo.git/  ",
        ] {
            assert_eq!(project(url).as_ref(), Some(&expected), "{url}");
        }
        assert_ne!(
            project("https://gitlab.com/owner/repo"),
            Some(expected.clone())
        );
        assert_ne!(project("https://github.com/owner/other"), Some(expected));
    }

    #[test]
    fn local_and_malformed_repositories_have_no_project_id() {
        for url in [
            "",
            "repo",
            "/srv/git/repo.git",
            "file:///srv/git/repo.git",
            "https://github.com",
            "https://github.com/",
            "git@github.com:",
            "./relative/path:repo",
            "C:\\work\\repo",
            "C:/repos/project.git",
            "d:repos/project",
        ] {
            assert_eq!(project(url), None, "{url}");
        }
    }

    #[test]
    fn detects_each_ci_provider_and_ignores_disabled_flags() {
        for provider in CiProvider::ALL {
            assert_eq!(
                detect(&[(provider.marker(), "true")], None).environment(),
                Environment::Ci {
                    ci_provider: provider
                },
                "{provider:?}"
            );
        }
        for disabled in ["", " ", "false", "FALSE", "0"] {
            assert_eq!(
                detect(&[("CI", disabled)], None).environment(),
                Environment::Host
            );
        }
        assert_eq!(
            detect(&[("CI", "true"), ("GITLAB_CI", "true")], None).environment(),
            Environment::Ci {
                ci_provider: CiProvider::Gitlab
            }
        );
    }

    #[test]
    fn detects_kubernetes_then_container_then_host() {
        let kubernetes = [("KUBERNETES_SERVICE_HOST", "10.0.0.1")];
        assert_eq!(
            detect(&kubernetes, None).environment(),
            Environment::Kubernetes
        );
        let container = Attribution::from_sources(|_| None, None, true, None);
        assert_eq!(container.environment(), Environment::Container);
        assert_eq!(detect(&[], None).environment(), Environment::Host);
        assert_eq!(
            detect(
                &[("CI", "1"), ("KUBERNETES_SERVICE_HOST", "10.0.0.1")],
                None
            )
            .environment(),
            Environment::Ci {
                ci_provider: CiProvider::Other
            }
        );
    }

    #[test]
    fn ci_repository_variables_match_the_local_remote() {
        let expected = project("git@github.com:owner/repo.git");
        let cases: [&[(&str, &str)]; 9] = [
            &[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_REPOSITORY", "owner/repo"),
            ],
            &[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_SERVER_URL", "https://github.com/"),
                ("GITHUB_REPOSITORY", "Owner/Repo"),
            ],
            &[
                ("GITLAB_CI", "true"),
                ("CI_PROJECT_URL", "https://github.com/owner/repo"),
            ],
            &[
                ("CIRCLECI", "true"),
                ("CIRCLE_REPOSITORY_URL", "git@github.com:owner/repo.git"),
            ],
            &[
                ("BUILDKITE", "true"),
                ("BUILDKITE_REPO", "https://github.com/owner/repo.git"),
            ],
            &[
                ("JENKINS_URL", "https://ci"),
                ("GIT_URL", "https://github.com/owner/repo"),
            ],
            &[
                ("TF_BUILD", "True"),
                ("BUILD_REPOSITORY_URI", "https://github.com/owner/repo"),
            ],
            &[
                ("CODEBUILD_BUILD_ID", "b"),
                ("CODEBUILD_SOURCE_REPO_URL", "https://github.com/owner/repo"),
            ],
            &[
                ("DRONE", "true"),
                ("DRONE_GIT_HTTP_URL", "https://github.com/owner/repo.git"),
            ],
        ];
        for variables in cases {
            assert_eq!(
                detect(variables, None).project_id,
                expected,
                "{variables:?}"
            );
        }
        let bitbucket = [
            ("BITBUCKET_BUILD_NUMBER", "1"),
            (
                "BITBUCKET_GIT_HTTP_ORIGIN",
                "http://bitbucket.org/owner/repo",
            ),
        ];
        assert_eq!(
            detect(&bitbucket, None).project_id,
            project("git@bitbucket.org:owner/repo.git")
        );
        for provider in [CiProvider::Travis, CiProvider::Teamcity, CiProvider::Other] {
            assert_eq!(
                detect(&[(provider.marker(), "true")], None).project_id,
                None
            );
        }
    }

    #[test]
    fn project_id_prefers_explicit_then_ci_then_git_remote() {
        let checkout = tempfile::tempdir().expect("checkout");
        std::fs::create_dir(checkout.path().join(".git")).expect("git dir");
        std::fs::write(
            checkout.path().join(".git/config"),
            "[remote \"origin\"]\n\turl = https://github.com/local/repo.git\n",
        )
        .expect("git config");
        let explicit = "0123456789abcdef0123456789abcdef";

        let attribution = detect(
            &[
                (PROJECT_ID_ENV, explicit),
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_REPOSITORY", "ci/repo"),
            ],
            Some(checkout.path()),
        );
        assert_eq!(
            attribution.project_id().map(ProjectId::as_str),
            Some(explicit)
        );
        assert_eq!(
            detect(
                &[("GITHUB_ACTIONS", "true"), ("GITHUB_REPOSITORY", "ci/repo")],
                Some(checkout.path())
            )
            .project_id,
            project("https://github.com/ci/repo")
        );
        assert_eq!(
            detect(&[], Some(checkout.path())).project_id,
            project("https://github.com/local/repo")
        );
        assert_eq!(
            detect(&[(PROJECT_ID_ENV, "bad\nvalue")], Some(checkout.path())).project_id,
            project("https://github.com/local/repo")
        );
        assert_eq!(detect(&[], None).project_id, None);
    }

    #[test]
    fn deployment_id_prefers_explicit_over_storage() {
        let storage = DeploymentId::for_s3("bucket", "db/", None);
        let with = |variables: &[(&str, &str)]| {
            let variables: HashMap<&str, &str> = variables.iter().copied().collect();
            Attribution::from_sources(
                |name| variables.get(name).map(|value| (*value).to_owned()),
                None,
                false,
                storage.clone(),
            )
        };
        assert_eq!(
            with(&[(DEPLOYMENT_ID_ENV, "helix-internal-ci")])
                .deployment_id()
                .map(DeploymentId::as_str),
            Some("helix-internal-ci")
        );
        assert_eq!(with(&[]).deployment_id, storage);
        let oversized = "x".repeat(65);
        assert_eq!(
            with(&[(DEPLOYMENT_ID_ENV, oversized.as_str())]).deployment_id,
            storage
        );
        assert_ne!(
            DeploymentId::for_s3("bucket", "db/", None),
            DeploymentId::for_s3("bucket-db/", "", None),
            "parts are separated so concatenations cannot collide"
        );
    }

    #[test]
    fn sessions_are_unique_per_runtime() {
        assert_ne!(
            detect(&[], None).session_id(),
            detect(&[], None).session_id()
        );
    }

    #[test]
    fn reads_origin_through_worktree_and_submodule_git_files() {
        let root = tempfile::tempdir().expect("root");
        let common = root.path().join("main/.git");
        std::fs::create_dir_all(common.join("worktrees/feature")).expect("worktree dir");
        std::fs::write(
            common.join("config"),
            "[core]\n\turl = https://example.com/core\n[remote \"upstream\"]\n\turl = https://github.com/upstream/repo\n[remote  \"origin\"]\n\t# url = https://github.com/comment/repo\n\tURL = \"git@github.com:owner/repo.git\"\n",
        )
        .expect("config");
        std::fs::write(common.join("worktrees/feature/commondir"), "../..\n").expect("commondir");
        let worktree = root.path().join("feature");
        std::fs::create_dir_all(worktree.join("nested/dir")).expect("worktree");
        std::fs::write(
            worktree.join(".git"),
            format!("gitdir: {}\n", common.join("worktrees/feature").display()),
        )
        .expect("worktree pointer");
        assert_eq!(
            git_origin_url(&worktree.join("nested/dir")).as_deref(),
            Some("git@github.com:owner/repo.git")
        );

        let submodule = root.path().join("main/vendor/lib");
        std::fs::create_dir_all(common.join("modules/lib")).expect("module dir");
        std::fs::write(
            common.join("modules/lib/config"),
            "[remote \"origin\"]\n\turl = https://github.com/vendor/lib\n",
        )
        .expect("module config");
        std::fs::create_dir_all(&submodule).expect("submodule");
        std::fs::write(submodule.join(".git"), "gitdir: ../../.git/modules/lib\n")
            .expect("submodule pointer");
        assert_eq!(
            git_origin_url(&submodule).as_deref(),
            Some("https://github.com/vendor/lib")
        );
    }

    #[test]
    fn repositories_without_origin_or_config_have_no_url() {
        let root = tempfile::tempdir().expect("root");
        assert_eq!(git_origin_url(root.path()), None);
        std::fs::create_dir(root.path().join(".git")).expect("git dir");
        assert_eq!(git_origin_url(root.path()), None);
        std::fs::write(
            root.path().join(".git/config"),
            "[remote \"upstream\"]\n\turl = https://github.com/upstream/repo\n",
        )
        .expect("config");
        assert_eq!(git_origin_url(root.path()), None);
        std::fs::remove_dir_all(root.path().join(".git")).expect("remove git dir");
        std::fs::write(root.path().join(".git"), "not a pointer").expect("bad pointer");
        assert_eq!(git_origin_url(root.path()), None);
    }

    #[test]
    fn environment_serializes_flat() {
        #[derive(Serialize, Deserialize)]
        struct Client {
            #[serde(flatten)]
            environment: Option<Environment>,
        }
        let encoded = serde_json::to_string(&Client {
            environment: Some(Environment::Ci {
                ci_provider: CiProvider::GithubActions,
            }),
        })
        .expect("encode");
        assert_eq!(
            encoded,
            r#"{"environment":"ci","ci_provider":"github_actions"}"#
        );
        assert_eq!(
            serde_json::to_string(&Client {
                environment: Some(Environment::Container)
            })
            .expect("encode"),
            r#"{"environment":"container"}"#
        );
        assert_eq!(
            serde_json::to_string(&Client { environment: None }).expect("encode"),
            "{}"
        );
    }

    #[test]
    fn forwards_every_marker_and_the_deployment_label() {
        let forwarded: Vec<_> = forwarded_env_vars().collect();
        assert!(CiProvider::ALL
            .iter()
            .all(|provider| forwarded.contains(&provider.marker())));
        assert!(forwarded.contains(&DEPLOYMENT_ID_ENV));
        assert!(!forwarded.contains(&PROJECT_ID_ENV));
    }
}
