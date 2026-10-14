use crate::output::HelixTheme;
use cliclack::Theme as _;
use console::style;
use serde::Serialize;
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;

/// A user-facing error: what failed, why, and what to do next.
///
/// Rendered as a cliclack-style block on stderr in human mode and serialized
/// as `{"error": {...}}` in `--json` mode (see [`crate::output::report_error`]).
#[derive(Debug, Clone, Serialize)]
pub struct CliError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caused_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// Resources the user could have meant, e.g. when a name is ambiguous or
    /// a selection is required but no prompt is possible.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<Candidate>,
}

/// One resource listed in an error so the user (or agent) can pick it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Candidate {
    pub id: String,
    pub name: String,
}

impl CliError {
    pub fn new<S: Into<String>>(message: S) -> Self {
        Self {
            message: message.into(),
            context: None,
            caused_by: None,
            hint: None,
            candidates: Vec::new(),
        }
    }

    pub fn with_context<S: Into<String>>(mut self, context: S) -> Self {
        self.context = Some(context.into());
        self
    }

    pub fn with_hint<S: Into<String>>(mut self, hint: S) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_caused_by<S: Into<String>>(mut self, caused_by: S) -> Self {
        self.caused_by = Some(caused_by.into());
        self
    }

    pub fn with_candidates(mut self, candidates: Vec<Candidate>) -> Self {
        self.candidates = candidates;
        self
    }

    /// The most specific user-facing error in `report`: a typed CLI error if
    /// one is in the chain, otherwise the top message with its causes.
    pub fn from_report(report: &eyre::Report) -> Self {
        report
            .downcast_ref::<CliError>()
            .cloned()
            .or_else(|| {
                report
                    .downcast_ref::<ConfigError>()
                    .map(ConfigError::to_cli_error)
            })
            .or_else(|| {
                report
                    .downcast_ref::<ProjectError>()
                    .map(ProjectError::to_cli_error)
            })
            .or_else(|| {
                report
                    .downcast_ref::<PortError>()
                    .map(PortError::to_cli_error)
            })
            .unwrap_or_else(|| {
                let mut chain = report.chain().map(ToString::to_string);
                let message = chain.next().unwrap_or_default();
                let causes: Vec<String> = chain.collect();
                let error = CliError::new(message);
                if causes.is_empty() {
                    error
                } else {
                    error.with_caused_by(causes.join(": "))
                }
            })
    }

    /// The cliclack-style block written to stderr:
    ///
    /// ```text
    /// ■  message
    /// │  context
    /// │  caused by: …
    /// │  hint: …
    /// │  candidates:
    /// │    name  id
    /// ```
    pub fn render(&self) -> String {
        let bar = style("│").dim().for_stderr();
        let header = format!(
            "{}  {}",
            HelixTheme.error_symbol(),
            style(&self.message).red().bold().for_stderr()
        );
        let context = self
            .context
            .iter()
            .flat_map(|context| context.lines())
            .map(str::to_owned);
        let caused_by = self.caused_by.iter().flat_map(|cause| {
            cause.lines().enumerate().map(|(index, line)| match index {
                0 => format!("{} {line}", style("caused by:").dim().for_stderr()),
                _ => format!("  {line}"),
            })
        });
        let hint = self
            .hint
            .iter()
            .map(|hint| format!("{} {hint}", style("hint:").cyan().for_stderr()));
        let name_width = self
            .candidates
            .iter()
            .map(|candidate| console::measure_text_width(&candidate.name))
            .max()
            .unwrap_or(0);
        let candidates = (!self.candidates.is_empty())
            .then(|| "candidates:".to_owned())
            .into_iter()
            .chain(self.candidates.iter().map(|candidate| {
                if candidate.name == candidate.id {
                    format!("  {}", candidate.id)
                } else {
                    format!(
                        "  {:<name_width$}  {}",
                        candidate.name,
                        style(&candidate.id).dim().for_stderr()
                    )
                }
            }));
        std::iter::once(header)
            .chain(
                context
                    .chain(caused_by)
                    .chain(hint)
                    .chain(candidates)
                    .map(|line| format!("{bar}  {line}")),
            )
            .map(|line| line + "\n")
            .collect()
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        let Some(caused_by) = &self.caused_by else {
            return Ok(());
        };
        write!(f, ": {caused_by}")
    }
}

impl std::error::Error for CliError {}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot find home directory")]
    HomeDirNotFound,
    #[error("failed to read helix.toml at {path}: {source}")]
    ReadHelixConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse helix.toml at {path}: {source}")]
    ParseHelixConfig {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize helix.toml: {source}")]
    SerializeHelixConfig {
        #[source]
        source: toml::ser::Error,
    },
    #[error("failed to write helix.toml at {path}: {source}")]
    WriteHelixConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("project name cannot be empty in {path}")]
    EmptyProjectName { path: PathBuf },
    #[error("at least one instance must be defined in {path}")]
    MissingInstances { path: PathBuf },
    #[error("instance name cannot be empty in {path}")]
    EmptyInstanceName { path: PathBuf },
    #[error(
        "instance name '{name}' in {path} must contain only ASCII letters, digits, '-', or '_'"
    )]
    InvalidInstanceName { name: String, path: PathBuf },
    #[error("instance name '{name}' in {path} must be at most {max_len} characters")]
    InstanceNameTooLong {
        name: String,
        path: PathBuf,
        max_len: usize,
    },
    #[error(
        "local instance '{name}' uses s3 storage but has no [local.{name}.s3] config in {path}"
    )]
    MissingS3Config { name: String, path: PathBuf },
    #[error("local instance '{name}' has s3 config but storage is not set to \"s3\" in {path}")]
    UnexpectedS3Config { name: String, path: PathBuf },
    #[error("local instance '{name}' must have a non-empty S3 bucket in {path}")]
    MissingS3Bucket { name: String, path: PathBuf },
    #[error("local instance '{name}' must have a non-empty S3 prefix in {path}")]
    MissingS3Prefix { name: String, path: PathBuf },
    #[error("local instance '{name}' must have a non-empty S3 region in {path}")]
    MissingS3Region { name: String, path: PathBuf },
    #[error("local instance '{name}' has an invalid S3 endpoint URL in {path}: {message}")]
    InvalidS3Endpoint {
        name: String,
        path: PathBuf,
        message: String,
    },
    #[error("instance '{name}' not found in helix.toml")]
    InstanceNotFound { name: String },
}

#[derive(Debug, Error)]
pub enum ProjectError {
    #[error("failed to determine current directory: {source}")]
    CurrentDir {
        #[source]
        source: std::io::Error,
    },
    #[error("project configuration not found (searched from {start} up to filesystem root)")]
    ConfigNotFound { start: PathBuf },
    #[error("failed to create directory at {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is a symlink or an existing non-directory")]
    UnsafeHelixDir { path: PathBuf },
    #[error(transparent)]
    Config(Box<ConfigError>),
}

impl From<ConfigError> for ProjectError {
    fn from(e: ConfigError) -> Self {
        ProjectError::Config(Box::new(e))
    }
}

#[derive(Debug, Error)]
pub enum PortError {
    #[error("could not find available port in range {start}-{end}")]
    NoAvailablePort { start: u16, end: u16 },
}

impl ConfigError {
    pub fn to_cli_error(&self) -> CliError {
        match self {
            ConfigError::HomeDirNotFound => CliError::new("cannot find home directory"),
            ConfigError::ReadHelixConfig { path, source } => {
                CliError::new(format!("failed to read helix.toml at {}", path.display()))
                    .with_caused_by(source.to_string())
            }
            ConfigError::ParseHelixConfig { path, source } => {
                CliError::new(format!("failed to parse helix.toml at {}", path.display()))
                    .with_caused_by(source.to_string())
            }
            ConfigError::SerializeHelixConfig { source } => {
                CliError::new("failed to serialize helix.toml").with_caused_by(source.to_string())
            }
            ConfigError::WriteHelixConfig { path, source } => {
                CliError::new(format!("failed to write helix.toml at {}", path.display()))
                    .with_caused_by(source.to_string())
            }
            ConfigError::EmptyProjectName { path } => CliError::new(format!(
                "project name cannot be empty in {}",
                path.display()
            )),
            ConfigError::MissingInstances { path } => CliError::new(format!(
                "at least one instance must be defined in {}",
                path.display()
            ))
            .with_hint("add one with `helix add local --name dev` (or `helix add cloud`)"),
            ConfigError::EmptyInstanceName { path } => CliError::new(format!(
                "instance name cannot be empty in {}",
                path.display()
            )),
            ConfigError::InvalidInstanceName { name, path } => CliError::new(format!(
                "instance name '{}' in {} must contain only ASCII letters, digits, '-', or '_'",
                name,
                path.display()
            ))
            .with_hint(
                "instance names are used to build container names and local state directory \
                 paths, so characters like '/', '.', and '\\' aren't allowed",
            ),
            ConfigError::InstanceNameTooLong {
                name,
                path,
                max_len,
            } => CliError::new(format!(
                "instance name '{}' in {} must be at most {} characters",
                name,
                path.display(),
                max_len
            )),
            ConfigError::MissingS3Config { name, path } => CliError::new(format!(
                "local instance '{}' uses s3 storage but has no [local.{}.s3] config in {}",
                name,
                name,
                path.display()
            )),
            ConfigError::UnexpectedS3Config { name, path } => CliError::new(format!(
                "local instance '{}' has s3 config but storage is not set to \"s3\" in {}",
                name,
                path.display()
            )),
            ConfigError::MissingS3Bucket { name, path } => CliError::new(format!(
                "local instance '{}' must have a non-empty S3 bucket in {}",
                name,
                path.display()
            )),
            ConfigError::MissingS3Prefix { name, path } => CliError::new(format!(
                "local instance '{}' must have a non-empty S3 prefix in {}",
                name,
                path.display()
            )),
            ConfigError::MissingS3Region { name, path } => CliError::new(format!(
                "local instance '{}' must have a non-empty S3 region in {}",
                name,
                path.display()
            )),
            ConfigError::InvalidS3Endpoint {
                name,
                path,
                message,
            } => CliError::new(format!(
                "local instance '{}' has an invalid S3 endpoint URL in {}",
                name,
                path.display()
            ))
            .with_caused_by(message),
            ConfigError::InstanceNotFound { name } => {
                CliError::new(format!("instance '{}' not found in helix.toml", name))
            }
        }
    }
}

impl ProjectError {
    pub fn to_cli_error(&self) -> CliError {
        match self {
            ProjectError::CurrentDir { source } => {
                CliError::new("failed to determine current directory")
                    .with_caused_by(source.to_string())
            }
            ProjectError::ConfigNotFound { start } => CliError::new("no helix.toml found")
                .with_context(format!(
                    "searched from {} up to the filesystem root",
                    start.display()
                ))
                .with_hint("run `helix init` to create a project here"),
            ProjectError::CreateDir { path, source } => {
                CliError::new(format!("failed to create directory at {}", path.display()))
                    .with_caused_by(source.to_string())
            }
            ProjectError::UnsafeHelixDir { path } => CliError::new(format!(
                "{} is a symlink or an existing non-directory",
                path.display()
            ))
            .with_hint(
                "helix stores per-instance runtime state under .helix/<instance>; refusing to \
                 create or remove paths under it while it isn't a plain directory, since a \
                 symlink there could point outside the project. Remove or replace it with a \
                 real directory.",
            ),
            ProjectError::Config(config_error) => config_error.to_cli_error(),
        }
    }
}

impl PortError {
    pub fn to_cli_error(&self) -> CliError {
        CliError::new(self.to_string())
    }
}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => {
                CliError::new("file or directory not found").with_caused_by(err.to_string())
            }
            std::io::ErrorKind::PermissionDenied => CliError::new("permission denied")
                .with_caused_by(err.to_string())
                .with_hint("check file permissions and try again"),
            std::io::ErrorKind::InvalidInput => {
                CliError::new("invalid input").with_caused_by(err.to_string())
            }
            _ => CliError::new("I/O operation failed").with_caused_by(err.to_string()),
        }
    }
}

impl From<toml::de::Error> for CliError {
    fn from(err: toml::de::Error) -> Self {
        CliError::new("failed to parse TOML configuration")
            .with_caused_by(err.to_string())
            .with_hint("check the helix.toml file for syntax errors")
    }
}

impl From<serde_json::Error> for CliError {
    fn from(err: serde_json::Error) -> Self {
        CliError::new("failed to parse JSON").with_caused_by(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io_error(kind: std::io::ErrorKind) -> std::io::Error {
        std::io::Error::new(kind, "test failure")
    }

    #[test]
    fn render_includes_every_optional_section() {
        let rendered = CliError::new("careful")
            .with_context("first line\nsecond line")
            .with_caused_by("invalid value\nmore detail")
            .with_hint("fix the value")
            .with_candidates(vec![
                Candidate {
                    id: "ws-1".into(),
                    name: "Acme".into(),
                },
                Candidate {
                    id: "dev".into(),
                    name: "dev".into(),
                },
            ])
            .render();
        let plain = console::strip_ansi_codes(&rendered);
        assert_eq!(
            plain,
            "■  careful\n│  first line\n│  second line\n│  caused by: invalid value\n│    more detail\n│  hint: fix the value\n│  candidates:\n│    Acme  ws-1\n│    dev\n"
        );
    }

    #[test]
    fn json_omits_absent_sections() {
        assert_eq!(
            serde_json::to_value(CliError::new("boom")).unwrap(),
            serde_json::json!({"message": "boom"})
        );
        let full = serde_json::to_value(CliError::new("boom").with_hint("retry").with_candidates(
            vec![Candidate {
                id: "p-1".into(),
                name: "api".into(),
            }],
        ))
        .unwrap();
        assert_eq!(full["hint"], "retry");
        assert_eq!(full["candidates"][0]["id"], "p-1");
    }

    #[test]
    fn from_report_prefers_typed_errors_and_keeps_generic_causes() {
        let typed = eyre::Report::new(CliError::new("typed").with_hint("hint"));
        assert_eq!(CliError::from_report(&typed).hint.as_deref(), Some("hint"));

        let config = eyre::Report::new(ConfigError::InstanceNotFound { name: "qa".into() });
        assert!(CliError::from_report(&config).message.contains("'qa'"));

        let project = eyre::Report::new(ProjectError::ConfigNotFound {
            start: PathBuf::from("/tmp"),
        });
        assert_eq!(
            CliError::from_report(&project).message,
            "no helix.toml found"
        );

        let port = eyre::Report::new(PortError::NoAvailablePort { start: 1, end: 2 });
        assert!(CliError::from_report(&port).message.contains("1-2"));

        use eyre::WrapErr as _;
        let generic = Err::<(), _>(std::io::Error::other("disk full"))
            .wrap_err("write helix.toml")
            .unwrap_err();
        let error = CliError::from_report(&generic);
        assert_eq!(error.message, "write helix.toml");
        assert_eq!(error.caused_by.as_deref(), Some("disk full"));
        assert_eq!(error.to_string(), "write helix.toml: disk full");
        assert_eq!(CliError::from_report(&eyre::eyre!("plain")).caused_by, None);
    }

    #[test]
    fn config_errors_convert_to_actionable_cli_errors() {
        let path = PathBuf::from("/tmp/helix.toml");
        let parse_helix = toml::from_str::<crate::config::HelixConfig>("=").unwrap_err();
        let errors = vec![
            ConfigError::HomeDirNotFound,
            ConfigError::ReadHelixConfig {
                path: path.clone(),
                source: io_error(std::io::ErrorKind::NotFound),
            },
            ConfigError::ParseHelixConfig {
                path: path.clone(),
                source: parse_helix,
            },
            ConfigError::WriteHelixConfig {
                path: path.clone(),
                source: io_error(std::io::ErrorKind::PermissionDenied),
            },
            ConfigError::EmptyProjectName { path: path.clone() },
            ConfigError::MissingInstances { path: path.clone() },
            ConfigError::EmptyInstanceName { path: path.clone() },
            ConfigError::InvalidInstanceName {
                name: "../evil".into(),
                path: path.clone(),
            },
            ConfigError::MissingS3Config {
                name: "dev".into(),
                path: path.clone(),
            },
            ConfigError::UnexpectedS3Config {
                name: "dev".into(),
                path: path.clone(),
            },
            ConfigError::MissingS3Bucket {
                name: "dev".into(),
                path: path.clone(),
            },
            ConfigError::MissingS3Prefix {
                name: "dev".into(),
                path: path.clone(),
            },
            ConfigError::MissingS3Region {
                name: "dev".into(),
                path: path.clone(),
            },
            ConfigError::InvalidS3Endpoint {
                name: "dev".into(),
                path: path.clone(),
                message: "bad scheme".into(),
            },
            ConfigError::InstanceNotFound {
                name: "missing".into(),
            },
        ];

        for error in errors {
            let rendered = error.to_cli_error().render();
            assert!(rendered.contains("■"), "{rendered}");
        }
    }

    #[test]
    fn project_port_and_io_errors_preserve_context() {
        let path = PathBuf::from("/tmp/project");
        let project_errors = [
            ProjectError::CurrentDir {
                source: io_error(std::io::ErrorKind::NotFound),
            },
            ProjectError::ConfigNotFound {
                start: path.clone(),
            },
            ProjectError::CreateDir {
                path: path.clone(),
                source: io_error(std::io::ErrorKind::PermissionDenied),
            },
            ProjectError::from(ConfigError::MissingInstances { path: path.clone() }),
        ];
        for error in project_errors {
            assert!(!error.to_cli_error().message.is_empty());
        }

        let port = PortError::NoAvailablePort {
            start: 6_969,
            end: 6_979,
        }
        .to_cli_error();
        assert!(port.message.contains("6969-6979"));

        for (kind, expected) in [
            (std::io::ErrorKind::NotFound, "file or directory not found"),
            (std::io::ErrorKind::PermissionDenied, "permission denied"),
            (std::io::ErrorKind::InvalidInput, "invalid input"),
            (std::io::ErrorKind::Other, "I/O operation failed"),
        ] {
            let error = CliError::from(io_error(kind));
            assert_eq!(error.message, expected);
            assert!(error.caused_by.is_some());
        }
    }
}
