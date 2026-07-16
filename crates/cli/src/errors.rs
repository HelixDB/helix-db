use color_eyre::owo_colors::OwoColorize;
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum CliErrorSeverity {
    Error,
    Warning,
    Info,
}

impl CliErrorSeverity {
    pub fn label(&self) -> &'static str {
        match self {
            CliErrorSeverity::Error => "error",
            CliErrorSeverity::Warning => "warning",
            CliErrorSeverity::Info => "info",
        }
    }

    pub fn color_code<T: AsRef<str>>(&self, text: T) -> String {
        match self {
            CliErrorSeverity::Error => text.as_ref().red().bold().to_string(),
            CliErrorSeverity::Warning => text.as_ref().yellow().bold().to_string(),
            CliErrorSeverity::Info => text.as_ref().blue().bold().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CliError {
    pub severity: CliErrorSeverity,
    pub message: String,
    pub context: Option<String>,
    pub hint: Option<String>,
    pub file_path: Option<String>,
    pub caused_by: Option<String>,
}

impl CliError {
    pub fn new<S: Into<String>>(message: S) -> Self {
        Self {
            severity: CliErrorSeverity::Error,
            message: message.into(),
            context: None,
            hint: None,
            file_path: None,
            caused_by: None,
        }
    }

    pub fn warning<S: Into<String>>(message: S) -> Self {
        Self {
            severity: CliErrorSeverity::Warning,
            message: message.into(),
            context: None,
            hint: None,
            file_path: None,
            caused_by: None,
        }
    }

    #[allow(unused)]
    pub fn info<S: Into<String>>(message: S) -> Self {
        Self {
            severity: CliErrorSeverity::Info,
            message: message.into(),
            context: None,
            hint: None,
            file_path: None,
            caused_by: None,
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

    #[allow(unused)]
    pub fn with_file_path<S: Into<String>>(mut self, file_path: S) -> Self {
        self.file_path = Some(file_path.into());
        self
    }

    pub fn with_caused_by<S: Into<String>>(mut self, caused_by: S) -> Self {
        self.caused_by = Some(caused_by.into());
        self
    }

    pub fn render(&self) -> String {
        let mut output = String::new();

        // Error header: "error[C001]: message" or "error: message"
        let header = format!("{}: {}", self.severity.label(), self.message);
        output.push_str(&self.severity.color_code(header));
        output.push('\n');

        // File path if available
        if let Some(file_path) = &self.file_path {
            output.push_str(&format!("  {} {}\n", "-->".blue().bold(), file_path.bold()));
        }

        // Context if available
        if let Some(context) = &self.context {
            output.push('\n');
            // Add indented context with box drawing
            for line in context.lines() {
                output.push_str(&format!("   {} {}\n", "│".blue().bold(), line));
            }
        }

        // Caused by if available
        if let Some(caused_by) = &self.caused_by {
            output.push('\n');
            output.push_str(&format!(
                "   {} {}: {}\n",
                "│".blue().bold(),
                "caused by".bold(),
                caused_by
            ));
        }

        // Hint if available
        if let Some(hint) = &self.hint {
            output.push('\n');
            output.push_str(&format!(
                "   {} {}: {}\n",
                "=".blue().bold(),
                "help".bold(),
                hint
            ));
        }

        output
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.render())
    }
}

impl std::error::Error for CliError {}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot find home directory")]
    HomeDirNotFound,
    #[error("failed to create config directory at {path}: {source}")]
    CreateWorkspaceDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read workspace config at {path}: {source}")]
    ReadWorkspaceConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse workspace config at {path}: {source}")]
    ParseWorkspaceConfig {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize workspace config: {source}")]
    SerializeWorkspaceConfig {
        #[source]
        source: toml::ser::Error,
    },
    #[error("failed to write workspace config at {path}: {source}")]
    WriteWorkspaceConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
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
    #[error("Enterprise instance '{name}' must have a non-empty cluster_id in {path}")]
    MissingClusterId { name: String, path: PathBuf },
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
            ConfigError::CreateWorkspaceDir { path, source } => CliError::new(format!(
                "failed to create config directory at {}",
                path.display()
            ))
            .with_caused_by(source.to_string()),
            ConfigError::ReadWorkspaceConfig { path, source } => CliError::new(format!(
                "failed to read workspace config at {}",
                path.display()
            ))
            .with_caused_by(source.to_string()),
            ConfigError::ParseWorkspaceConfig { path, source } => CliError::new(format!(
                "failed to parse workspace config at {}",
                path.display()
            ))
            .with_caused_by(source.to_string()),
            ConfigError::SerializeWorkspaceConfig { source } => {
                CliError::new("failed to serialize workspace config")
                    .with_caused_by(source.to_string())
            }
            ConfigError::WriteWorkspaceConfig { path, source } => CliError::new(format!(
                "failed to write workspace config at {}",
                path.display()
            ))
            .with_caused_by(source.to_string()),
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
            .with_hint("add one with `helix add local --name dev` (or `helix add enterprise`)"),
            ConfigError::EmptyInstanceName { path } => CliError::new(format!(
                "instance name cannot be empty in {}",
                path.display()
            )),
            ConfigError::MissingClusterId { name, path } => CliError::new(format!(
                "Enterprise instance '{}' must have a non-empty cluster_id in {}",
                name,
                path.display()
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
            ProjectError::ConfigNotFound { start } => {
                config_error("project configuration not found")
                    .with_file_path(start.display().to_string())
                    .with_context(format!(
                        "searched from {} up to filesystem root",
                        start.display()
                    ))
            }
            ProjectError::CreateDir { path, source } => {
                CliError::new(format!("failed to create directory at {}", path.display()))
                    .with_caused_by(source.to_string())
            }
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

#[allow(unused)]
pub type CliResult<T> = Result<T, CliError>;

// Convenience functions for common error patterns with error codes
#[allow(unused)]
pub fn config_error<S: Into<String>>(message: S) -> CliError {
    CliError::new(message).with_hint("run `helix init` if you need to create a new project")
}

#[allow(unused)]
pub fn file_error<S: Into<String>>(message: S, file_path: S) -> CliError {
    CliError::new(message).with_file_path(file_path)
}

#[allow(unused)]
pub fn docker_error<S: Into<String>>(message: S) -> CliError {
    CliError::new(message).with_hint("ensure Docker is running and accessible")
}

#[allow(unused)]
pub fn network_error<S: Into<String>>(message: S) -> CliError {
    CliError::new(message).with_hint("check your internet connection and try again")
}

#[allow(unused)]
pub fn project_error<S: Into<String>>(message: S) -> CliError {
    CliError::new(message).with_hint("ensure you're in a valid helix project directory")
}

#[allow(unused)]
pub fn cloud_error<S: Into<String>>(message: S) -> CliError {
    CliError::new(message).with_hint("run `helix auth login` to authenticate with Helix Cloud")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io_error(kind: std::io::ErrorKind) -> std::io::Error {
        std::io::Error::new(kind, "test failure")
    }

    #[test]
    fn cli_error_render_includes_every_optional_section_and_severity() {
        let rendered = CliError::warning("careful")
            .with_file_path("helix.toml")
            .with_context("first line\nsecond line")
            .with_caused_by("invalid value")
            .with_hint("fix the value")
            .render();
        assert!(rendered.contains("warning: careful"));
        assert!(rendered.contains("helix.toml"));
        assert!(rendered.contains("first line"));
        assert!(rendered.contains("second line"));
        assert!(rendered.contains("caused by"));
        assert!(rendered.contains("help"));
        assert_eq!(CliErrorSeverity::Error.label(), "error");
        assert_eq!(CliErrorSeverity::Info.label(), "info");
        assert!(CliError::info("notice")
            .to_string()
            .contains("info: notice"));
    }

    #[test]
    fn config_errors_convert_to_actionable_cli_errors() {
        let path = PathBuf::from("/tmp/helix.toml");
        let parse_workspace = toml::from_str::<crate::config::WorkspaceConfig>("=").unwrap_err();
        let parse_helix = toml::from_str::<crate::config::HelixConfig>("=").unwrap_err();
        let errors = vec![
            ConfigError::HomeDirNotFound,
            ConfigError::CreateWorkspaceDir {
                path: path.clone(),
                source: io_error(std::io::ErrorKind::PermissionDenied),
            },
            ConfigError::ReadWorkspaceConfig {
                path: path.clone(),
                source: io_error(std::io::ErrorKind::NotFound),
            },
            ConfigError::ParseWorkspaceConfig {
                path: path.clone(),
                source: parse_workspace,
            },
            ConfigError::WriteWorkspaceConfig {
                path: path.clone(),
                source: io_error(std::io::ErrorKind::PermissionDenied),
            },
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
            ConfigError::MissingClusterId {
                name: "prod".into(),
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
            assert!(rendered.contains("error:"), "{rendered}");
            assert!(!rendered.trim().is_empty());
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
            assert!(error.to_cli_error().render().contains("error:"));
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
