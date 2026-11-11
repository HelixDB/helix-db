use std::io::Write as _;
use std::{fs, path::PathBuf, time::SystemTime};

use serde::{Deserialize, Serialize};

use super::CliMetricsError;
use crate::query::{InstallationId, OssIdentity, UserId};
use crate::telemetry::DEFAULT_TELEMETRY_ENDPOINT;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricsLevel {
    Full,
    #[default]
    Basic,
    Off,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub level: MetricsLevel,
    #[serde(default)]
    pub installation_id: Option<String>,
    pub user_id: Option<String>,
    pub email: Option<String>,
    pub name: Option<String>,
    pub last_updated: u64,
    pub install_event_sent: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            level: MetricsLevel::Basic,
            installation_id: Some(InstallationId::now().to_string()),
            user_id: None,
            email: None,
            name: None,
            last_updated: unix_seconds(SystemTime::now()),
            install_event_sent: false,
        }
    }
}

impl MetricsConfig {
    #[must_use]
    pub fn telemetry_user_id(&self) -> Option<String> {
        match self.level {
            MetricsLevel::Full => self.user_id.clone(),
            MetricsLevel::Basic | MetricsLevel::Off => None,
        }
    }

    pub fn query_identity(&self) -> Result<Option<OssIdentity>, CliMetricsError> {
        if self.level == MetricsLevel::Off {
            return Ok(None);
        }
        let installation_id = self
            .installation_id
            .as_deref()
            .ok_or(CliMetricsError::Missing("installation_id"))?;
        let installation_id = InstallationId::parse(installation_id)
            .map_err(|_| CliMetricsError::InvalidInstallationId)?;
        let user_id = match (self.level, self.user_id.as_deref()) {
            (MetricsLevel::Full, Some(user_id)) => {
                Some(UserId::new(user_id).map_err(|_| CliMetricsError::Empty("user_id"))?)
            }
            (MetricsLevel::Full | MetricsLevel::Basic | MetricsLevel::Off, None)
            | (MetricsLevel::Basic | MetricsLevel::Off, Some(_)) => None,
        };
        Ok(Some(OssIdentity::new(installation_id, user_id)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryMetricsSettings {
    pub identity: OssIdentity,
    pub level: MetricsLevel,
    pub endpoint: String,
}

pub fn load_query_metrics_settings() -> Result<Option<QueryMetricsSettings>, CliMetricsError> {
    let root = metrics_root()?;
    let mut config = load_metrics_config_from(&root)?;
    if let Some(level) = std::env::var_os("HELIX_TELEMETRY_LEVEL") {
        config.level = parse_metrics_level(&level.to_string_lossy())?;
    }
    if do_not_track(std::env::var_os("DO_NOT_TRACK")) {
        config.level = MetricsLevel::Off;
    }
    if let Some(installation_id) = std::env::var_os("HELIX_TELEMETRY_INSTALLATION_ID") {
        config.installation_id = Some(installation_id.to_string_lossy().into_owned());
    }
    if let Some(user_id) = std::env::var_os("HELIX_TELEMETRY_USER_ID") {
        config.user_id = Some(user_id.to_string_lossy().into_owned());
    }
    let Some(identity) = config.query_identity()? else {
        return Ok(None);
    };
    let endpoint = std::env::var("HELIX_TELEMETRY_ENDPOINT")
        .unwrap_or_else(|_| DEFAULT_TELEMETRY_ENDPOINT.to_owned());
    Ok(Some(QueryMetricsSettings {
        identity,
        level: config.level,
        endpoint,
    }))
}

pub fn load_metrics_config() -> Result<MetricsConfig, CliMetricsError> {
    load_metrics_config_from(&metrics_root()?)
}

pub fn save_metrics_config(config: &MetricsConfig) -> Result<(), CliMetricsError> {
    save_metrics_config_to(&metrics_root()?, config)
}

pub(crate) fn metrics_root() -> Result<PathBuf, CliMetricsError> {
    let root = match std::env::var_os("HELIX_HOME").filter(|value| !value.is_empty()) {
        Some(value) => PathBuf::from(value),
        None => dirs::home_dir()
            .ok_or(CliMetricsError::HomeDirectory)?
            .join(".helix"),
    };
    fs::create_dir_all(&root)?;
    Ok(root)
}

pub(crate) fn load_metrics_config_from(
    root: &std::path::Path,
) -> Result<MetricsConfig, CliMetricsError> {
    let path = root.join("metrics.toml");
    if !path.exists() {
        let config = MetricsConfig::default();
        save_metrics_config_to(root, &config)?;
        return Ok(config);
    }
    let mut config: MetricsConfig = toml::from_str(&fs::read_to_string(path)?)?;
    if config.installation_id.is_none() {
        config.installation_id = Some(InstallationId::now().to_string());
        save_metrics_config_to(root, &config)?;
    }
    Ok(config)
}

pub(crate) fn save_metrics_config_to(
    root: &std::path::Path,
    config: &MetricsConfig,
) -> Result<(), CliMetricsError> {
    fs::create_dir_all(root)?;
    let mut temporary = tempfile::NamedTempFile::new_in(root)?;
    temporary.write_all(toml::to_string_pretty(config)?.as_bytes())?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(root.join("metrics.toml"))
        .map_err(|error| error.error)?;
    Ok(())
}

/// Parses `HELIX_TELEMETRY_LEVEL`. Boolean words are accepted too, so one
/// on/off switch (such as Foundry's `FOUNDRY_TELEMETRY`) can be passed
/// through unchanged: `on`, `true`, `yes` and `1` mean `basic`.
fn parse_metrics_level(value: &str) -> Result<MetricsLevel, CliMetricsError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "full" => Ok(MetricsLevel::Full),
        "basic" | "on" | "true" | "yes" | "1" => Ok(MetricsLevel::Basic),
        "off" | "false" | "no" | "0" => Ok(MetricsLevel::Off),
        _ => Err(CliMetricsError::InvalidEnum("query metrics level")),
    }
}

/// Whether `DO_NOT_TRACK` (<https://consoledonottrack.com>) turns telemetry
/// off: it does for any value except empty, `0`, `false` and `no`. It
/// overrides every other setting.
pub fn do_not_track(value: Option<std::ffi::OsString>) -> bool {
    value.is_some_and(|value| {
        let value = value.to_string_lossy().trim().to_ascii_lowercase();
        !matches!(value.as_str(), "" | "0" | "false" | "no")
    })
}

pub(crate) fn unix_seconds(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_accept_switch_words_and_reject_others() {
        for (value, level) in [
            ("full", MetricsLevel::Full),
            (" Basic ", MetricsLevel::Basic),
            ("on", MetricsLevel::Basic),
            ("TRUE", MetricsLevel::Basic),
            ("yes", MetricsLevel::Basic),
            ("1", MetricsLevel::Basic),
            ("off", MetricsLevel::Off),
            ("false", MetricsLevel::Off),
            ("No", MetricsLevel::Off),
            ("0", MetricsLevel::Off),
        ] {
            assert_eq!(parse_metrics_level(value).expect(value), level);
        }
        for value in ["", "maybe", "2"] {
            assert!(parse_metrics_level(value).is_err(), "{value:?}");
        }
    }

    #[test]
    fn do_not_track_turns_off_for_any_value_but_empty_or_false() {
        let dnt = |value: &str| do_not_track(Some(value.into()));
        assert!(!do_not_track(None));
        for value in ["", " ", "0", "false", "FALSE", "no"] {
            assert!(!dnt(value), "{value:?}");
        }
        for value in ["1", "true", "yes", "anything"] {
            assert!(dnt(value), "{value:?}");
        }
    }

    #[test]
    fn missing_config_defaults_to_basic() {
        let root = tempfile::tempdir().expect("temporary directory");
        let config = load_metrics_config_from(root.path()).expect("default config");
        assert_eq!(config.level, MetricsLevel::Basic);
        assert!(InstallationId::parse(config.installation_id.as_deref().unwrap()).is_ok());
        assert!(root.path().join("metrics.toml").exists());
    }

    #[test]
    fn basic_identity_is_anonymous() {
        let config = MetricsConfig {
            user_id: Some("user-1".to_string()),
            email: Some("user@example.com".to_string()),
            ..MetricsConfig::default()
        };
        assert_eq!(config.telemetry_user_id(), None);
    }

    #[test]
    fn full_identity_is_included() {
        let config = MetricsConfig {
            level: MetricsLevel::Full,
            user_id: Some("user-1".to_string()),
            email: Some("user@example.com".to_string()),
            ..MetricsConfig::default()
        };
        assert_eq!(config.telemetry_user_id(), Some("user-1".to_string()));
    }

    #[test]
    fn malformed_config_is_rejected() {
        let root = tempfile::tempdir().expect("temporary directory");
        fs::write(root.path().join("metrics.toml"), "level = [").expect("write config");
        assert!(load_metrics_config_from(root.path()).is_err());
    }

    #[test]
    fn saved_config_round_trips_and_off_is_anonymous() {
        let root = tempfile::tempdir().expect("temporary directory");
        let config = MetricsConfig {
            level: MetricsLevel::Off,
            installation_id: Some(InstallationId::now().to_string()),
            user_id: Some("user-1".to_string()),
            email: Some("user@example.com".to_string()),
            name: Some("User".to_string()),
            last_updated: 42,
            install_event_sent: true,
        };
        save_metrics_config_to(root.path(), &config).expect("save config");
        assert_eq!(
            load_metrics_config_from(root.path()).expect("load config"),
            config
        );
        assert_eq!(config.telemetry_user_id(), None);
        assert_eq!(
            unix_seconds(SystemTime::UNIX_EPOCH - std::time::Duration::from_secs(1)),
            0
        );
    }

    #[test]
    fn legacy_config_is_migrated_once_and_identity_is_stable() {
        let root = tempfile::tempdir().expect("temporary directory");
        fs::write(
            root.path().join("metrics.toml"),
            "level = \"basic\"\nlast_updated = 0\ninstall_event_sent = false\n",
        )
        .expect("write legacy config");
        let first = load_metrics_config_from(root.path()).expect("migrate config");
        let second = load_metrics_config_from(root.path()).expect("reload config");
        assert_eq!(first.installation_id, second.installation_id);
        assert!(first.query_identity().expect("identity").is_some());
    }

    #[test]
    fn query_identity_obeys_privacy_level_and_validates_values() {
        let installation_id = InstallationId::now().to_string();
        let basic = MetricsConfig {
            installation_id: Some(installation_id.clone()),
            user_id: Some("user-1".to_owned()),
            email: Some("secret@example.com".to_owned()),
            ..MetricsConfig::default()
        };
        assert!(basic.query_identity().unwrap().unwrap().user_id().is_none());
        let full = MetricsConfig {
            level: MetricsLevel::Full,
            installation_id: Some(installation_id),
            ..basic
        };
        assert_eq!(
            full.query_identity()
                .unwrap()
                .unwrap()
                .user_id()
                .unwrap()
                .as_str(),
            "user-1"
        );
        assert_eq!(
            MetricsConfig {
                level: MetricsLevel::Off,
                ..full.clone()
            }
            .query_identity()
            .unwrap(),
            None
        );
        assert!(MetricsConfig {
            installation_id: Some("not-a-uuid".to_owned()),
            ..full
        }
        .query_identity()
        .is_err());
    }
}
