//! Local image selection and pull contracts shared by configuration and CLI flags.

use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

/// A validated container tag or SHA-256 digest (without a repository).
///
/// ```
/// use helix_cli::image::ImageVersion;
/// let version: ImageVersion = "v1.2.3".parse().unwrap();
/// assert_eq!(version.reference("example/db"), "example/db:v1.2.3");
/// assert!("bad tag".parse::<ImageVersion>().is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ImageVersion(String);

impl ImageVersion {
    pub fn reference(&self, repository: &str) -> String {
        let separator = if self.0.starts_with("sha256:") {
            '@'
        } else {
            ':'
        };
        format!("{repository}{separator}{self}")
    }

    pub fn default_pull_policy(&self) -> PullPolicy {
        if self.0 == "latest" {
            PullPolicy::Always
        } else {
            PullPolicy::Missing
        }
    }
}

impl FromStr for ImageVersion {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let valid = match value.strip_prefix("sha256:") {
            Some(digest) => {
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            }
            None => {
                !value.is_empty()
                    && value.len() <= 128
                    && value.bytes().enumerate().all(|(index, byte)| {
                        byte.is_ascii_alphanumeric()
                            || byte == b'_'
                            || (index > 0 && matches!(byte, b'.' | b'-'))
                    })
            }
        };
        if !valid {
            return Err(
                "expected an image tag or sha256: followed by 64 lowercase hexadecimal digits"
                    .into(),
            );
        }
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for ImageVersion {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ImageVersion> for String {
    fn from(value: ImageVersion) -> Self {
        value.0
    }
}

impl fmt::Display for ImageVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// `Always` requires a successful registry pull; `Never` requires a cached image.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum PullPolicy {
    Always,
    Missing,
    Never,
}

#[derive(Debug, Clone, Default, clap::Args)]
pub struct ImageArgs {
    /// Override the image tag or sha256 digest for this run
    #[arg(long)]
    pub image_version: Option<ImageVersion>,
    /// Pull policy (default: always for latest, missing for other versions)
    #[arg(long, value_enum)]
    pub pull: Option<PullPolicy>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_versions_and_builds_references() {
        for tag in ["latest", "v1.2.3", "_dev", "release-1", "A"] {
            let version: ImageVersion = tag.parse().unwrap();
            assert_eq!(
                version.reference("localhost:5000/db"),
                format!("localhost:5000/db:{tag}")
            );
        }
        let digest = format!("sha256:{}", "a1".repeat(32));
        let version: ImageVersion = digest.parse().unwrap();
        assert_eq!(
            version.reference("example/db"),
            format!("example/db@{digest}")
        );
        assert_eq!(version.default_pull_policy(), PullPolicy::Missing);
        assert_eq!(
            "latest"
                .parse::<ImageVersion>()
                .unwrap()
                .default_pull_policy(),
            PullPolicy::Always
        );
        assert_eq!(
            "v1".parse::<ImageVersion>().unwrap().default_pull_policy(),
            PullPolicy::Missing
        );
        assert!("a".repeat(128).parse::<ImageVersion>().is_ok());
        for invalid in [
            "",
            "-tag",
            ".tag",
            "bad tag",
            "repo:tag",
            "repo/tag",
            "sha256:",
            &"a".repeat(129),
            &format!("sha256:{}", "A".repeat(64)),
            &format!("sha256:{}", "g".repeat(64)),
        ] {
            assert!(invalid.parse::<ImageVersion>().is_err(), "{invalid}");
        }
    }
}
