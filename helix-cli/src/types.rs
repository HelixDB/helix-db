use std::{cmp::Ordering, fmt};

use clap::{Subcommand, ValueEnum};

#[derive(Debug, PartialEq, Eq)]
pub struct Version {
    major: u32,
    minor: u32,
    patch: u32,
}

impl Version {
    pub fn parse(version: &str) -> Result<Self, String> {
        let version = version.trim_start_matches('v');

        let parts: Vec<&str> = version.split('.').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid version format: {version}"));
        }

        let major = parts[0]
            .parse::<u32>()
            .map_err(|_| format!("Invalid major version: {}", parts[0]))?;
        let minor = parts[1]
            .parse::<u32>()
            .map_err(|_| format!("Invalid minor version: {}", parts[1]))?;
        let patch = parts[2]
            .parse::<u32>()
            .map_err(|_| format!("Invalid patch version: {}", parts[2]))?;

        Ok(Version {
            major,
            minor,
            patch,
        })
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.major.cmp(&other.major) {
            Ordering::Equal => match self.minor.cmp(&other.minor) {
                Ordering::Equal => self.patch.cmp(&other.patch),
                other => other,
            },
            other => other,
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[derive(Debug, Subcommand, Clone, ValueEnum)]
#[clap(name = "output")]
pub enum OutputLanguage {
    #[clap(name = "rust", alias = "rs")]
    Rust,
    #[clap(name = "typescript", alias = "ts")]
    TypeScript,
}

impl PartialEq for OutputLanguage {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (OutputLanguage::TypeScript, OutputLanguage::TypeScript)
                | (OutputLanguage::Rust, OutputLanguage::Rust)
        )
    }
}


pub enum BuildMode {
    Dev,
    Release,
}

impl BuildMode {
    pub fn from_release(release: bool) -> Self {
        if release {
            BuildMode::Release
        } else {
            BuildMode::Dev
        }
    }
    
    pub fn to_path(&self) -> &'static str {
        match self {
            BuildMode::Dev => "debug",
            BuildMode::Release => "release",
        }
    }
}
