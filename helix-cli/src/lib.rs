// Library interface for helix-cli to enable testing
use clap::Subcommand;

pub mod cleanup;
pub mod commands;
pub mod config;
pub mod docker;
pub mod errors;
pub mod github_issue;
pub mod metrics_sender;
pub mod project;
pub mod update;
pub mod utils;

#[derive(Subcommand)]
pub enum AuthAction {
    /// Login to Helix cloud
    Login,
    /// Logout from Helix cloud
    Logout,
    /// Create a new API key
    CreateKey {
        /// Cluster ID
        cluster: String,
    },
}

#[derive(Subcommand)]
pub enum MetricsAction {
    /// Enable metrics collection
    Full,
    /// Disable metrics collection
    Basic,
    /// Disable metrics collection
    Off,
    /// Show metrics status
    Status,
}

#[derive(Subcommand)]
pub enum CloudDeploymentTypeCommand {
    /// Initialize Helix Cloud deployment
    #[clap(name = "cloud")]
    Helix {
        /// Region for Helix cloud instance (default: us-east-1)
        #[clap(long, default_value = "us-east-1")]
        region: Option<String>,

        /// Instance name
        #[clap(short, long)]
        name: Option<String>,
    },
    /// Initialize ECR deployment
    Ecr {
        /// Instance name
        #[clap(short, long)]
        name: Option<String>,
    },
    /// Initialize Fly.io deployment
    Fly {
        /// Authentication type
        #[clap(long, default_value = "cli")]
        auth: String,

        /// volume size
        #[clap(long, default_value = "20")]
        volume_size: u16,

        /// vm size
        #[clap(long, default_value = "shared-cpu-4x")]
        vm_size: String,

        /// privacy
        #[clap(long, default_value = "false")]
        private: bool,

        /// Instance name
        #[clap(short, long)]
        name: Option<String>,
    },

    /// Initialize Local deployment
    Local {
        /// Instance name
        #[clap(short, long)]
        name: Option<String>,
    },
}

impl CloudDeploymentTypeCommand {
    pub fn name(&self) -> Option<String> {
        match self {
            CloudDeploymentTypeCommand::Helix { name, .. } => name.clone(),
            CloudDeploymentTypeCommand::Ecr { name } => name.clone(),
            CloudDeploymentTypeCommand::Fly { name, .. } => name.clone(),
            CloudDeploymentTypeCommand::Local { name } => name.clone(),
        }
    }
}

#[cfg(test)]
mod tests;
