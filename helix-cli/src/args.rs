use crate::types::OutputLanguage;
use clap::{Args, Parser, Subcommand};

pub mod version {
    pub const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub const NAME: &str = "Helix CLI";
    pub const AUTHORS: &str = "Helix Team";
}

use version::{AUTHORS, NAME, VERSION};

#[derive(Debug, Parser)]
#[clap(name = NAME, version = VERSION, author = AUTHORS)]
pub struct HelixCli {
    #[clap(subcommand)]
    pub command: CommandType,
}

#[derive(Debug, Subcommand)]
pub enum CommandType {
    /// Deploy a Helix project
    Deploy(DeployCommand),

    /// Update the cli and core database
    Update,

    /// Lint and Compile a Helix project
    Compile(CompileCommand),

    /// Checks the projects schema and queries
    Check(CheckCommand),

    /// Install the Helix core database
    Install(InstallCommand),

    /// Initialise a new Helix project
    Init(InitCommand),

    /// List all Helix instances
    Status,

    /// Stop Helix instances
    Stop(StopCommand),

    /// Save an instances data.mdb file
    Save(SaveCommand),

    /// Delete an instance and all its data
    Delete(DeleteCommand),

    /// Get the current version of the cli and core database
    Version,

    /// Open graph visualizer in default browser
    Visualize(VisualizeCommand),

    /// Check login credentials or login with github
    Login,

    /// Remove login credentials
    Logout,

    /// Create a new key for a cloud cluster
    #[command(name = "create-key")]
    CreateKey { cluster: String },
}

#[derive(Debug, Args)]
#[clap(name = "deploy", about = "Deploy a Helix project")]
pub struct DeployCommand {
    #[clap(long, help = "Build in release mode (default is dev)")]
    pub release: bool,

    #[clap(short, long, help = "Redeploy a remote instance of HelixDB")]
    pub remote: bool,

    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,

    #[clap(short, long, help = "Cluster id if restarting a running instance")]
    pub cluster: Option<String>,

    #[clap(long, help = "Port to run the instance on")]
    pub port: Option<u16>,

    #[clap(long, help = "Enable dev-instance feature flags, allows you to use visualizer endpoints")]
    pub dev: bool,
}

#[derive(Debug, Args)]
#[clap(name = "compile", about = "Compile a Helix project")]
pub struct CompileCommand {
    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,

    #[clap(short, long, help = "The output path")]
    pub output: Option<String>,

    #[clap(short, long, help = "The target language")]
    pub r#gen: Option<OutputLanguage>,
}

#[derive(Debug, Args)]
#[clap(name = "check", about = "Lint a Helix project")]
pub struct CheckCommand {
    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "install", about = "Install the Helix repo")]
pub struct InstallCommand {
    #[clap(
        short,
        long,
        help = "Install HelixDB from the development branch (considered unstable)"
    )]
    pub dev: bool,
}

#[derive(Debug, Args)]
#[clap(name = "init", about = "Initialise a new Helix project")]
pub struct InitCommand {
    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "stop", about = "Stop Helix instances")]
pub struct StopCommand {
    #[clap(short, long, help = "Stop all running clusters")]
    pub all: bool,

    #[clap(help = "Cluster ID to stop")]
    pub cluster: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "save", about = "Save an instances data.mdb file")]
pub struct SaveCommand {
    #[clap(help = "Cluster ID to save")]
    pub cluster: String,

    #[clap(short, long, help = "Where to save the file to")]
    pub output: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "delete", about = "Delete an cluster and its saved data")]
pub struct DeleteCommand {
    #[clap(help = "Cluster ID to delete")]
    pub cluster: Option<String>,

    #[clap(short, long, help = "Delete all clusters")]
    pub all: bool,
}

#[derive(Debug, Args)]
#[clap(name = "visualize", about = "Visualize the Helix graph")]
pub struct VisualizeCommand {
    #[clap(help = "Cluster ID to visualize")]
    pub cluster: String,
}
