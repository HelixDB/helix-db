use clap::{Args, Parser, Subcommand};

pub mod version {
    pub const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub const NAME: &str = "Helix CLI";
    pub const AUTHORS: &str = "Helix Team";
}

use version::{VERSION, NAME, AUTHORS};

#[derive(Debug, Parser)]
#[clap(name = NAME, version = VERSION, author = AUTHORS)]
pub struct HelixCLI {
    #[clap(subcommand)]
    pub command: CommandType,
}

#[derive(Debug, Subcommand)]
pub enum CommandType {
    /// Deploy a Helix project
    Deploy(DeployCommand),

    /// Re-deploy a Helix project with new queries
    Redeploy(RedeployCommand),

    /// Compile a Helix project
    Compile(CompileCommand),

    /// Lint a Helix project
    Check(LintCommand),

    /// Install the Helix repo
    Install(InstallCommand),

    /// Initialise a new Helix project
    Init(InitCommand),

    /// Test a Helix project
    Test(TestCommand),

    /// List running Helix instances
    Instances(InstancesCommand),

    /// Stop Helix instances
    Stop(StopCommand),

    /// Start a stopped Helix instance
    Start(StartCommand),

    /// Ingest data into Helix
    Ingest(IngestCommand),

    /// Give an instance a short description
    Label(LabelCommand),

    /// Save an instnaces data.mdb file
    Save(SaveCommand),

    /// Delete an instance and all its data
    Delete(DeleteCommand),
}

#[derive(Debug, Args)]
#[clap(name = "deploy", about = "Deploy a Helix project")]
pub struct DeployCommand {
    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,

    #[clap(short, long, help = "The output path")]
    pub output: Option<String>,

    #[clap(short, long, help = "Port to run the instance on")]
    pub port: Option<u16>,
}

#[derive(Debug, Args)]
#[clap(name = "redeploy", about = "Re-deploy a Helix project with new queries")]
pub struct RedeployCommand {
    #[clap(help = "Existing helix instance ID")]
    pub instance: String,

    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "compile", about = "Compile a Helix project")]
pub struct CompileCommand {
    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,

    #[clap(short, long, help = "The output path")]
    pub output: Option<String>,

    // #[clap(short, long, help = "The target platform")]
    // pub target: Option<String>,

    // #[clap(short, long, help = "Should compile in release mode")]
    // pub release: bool,
}

#[derive(Debug, Args)]
#[clap(name = "check", about = "Lint a Helix project")]
pub struct LintCommand {
    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "install", about = "Install the Helix repo")]
pub struct InstallCommand {
    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "init", about = "Initialise a new Helix project")]
pub struct InitCommand {
    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "test", about = "Test a Helix project")]
pub struct TestCommand {
    #[clap(short, long, help = "The path to the project")]
    pub path: Option<String>,

    #[clap(short, long, help = "The test to run")]
    pub test: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "instances", about = "List running Helix instances")]
pub struct InstancesCommand {}

#[derive(Debug, Args)]
#[clap(name = "stop", about = "Stop Helix instances")]
pub struct StopCommand {
    #[clap(long, help = "Stop all running instances")]
    pub all: bool,

    #[clap(help = "Instance ID to stop")]
    pub instance: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "start", about = "Start a stopped Helix instance")]
pub struct StartCommand {
    #[clap(help = "Instance ID to Start")]
    pub instance: String,
}

#[derive(Debug, Args)]
#[clap(name = "ingest", about = "Ingest data into Helix")]
pub struct IngestCommand {
    /// Type of database to ingest from ('sqlite' or 'pg')
    #[clap(short = 't', long = "type", value_parser = ["sqlite", "pg"])]
    pub db_type: String,

    /// Database connection string or path
    #[clap(short, long = "db", help = "Database connection string or path")]
    pub db_url: String,

    /// Helix instance to ingest data into
    #[clap(short = 'i', long = "instance", help = "Helixdb instance to ingest data into")]
    pub instance: String,

    /// Batch size for ingestion (only used for PostgreSQL)
    #[clap(short = 'b', long = "batch", default_value = "1000", help = "Batch size for ingestion")]
    pub batch_size: usize,

    /// Output directory for JSONL files
    #[clap(short = 'o', long = "output", default_value = "./", help = "Output directory for JSONL files")]
    pub output_dir: Option<String>,

    /// Use SSL for PostgreSQL
    #[clap(short = 's', long = "ssl", help = "Use SSL for PostgreSQL")]
    pub use_ssl: bool,
}

#[derive(Debug, Args)]
#[clap(name = "label", about = "Give an instance a short description")]
pub struct LabelCommand {
    #[clap(help = "Instance ID to label")]
    pub instance: String,

    #[clap(help = "Short description to label")]
    pub label: String,
}

#[derive(Debug, Args)]
#[clap(name = "save", about = "Save an instances data.mdb file")]
pub struct SaveCommand {
    #[clap(help = "Instance ID to save")]
    pub instance: String,

    #[clap(help = "Where to save the file to")]
    pub output: Option<String>,
}

#[derive(Debug, Args)]
#[clap(name = "delete", about = "Delete an instance and its saved data")]
pub struct DeleteCommand {
    #[clap(help = "Instance ID to delete")]
    pub instance: String,
}

#[derive(Debug)]
pub enum CliError {
    Io(std::io::Error),
    New(String),
    ConfigFileNotFound,
    CompileFailed,
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Io(e) => write!(f, "IO error: {}", e),
            CliError::New(msg) => write!(f, "{}", msg),
            CliError::ConfigFileNotFound => write!(f, "Config file not found"),
            CliError::CompileFailed => write!(f, "Failed to compile queries"),
        }
    }
}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        CliError::Io(e)
    }
}

impl From<&'static str> for CliError {
    fn from(e: &'static str) -> Self {
        CliError::New(e.to_string())
    }
}

impl From<String> for CliError {
    fn from(e: String) -> Self {
        CliError::New(e)
    }
}

impl From<sonic_rs::Error> for CliError {
    fn from(e: sonic_rs::Error) -> Self {
        CliError::New(e.to_string())
    }
}

