use clap::{Parser, Subcommand};
use eyre::Result;

mod commands;
mod config;
mod docker;
mod env_utils;
mod errors;
mod metrics_sender;
mod project;
mod update;
mod utils;

#[derive(Parser)]
#[command(name = "Helix CLI")]
#[command(version)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new Helix project with helix.toml
    Init {
        /// Project directory (defaults to current directory)
        #[clap(short, long)]
        path: Option<String>,

        #[clap(short, long, default_value = "empty")]
        template: String,

        /// Queries directory path (defaults to ./db/)
        #[clap(short = 'q', long = "queries-path", default_value = "./db/")]
        queries_path: String,

        #[clap(subcommand)]
        cloud: Option<CloudDeploymentTypeCommand>,
    },

    /// Add a new instance to an existing Helix project
    Add {
        #[clap(subcommand)]
        cloud: CloudDeploymentTypeCommand,
    },

    /// Validate project configuration and queries
    Check {
        /// Instance to check (defaults to all instances)
        instance: Option<String>,
    },

    /// Compile project queries into the workspace
    Compile {
        /// Directory containing helix.toml (defaults to current directory or project root)
        #[clap(short, long)]
        path: Option<String>,

        /// Path to output compiled queries
        #[clap(short, long)]
        output: Option<String>,
    },

    /// Build and compile project for an instance
    Build {
        /// Instance name to build
        instance: String,
    },

    /// Deploy/start an instance
    Push {
        /// Instance name to push
        instance: String,
    },

    /// Pull .hql files from instance back to local project
    Pull {
        /// Instance name to pull from
        instance: String,
    },

    /// Start an instance (doesn't rebuild)
    Start {
        /// Instance name to start
        instance: String,
    },

    /// Stop an instance
    Stop {
        /// Instance name to stop
        instance: String,
    },

    /// Show status of all instances
    Status,

    /// Cloud operations (login, keys, etc.)
    Auth {
        #[clap(subcommand)]
        action: AuthAction,
    },

    /// Prune containers, images and workspace (preserves volumes)
    Prune {
        /// Instance to prune (if not specified, prunes unused resources)
        instance: Option<String>,

        /// Prune all instances in project
        #[clap(short, long)]
        all: bool,
    },

    /// Delete an instance completely
    Delete {
        /// Instance name to delete
        instance: String,
    },

    /// Manage metrics collection
    Metrics {
        #[clap(subcommand)]
        action: MetricsAction,
    },

    /// Update to the latest version
    Update {
        /// Force update even if already on latest version
        #[clap(long)]
        force: bool,
    },

    /// Migrate v1 project to v2 format
    Migrate {
        /// Project directory to migrate (defaults to current directory)
        #[clap(short, long)]
        path: Option<String>,

        /// Directory to move .hx files to (defaults to ./db/)
        #[clap(short = 'q', long = "queries-dir", default_value = "./db/")]
        queries_dir: String,

        /// Name for the default local instance (defaults to "dev")
        #[clap(short, long, default_value = "dev")]
        instance_name: String,

        /// Port for local instance (defaults to 6969)
        #[clap(long, default_value = "6969")]
        port: u16,

        /// Show what would be migrated without making changes
        #[clap(long)]
        dry_run: bool,

        /// Skip creating backup of v1 files
        #[clap(long)]
        no_backup: bool,
    },
}

#[derive(Subcommand)]
enum AuthAction {
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
enum MetricsAction {
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
enum CloudDeploymentTypeCommand {
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
    fn name(&self) -> Option<String> {
        match self {
            CloudDeploymentTypeCommand::Helix { name, .. } => name.clone(),
            CloudDeploymentTypeCommand::Ecr { name } => name.clone(),
            CloudDeploymentTypeCommand::Fly { name, .. } => name.clone(),
            CloudDeploymentTypeCommand::Local { name } => name.clone(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize error reporting
    color_eyre::install()?;

    // Initialize metrics sender
    let metrics_sender = metrics_sender::MetricsSender::new()?;

    // Send CLI install event (only first time)
    metrics_sender.send_cli_install_event_if_first_time();

    // Check for updates before processing commands
    update::check_for_updates().await?;

    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Init {
            path,
            template,
            queries_path,
            cloud,
        } => commands::init::run(path, template, queries_path, cloud).await,
        Commands::Add { cloud } => commands::add::run(cloud).await,
        Commands::Check { instance } => commands::check::run(instance).await,
        Commands::Compile { output, path } => commands::compile::run(output, path).await,
        Commands::Build { instance } => commands::build::run(instance, &metrics_sender)
            .await
            .map(|_| ()),
        Commands::Push { instance } => commands::push::run(instance, &metrics_sender).await,
        Commands::Pull { instance } => commands::pull::run(instance).await,
        Commands::Start { instance } => commands::start::run(instance).await,
        Commands::Stop { instance } => commands::stop::run(instance).await,
        Commands::Status => commands::status::run().await,
        Commands::Auth { action } => commands::auth::run(action).await,
        Commands::Prune { instance, all } => commands::prune::run(instance, all).await,
        Commands::Delete { instance } => commands::delete::run(instance).await,
        Commands::Metrics { action } => commands::metrics::run(action).await,
        Commands::Update { force } => commands::update::run(force).await,
        Commands::Migrate {
            path,
            queries_dir,
            instance_name,
            port,
            dry_run,
            no_backup,
        } => commands::migrate::run(path, queries_dir, instance_name, port, dry_run, no_backup).await,
    };

    // Shutdown metrics sender
    metrics_sender.shutdown().await?;

    // Handle result with proper error formatting
    if let Err(e) = result {
        eprintln!("{e}");
        std::process::exit(1);
    }

    Ok(())
}
