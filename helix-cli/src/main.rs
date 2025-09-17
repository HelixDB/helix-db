use clap::{Parser, Subcommand};
use color_eyre::owo_colors::OwoColorize;
use eyre::Result;

mod commands;
mod config;
mod docker;
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

        #[clap(long, default_value = "empty")]
        template: String,

        #[clap(subcommand)]
        cloud: CloudDeploymentTypeCommand,
    },

    /// Validate project configuration and queries
    Check {
        /// Instance to check (defaults to all instances)
        instance: Option<String>,
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
    /// Initialize Helix deployment
    Helix,
    /// Initialize ECR deployment
    Ecr,
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
        #[clap(long, default_value = "true")]
        public: bool,
    },
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
            cloud,
        } => commands::init::run(path, template, cloud).await,
        Commands::Check { instance } => commands::check::run(instance).await,
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
    };

    // Shutdown metrics sender
    metrics_sender.shutdown().await?;

    // Handle result with proper error formatting
    if let Err(e) = result {
        // Check if this is already a formatted error (eyre with our custom display)
        eprintln!("{}", format!("error: {e}").red().bold());
        std::process::exit(1);
    }

    Ok(())
}
