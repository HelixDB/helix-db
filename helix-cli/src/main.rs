use clap::builder::styling::{AnsiColor, Color, RgbColor, Style, Styles};
use clap::{ArgGroup, Parser, Subcommand};
use color_eyre::owo_colors::OwoColorize;
use eyre::Result;
use helix_cli::{
    AddTarget, AuthAction, ClusterConfigAction, ConfigAction, InitTarget, MetricsAction,
    ProjectConfigAction, SkillsAction, WorkspaceConfigAction, commands, errors, metrics_sender,
    output, update,
};
use std::io::IsTerminal;
use tui_banner::{Align, Banner, ColorMode, Fill, Gradient, Palette};

/// Helix brand orange, matching the welcome banner.
const HELIX_ORANGE: Color = Color::Rgb(RgbColor(255, 165, 54));

/// Coloured help styling applied to every command (`--help`).
///
/// clap colours the structural parts of help output — section headers, the
/// usage line, flag literals, and value placeholders — automatically when
/// stdout is a TTY (and honours `NO_COLOR`). Free-form prose in `long_about`
/// and `after_long_help` is left uncoloured on purpose so it stays readable
/// when piped or redirected.
const HELP_STYLES: Styles = Styles::styled()
    .header(Style::new().bold().fg_color(Some(HELIX_ORANGE)))
    .usage(Style::new().bold().fg_color(Some(HELIX_ORANGE)))
    .literal(
        Style::new()
            .bold()
            .fg_color(Some(Color::Ansi(AnsiColor::Cyan))),
    )
    .placeholder(Style::new().fg_color(Some(Color::Ansi(AnsiColor::Green))))
    .error(
        Style::new()
            .bold()
            .fg_color(Some(Color::Ansi(AnsiColor::Red))),
    )
    .valid(Style::new().fg_color(Some(Color::Ansi(AnsiColor::Green))))
    .invalid(
        Style::new()
            .bold()
            .fg_color(Some(Color::Ansi(AnsiColor::Yellow))),
    );

#[derive(Parser)]
#[command(name = "Helix CLI")]
#[command(version)]
#[command(styles = HELP_STYLES)]
struct Cli {
    /// Suppress output (errors and final result only)
    #[arg(long, global = true)]
    quiet: bool,

    /// Show detailed output with timing information
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a v2 Helix project
    Init {
        /// Project directory (defaults to current directory)
        #[arg(short, long, global = true)]
        path: Option<String>,
        /// Install the Helix agent skills + docs MCP (prompted when interactive)
        #[arg(long, conflicts_with = "no_skills")]
        skills: bool,
        /// Skip installing the Helix agent skills + docs MCP
        #[arg(long = "no-skills", conflicts_with = "skills")]
        no_skills: bool,
        #[command(subcommand)]
        target: Option<InitTarget>,
    },

    /// Bootstrap a first Helix app for a coding agent
    #[command(alias = "cook")]
    Chef {},

    /// Add a local v2 or Enterprise Cloud instance
    Add {
        /// Project directory (defaults to current directory)
        #[arg(short, long, global = true)]
        path: Option<String>,
        #[command(subcommand)]
        target: Option<AddTarget>,
    },

    /// Start a local v2 instance in the background
    #[command(alias = "run")]
    Start {
        /// Instance name to start
        instance: Option<String>,
        /// Run in the foreground and stop on Ctrl-C
        #[arg(long, conflicts_with = "detach")]
        foreground: bool,
        /// Run in the background (default)
        #[arg(long, hide = true)]
        detach: bool,
        /// Override local port for this run
        #[arg(long)]
        port: Option<u16>,
        /// Use on-disk storage backed by a local MinIO container for this run
        #[arg(long)]
        disk: bool,
        /// Persist the resolved port/storage settings back to helix.toml
        #[arg(long)]
        persist: bool,
    },

    /// Stop a background local v2 instance
    Stop {
        /// Instance name to stop
        instance: Option<String>,
    },

    /// Restart a background local v2 instance
    Restart {
        /// Instance name to restart
        instance: Option<String>,
    },

    /// Show local and Enterprise Cloud instance status
    Status {
        /// Instance name to show, defaults to all instances
        instance: Option<String>,
    },

    /// View logs for a local or Enterprise Cloud instance
    Logs {
        /// Instance name
        instance: Option<String>,
        /// Follow logs
        #[arg(long, short = 'f')]
        follow: bool,
        /// Query historical logs with time range for Enterprise Cloud
        #[arg(long, short = 'r')]
        range: bool,
        /// Start time (ISO 8601)
        #[arg(long, requires = "range")]
        start: Option<String>,
        /// End time (ISO 8601)
        #[arg(long, requires = "range")]
        end: Option<String>,
    },

    /// Send a query to a running Helix instance
    #[command(group(
        ArgGroup::new("query_input")
            .required(true)
            .args(["file", "json", "ts", "ts_file"])
    ))]
    // Use the compact (short) help layout for both `-h` and `--help`. clap's
    // long-help layout hardcodes a blank line between every option, which is
    // too sparse here, so we render the short layout and supply examples via
    // `after_help` instead of `after_long_help`.
    #[command(disable_help_flag = true)]
    #[command(after_help = r#"Examples:
  helix query --file examples/request.json
  helix query --file examples/request.json --dry-run
  helix query -e 'readBatch().varAs("c", g().nWithLabel("User").count()).returning(["c"])'

Docs: https://docs.helix-db.com/cli/command-reference/query"#)]
    Query {
        /// Print help
        #[arg(short = 'h', long = "help", action = clap::ArgAction::HelpShort)]
        help: Option<bool>,
        /// Instance to query (default: dev)
        instance: Option<String>,
        /// Query from a JSON request file
        #[arg(
            short,
            long,
            value_name = "REQUEST.json",
            help_heading = "Input (pick one)"
        )]
        file: Option<String>,
        /// Query from an inline JSON string
        #[arg(long, value_name = "JSON", help_heading = "Input (pick one)")]
        json: Option<String>,
        /// Query from a TypeScript DSL expression, like `mysql -e`
        #[arg(
            short = 'e',
            long = "ts",
            value_name = "TS",
            help_heading = "Input (pick one)"
        )]
        ts: Option<String>,
        /// Query from a TypeScript DSL file
        #[arg(
            long = "ts-file",
            value_name = "QUERY.ts",
            help_heading = "Input (pick one)"
        )]
        ts_file: Option<String>,
        /// Override the host (local instances only)
        #[arg(long, value_name = "HOST", help_heading = "Connection")]
        host: Option<String>,
        /// Override the port (local instances only)
        #[arg(long, value_name = "PORT", help_heading = "Connection")]
        port: Option<u16>,
        /// Pre-warm caches with X-Helix-Warm (read requests only)
        #[arg(long, help_heading = "Output")]
        warm: bool,
        /// Print compact single-line JSON
        #[arg(long, help_heading = "Output")]
        compact: bool,
        /// Print the dynamic query request JSON without sending it
        #[arg(long, help_heading = "Output")]
        dry_run: bool,
    },

    /// Deploy an Enterprise Cloud instance
    Push {
        /// Enterprise instance name to deploy
        instance: Option<String>,
        /// Deprecated Helix Cloud dev deploy override; ignored for Enterprise deploys
        #[arg(long, hide = true)]
        dev: bool,
    },

    /// Enterprise Cloud auth operations
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },

    /// Configure workspace, project, and Enterprise cluster defaults
    #[command(hide = true)]
    Config {
        #[command(subcommand)]
        action: Option<ConfigAction>,
    },

    /// Manage active Enterprise Cloud workspace selection
    Workspace {
        #[command(subcommand)]
        action: Option<WorkspaceConfigAction>,
    },

    /// Manage linked Enterprise Cloud project selection
    Project {
        #[command(subcommand)]
        action: Option<ProjectConfigAction>,
    },

    /// List and inspect Enterprise Cloud clusters
    Cluster {
        #[command(subcommand)]
        action: Option<ClusterConfigAction>,
    },

    /// Sync Enterprise Cloud metadata into helix.toml
    Sync {
        /// Enterprise instance name
        instance: Option<String>,
        /// Overwrite local/remote source during reconciliation without confirmation prompts
        #[arg(short = 'y', long)]
        yes: bool,
        /// Show what would change without applying anything
        #[arg(long, conflicts_with = "yes")]
        dry_run: bool,
    },

    /// Prune local v2 containers/workspaces
    Prune {
        /// Instance to prune
        instance: Option<String>,
        /// Prune all local instances
        #[arg(short, long)]
        all: bool,
        /// Skip confirmation prompts
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Delete an instance from helix.toml and local runtime state
    Delete {
        /// Instance name to delete
        instance: String,
        /// Skip confirmation prompts
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Install, update, and list the Helix agent skills
    Skills {
        #[command(subcommand)]
        action: SkillsAction,
    },

    /// Manage metrics collection
    Metrics {
        #[command(subcommand)]
        action: MetricsAction,
    },

    /// Update to the latest CLI version
    Update {
        /// Force update even if already on latest version
        #[arg(long)]
        force: bool,
        /// Update to the last v1-compatible CLI version
        #[arg(long)]
        v1: bool,
    },

    /// Send feedback to the Helix team
    Feedback {
        /// Feedback message
        message: Option<String>,
    },

    // --- Removed v2 commands -------------------------------------------------
    // Hidden so they don't clutter `--help`, but caught explicitly to return a
    // helpful "this moved" message instead of clap's bare "unrecognized
    // subcommand". The trailing args make `helix compile path --flag` route here
    // (a friendly error) rather than failing on an unexpected-argument parse.
    /// (removed) HelixDB v2 validates queries server-side; there is no compile step
    #[command(hide = true)]
    Compile {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, hide = true)]
        args: Vec<String>,
    },
    /// (removed) HelixDB v2 validates queries server-side; there is no check step
    #[command(hide = true)]
    Check {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, hide = true)]
        args: Vec<String>,
    },
    /// (removed) use `helix push` to deploy an Enterprise instance
    #[command(hide = true)]
    Deploy {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, hide = true)]
        args: Vec<String>,
    },
}

/// Build the friendly error shown when an agent guesses a removed query
/// command (`helix compile` / `helix check`). HelixDB v2 has no client-side
/// compile step — queries are validated server-side when sent to a running
/// instance.
fn removed_query_command_error(command: &str) -> eyre::Report {
    errors::CliError::new(format!("`helix {command}` is not a command in HelixDB v2"))
        .with_hint(
            "HelixDB v2 validates queries server-side — there is no compile/check step. \
             Send a dynamic query to a running instance with \
             `helix query <instance> --file <request.json>`.",
        )
        .into()
}

/// Build the friendly error shown when `helix deploy` is guessed instead of
/// the real `helix push`.
fn removed_deploy_command_error() -> eyre::Report {
    errors::CliError::new("`helix deploy` is not a command in HelixDB v2")
        .with_hint("Use `helix push <instance>` to deploy an Enterprise Cloud instance.")
        .into()
}

fn display_welcome(update_available: Option<String>, skills_update_available: bool) {
    let use_color = std::io::stdout().is_terminal();

    if let Ok(banner) = Banner::new("> HELIX DB") {
        let banner = banner
            .color_mode(ColorMode::TrueColor)
            .gradient(Gradient::vertical(Palette::from_hex(&[
                "#ff7f17", "#e36600", "#8f4000",
            ])))
            .fill(Fill::Keep)
            .dither()
            .targets("░▒▓")
            .checker(3)
            .align(Align::Center)
            .padding(3)
            .render();
        println!("{banner}");
    }

    let version = update::current_version();
    if use_color {
        println!(
            "  {} {}\n",
            "Helix DB CLI".bold(),
            format!("v{}", version).dimmed()
        );
    } else {
        println!("  Helix DB CLI v{}\n", version);
    }

    if let Some(latest_version) = update_available {
        println!("  Update available: v{} -> v{}", version, latest_version);
        println!("  Run 'helix update' to upgrade\n");
    }

    if skills_update_available {
        println!("  Helix skills update available");
        println!("  Run 'helix skills update' to refresh\n");
    }

    print_section("Getting Started", use_color);
    print_command(
        "helix chef",
        "Bootstrap a Helix app with an AI agent",
        use_color,
    );
    print_command("helix init", "Create a new project", use_color);
    print_command(
        "helix add",
        "Add a local or Enterprise Cloud instance",
        use_color,
    );

    print_section("Local Development", use_color);
    print_command(
        "helix start <instance>",
        "Start a local instance in the background",
        use_color,
    );
    print_command(
        "helix status",
        "Show local and cloud instance status",
        use_color,
    );
    print_command(
        "helix logs <instance> -f",
        "Follow logs for an instance",
        use_color,
    );
    print_command(
        "helix query <instance> --file request.json",
        "Send a dynamic query",
        use_color,
    );

    print_section("HelixDB Cloud", use_color);
    print_command("helix auth login", "Login to the cloud", use_color);
    print_command(
        "helix push <instance>",
        "Deploy a cloud instance",
        use_color,
    );
    print_command(
        "helix sync <instance>",
        "Sync queries and config with a cloud instance",
        use_color,
    );

    println!();
    println!("Docs: https://docs.helix-db.com");
    println!("Rust DSL: https://docs.rs/helix-enterprise-ql")
}

fn print_section(title: &str, use_color: bool) {
    println!();
    if use_color {
        println!("{}", title.bold());
    } else {
        println!("{title}");
    }
    println!();
}

fn print_command(cmd: &str, desc: &str, use_color: bool) {
    print_command_w(cmd, desc, 38, use_color);
}

fn print_command_w(cmd: &str, desc: &str, width: usize, use_color: bool) {
    let padded = format!("{cmd:<width$}");
    if use_color {
        println!(
            "  {} {}",
            padded.truecolor(255, 165, 54).bold(),
            desc.dimmed()
        );
    } else {
        println!("  {padded} {desc}");
    }
}

/// True when the invocation is a bare top-level help request (`helix help`,
/// `helix --help`, `helix -h`) that should render our grouped overview. A help
/// flag/word that follows a subcommand (e.g. `helix query --help`, `helix help
/// query`) returns false so clap renders that command's own detailed help.
fn wants_top_level_help() -> bool {
    is_top_level_help_request(std::env::args().skip(1))
}

/// Pure core of [`wants_top_level_help`] so the arg matching can be unit tested
/// without touching the process argv.
fn is_top_level_help_request<I, S>(args: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut args = args.into_iter();
    match args.next().as_ref().map(AsRef::as_ref) {
        Some("-h") | Some("--help") => true,
        // `helix help` alone is ours; `helix help <command>` falls through to
        // clap so it prints that command's detailed help.
        Some("help") => args.next().is_none(),
        _ => false,
    }
}

/// Render a clean, grouped overview of every command — used for `helix help`,
/// `helix --help`, and `helix -h`. Subcommand-level detail still comes from
/// clap's per-command `--help` (e.g. `helix query --help`).
fn print_help() {
    let use_color = std::io::stdout().is_terminal();
    let version = update::current_version();
    const W: usize = 14;

    if use_color {
        println!(
            "{} {}",
            "Helix DB CLI".bold(),
            format!("v{version}").dimmed()
        );
    } else {
        println!("Helix DB CLI v{version}");
    }
    println!();
    println!("Usage: helix [OPTIONS] <COMMAND>");

    print_section("Getting started", use_color);
    print_command_w(
        "chef",
        "Bootstrap a Helix app with a coding agent (alias: cook)",
        W,
        use_color,
    );
    print_command_w(
        "init",
        "Scaffold a new project (init local | init cloud)",
        W,
        use_color,
    );
    print_command_w(
        "add",
        "Add a local or Cloud instance to an existing project",
        W,
        use_color,
    );

    print_section("Local development", use_color);
    print_command_w(
        "start",
        "Start a local instance in the background (alias: run)",
        W,
        use_color,
    );
    print_command_w("stop", "Stop a background local instance", W, use_color);
    print_command_w(
        "restart",
        "Restart a background local instance",
        W,
        use_color,
    );
    print_command_w(
        "status",
        "Show local and Cloud instance status",
        W,
        use_color,
    );
    print_command_w("logs", "View or follow instance logs", W, use_color);
    print_command_w(
        "query",
        "Send a dynamic query to POST /v1/query",
        W,
        use_color,
    );
    print_command_w(
        "prune",
        "Remove Helix-owned local containers and state",
        W,
        use_color,
    );
    print_command_w("delete", "Delete an instance from helix.toml", W, use_color);

    print_section("Helix Cloud", use_color);
    print_command_w("auth", "Log in/out and manage Cloud API keys", W, use_color);
    print_command_w("push", "Deploy an Enterprise Cloud instance", W, use_color);
    print_command_w("sync", "Sync Cloud metadata into helix.toml", W, use_color);
    print_command_w(
        "workspace",
        "Manage the active Cloud workspace",
        W,
        use_color,
    );
    print_command_w("project", "Manage the linked Cloud project", W, use_color);
    print_command_w("cluster", "List and inspect Cloud clusters", W, use_color);

    print_section("CLI", use_color);
    print_command_w(
        "skills",
        "Install, update, and list Helix agent skills",
        W,
        use_color,
    );
    print_command_w("metrics", "Manage telemetry collection", W, use_color);
    print_command_w(
        "update",
        "Update the CLI to the latest version",
        W,
        use_color,
    );
    print_command_w("feedback", "Send feedback to the Helix team", W, use_color);
    print_command_w("help", "Show this help", W, use_color);

    print_section("Options", use_color);
    print_command_w("--quiet", "Errors and final result only", W, use_color);
    print_command_w(
        "-v, --verbose",
        "Detailed output with timing information",
        W,
        use_color,
    );
    print_command_w("-h, --help", "Show this help", W, use_color);
    print_command_w("-V, --version", "Show the CLI version", W, use_color);

    println!();
    println!("Run 'helix <command> --help' for details on a specific command.");
    println!("Docs: https://docs.helix-db.com");
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    // Render our grouped overview for a bare top-level help request before doing
    // any setup — keeps `helix help` / `helix --help` instant and offline. clap
    // still owns per-command help (`helix query --help`) and the welcome banner
    // on a no-arg invocation.
    if wants_top_level_help() {
        print_help();
        return Ok(());
    }

    let metrics_sender = metrics_sender::MetricsSender::new()?;
    metrics_sender.send_cli_install_event_if_first_time();
    let update_available = update::check_for_updates().await?;
    let skills_update_available = update::check_skills_update().await;

    let cli = Cli::parse();
    output::Verbosity::set(output::Verbosity::from_flags(cli.quiet, cli.verbose));

    let result = match cli.command {
        None => {
            display_welcome(update_available, skills_update_available);
            Ok(())
        }
        Some(Commands::Init {
            path,
            skills,
            no_skills,
            target,
        }) => {
            let skills = if skills {
                Some(true)
            } else if no_skills {
                Some(false)
            } else {
                None
            };
            commands::init::run(path, target, skills).await
        }
        Some(Commands::Chef {}) => commands::chef::run(&metrics_sender).await,
        Some(Commands::Add { path, target }) => commands::add::run(path, target).await,
        Some(Commands::Start {
            instance,
            foreground,
            detach: _,
            port,
            disk,
            persist,
        }) => commands::start::run(instance, foreground, port, disk, persist).await,
        Some(Commands::Stop { instance }) => commands::stop::run(instance).await,
        Some(Commands::Restart { instance }) => commands::restart::run(instance).await,
        Some(Commands::Status { instance }) => commands::status::run(instance).await,
        Some(Commands::Logs {
            instance,
            follow,
            range,
            start,
            end,
        }) => commands::logs::run(instance, follow, range, start, end).await,
        Some(Commands::Query {
            instance,
            file,
            json,
            ts,
            ts_file,
            warm,
            host,
            port,
            compact,
            dry_run,
            ..
        }) => {
            commands::query::run(
                instance, file, json, ts, ts_file, warm, host, port, compact, dry_run,
            )
            .await
        }
        Some(Commands::Push { instance, dev }) => {
            commands::push::run(instance, dev, &metrics_sender).await
        }
        Some(Commands::Auth { action }) => commands::auth::run(action).await,
        Some(Commands::Config { action }) => commands::config::run(action).await,
        Some(Commands::Workspace { action }) => commands::config::run_workspace(action).await,
        Some(Commands::Project { action }) => commands::config::run_project(action).await,
        Some(Commands::Cluster { action }) => commands::config::run_cluster(action).await,
        Some(Commands::Sync {
            instance,
            yes,
            dry_run,
        }) => commands::sync::run(instance, yes, dry_run).await,
        Some(Commands::Prune { instance, all, yes }) => {
            commands::prune::run(instance, all, yes).await
        }
        Some(Commands::Delete { instance, yes }) => commands::delete::run(instance, yes).await,
        Some(Commands::Skills { action }) => commands::skills::run(action).await,
        Some(Commands::Metrics { action }) => commands::metrics::run(action).await,
        Some(Commands::Update { force, v1 }) => commands::update::run(force, v1).await,
        Some(Commands::Feedback { message }) => commands::feedback::run(message).await,
        Some(Commands::Compile { .. }) => Err(removed_query_command_error("compile")),
        Some(Commands::Check { .. }) => Err(removed_query_command_error("check")),
        Some(Commands::Deploy { .. }) => Err(removed_deploy_command_error()),
    };

    metrics_sender.shutdown().await?;

    if let Err(e) = result {
        if let Some(cli_error) = e.downcast_ref::<errors::CliError>() {
            eprint!("{}", cli_error.render());
        } else if let Some(config_error) = e.downcast_ref::<errors::ConfigError>() {
            eprint!("{}", config_error.to_cli_error().render());
        } else if let Some(project_error) = e.downcast_ref::<errors::ProjectError>() {
            eprint!("{}", project_error.to_cli_error().render());
        } else if let Some(port_error) = e.downcast_ref::<errors::PortError>() {
            eprint!("{}", port_error.to_cli_error().render());
        } else {
            eprintln!("{e}");
        }
        std::process::exit(1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_defaults_to_background() {
        let cli = Cli::parse_from(["helix", "start", "qa"]);

        match cli.command {
            Some(Commands::Start {
                instance,
                foreground,
                detach,
                port,
                disk,
                persist,
            }) => {
                assert_eq!(instance.as_deref(), Some("qa"));
                assert!(!foreground);
                assert!(!detach);
                assert_eq!(port, None);
                assert!(!disk);
                assert!(!persist);
            }
            _ => panic!("expected start command"),
        }
    }

    #[test]
    fn run_alias_maps_to_start_command() {
        let cli = Cli::parse_from(["helix", "run", "qa"]);

        match cli.command {
            Some(Commands::Start { instance, .. }) => {
                assert_eq!(instance.as_deref(), Some("qa"));
            }
            _ => panic!("expected run alias to map to start command"),
        }
    }

    #[test]
    fn start_foreground_flag_enables_attached_mode() {
        let cli = Cli::parse_from(["helix", "start", "qa", "--foreground"]);

        match cli.command {
            Some(Commands::Start { foreground, .. }) => assert!(foreground),
            _ => panic!("expected start command"),
        }
    }

    #[test]
    fn start_disk_flag_enables_on_disk_mode() {
        let cli = Cli::parse_from(["helix", "start", "qa", "--disk"]);

        match cli.command {
            Some(Commands::Start { disk, .. }) => assert!(disk),
            _ => panic!("expected start command"),
        }
    }

    #[test]
    fn start_detach_flag_remains_background_alias() {
        let cli = Cli::parse_from(["helix", "start", "qa", "--detach"]);

        match cli.command {
            Some(Commands::Start {
                foreground, detach, ..
            }) => {
                assert!(!foreground);
                assert!(detach);
            }
            _ => panic!("expected start command"),
        }
    }

    #[test]
    fn start_foreground_conflicts_with_detach_alias() {
        assert!(Cli::try_parse_from(["helix", "start", "qa", "--foreground", "--detach"]).is_err());
    }

    #[test]
    fn init_local_disk_flag_parses() {
        let cli = Cli::parse_from(["helix", "init", "local", "--disk"]);

        match cli.command {
            Some(Commands::Init {
                target:
                    Some(InitTarget::Local {
                        name, port, disk, ..
                    }),
                ..
            }) => {
                assert_eq!(name, "dev");
                assert_eq!(port, helix_cli::config::DEFAULT_LOCAL_PORT);
                assert!(disk);
            }
            _ => panic!("expected init local command"),
        }
    }

    #[test]
    fn init_cloud_with_cluster_id_parses() {
        let cli = Cli::parse_from(["helix", "init", "cloud", "--cluster-id", "abc"]);

        match cli.command {
            Some(Commands::Init {
                target:
                    Some(InitTarget::Enterprise {
                        name, cluster_id, ..
                    }),
                ..
            }) => {
                assert_eq!(name, "production");
                assert_eq!(cluster_id.as_deref(), Some("abc"));
            }
            _ => panic!("expected init cloud command"),
        }
    }

    #[test]
    fn init_path_parses_before_subcommand() {
        let cli = Cli::parse_from(["helix", "init", "--path", "/tmp/proj", "local"]);

        match cli.command {
            Some(Commands::Init {
                path,
                target: Some(InitTarget::Local { .. }),
                ..
            }) => assert_eq!(path.as_deref(), Some("/tmp/proj")),
            _ => panic!("expected init local command with path"),
        }
    }

    #[test]
    fn init_path_parses_after_subcommand() {
        let cli = Cli::parse_from(["helix", "init", "local", "--path", "/tmp/proj"]);

        match cli.command {
            Some(Commands::Init {
                path,
                target: Some(InitTarget::Local { .. }),
                ..
            }) => assert_eq!(path.as_deref(), Some("/tmp/proj")),
            _ => panic!("expected init local command with path"),
        }
    }

    #[test]
    fn add_path_parses_after_subcommand() {
        let cli = Cli::parse_from([
            "helix",
            "add",
            "local",
            "--name",
            "qa",
            "--path",
            "/tmp/proj",
        ]);

        match cli.command {
            Some(Commands::Add {
                path,
                target: Some(AddTarget::Local { name, .. }),
            }) => {
                assert_eq!(path.as_deref(), Some("/tmp/proj"));
                assert_eq!(name, "qa");
            }
            _ => panic!("expected add local command with path"),
        }
    }

    #[test]
    fn init_no_skills_parses_before_subcommand() {
        let cli = Cli::parse_from(["helix", "init", "--no-skills", "local"]);

        match cli.command {
            Some(Commands::Init {
                no_skills,
                target: Some(target),
                ..
            }) => {
                assert!(no_skills);
                assert!(matches!(target, InitTarget::Local { .. }));
            }
            _ => panic!("expected init local command"),
        }
    }

    #[test]
    fn init_no_skills_parses_after_subcommand() {
        // Agents naturally type `helix init local --no-skills`; the flag lives on
        // the subcommand too, so this must parse and resolve to "skip skills".
        let cli = Cli::parse_from(["helix", "init", "local", "--no-skills"]);

        match cli.command {
            Some(Commands::Init {
                target: Some(target),
                ..
            }) => {
                assert!(matches!(target, InitTarget::Local { .. }));
                assert_eq!(target.skills_override(), Some(false));
            }
            _ => panic!("expected init local command"),
        }
    }

    #[test]
    fn init_skills_parses_after_subcommand() {
        let cli = Cli::parse_from(["helix", "init", "local", "--skills"]);

        match cli.command {
            Some(Commands::Init {
                target: Some(target),
                ..
            }) => assert_eq!(target.skills_override(), Some(true)),
            _ => panic!("expected init local command"),
        }
    }

    #[test]
    fn init_skills_and_no_skills_conflict_after_subcommand() {
        assert!(
            Cli::try_parse_from(["helix", "init", "local", "--skills", "--no-skills"]).is_err()
        );
    }

    #[test]
    fn init_cloud_without_cluster_id_parses() {
        let cli = Cli::parse_from(["helix", "init", "cloud"]);

        match cli.command {
            Some(Commands::Init {
                target: Some(InitTarget::Enterprise { cluster_id, .. }),
                ..
            }) => assert!(cluster_id.is_none()),
            _ => panic!("expected init cloud command"),
        }
    }

    #[test]
    fn add_cloud_with_cluster_id_parses() {
        let cli = Cli::parse_from([
            "helix",
            "add",
            "cloud",
            "--name",
            "production",
            "--cluster-id",
            "abc",
        ]);

        match cli.command {
            Some(Commands::Add {
                target:
                    Some(AddTarget::Enterprise {
                        name, cluster_id, ..
                    }),
                ..
            }) => {
                assert_eq!(name, "production");
                assert_eq!(cluster_id.as_deref(), Some("abc"));
            }
            _ => panic!("expected add cloud command"),
        }
    }

    #[test]
    fn add_cloud_without_cluster_id_parses() {
        let cli = Cli::parse_from(["helix", "add", "cloud", "--name", "production"]);

        match cli.command {
            Some(Commands::Add {
                target: Some(AddTarget::Enterprise { cluster_id, .. }),
                ..
            }) => assert!(cluster_id.is_none()),
            _ => panic!("expected add cloud command"),
        }
    }

    #[test]
    fn init_skills_flag_parses() {
        let cli = Cli::parse_from(["helix", "init", "--skills", "local"]);

        match cli.command {
            Some(Commands::Init {
                skills, no_skills, ..
            }) => {
                assert!(skills);
                assert!(!no_skills);
            }
            _ => panic!("expected init command"),
        }
    }

    #[test]
    fn init_no_skills_flag_parses() {
        let cli = Cli::parse_from(["helix", "init", "--no-skills", "local"]);

        match cli.command {
            Some(Commands::Init {
                skills, no_skills, ..
            }) => {
                assert!(!skills);
                assert!(no_skills);
            }
            _ => panic!("expected init command"),
        }
    }

    #[test]
    fn init_defaults_to_no_skills_flags() {
        let cli = Cli::parse_from(["helix", "init", "local"]);

        match cli.command {
            Some(Commands::Init {
                skills, no_skills, ..
            }) => {
                assert!(!skills);
                assert!(!no_skills);
            }
            _ => panic!("expected init command"),
        }
    }

    #[test]
    fn init_skills_and_no_skills_conflict() {
        assert!(
            Cli::try_parse_from(["helix", "init", "--skills", "--no-skills", "local"]).is_err()
        );
    }

    #[test]
    fn chef_command_parses() {
        let cli = Cli::parse_from(["helix", "chef"]);

        match cli.command {
            Some(Commands::Chef {}) => {}
            _ => panic!("expected chef command"),
        }
    }

    #[test]
    fn cook_alias_parses() {
        let cli = Cli::parse_from(["helix", "cook"]);

        match cli.command {
            Some(Commands::Chef {}) => {}
            _ => panic!("expected chef command alias"),
        }
    }

    #[test]
    fn add_local_disk_flag_parses() {
        let cli = Cli::parse_from(["helix", "add", "local", "--name", "qa", "--disk"]);

        match cli.command {
            Some(Commands::Add {
                target: Some(AddTarget::Local { name, port, disk }),
                ..
            }) => {
                assert_eq!(name, "qa");
                assert_eq!(port, helix_cli::config::DEFAULT_LOCAL_PORT);
                assert!(disk);
            }
            _ => panic!("expected add local command"),
        }
    }

    #[test]
    fn update_v1_flag_parses() {
        let cli = Cli::parse_from(["helix", "update", "--v1"]);

        match cli.command {
            Some(Commands::Update { force, v1 }) => {
                assert!(!force);
                assert!(v1);
            }
            _ => panic!("expected update command"),
        }
    }

    #[test]
    fn add_allows_interactive_entrypoint() {
        let cli = Cli::parse_from(["helix", "add"]);

        match cli.command {
            Some(Commands::Add { target, .. }) => assert!(target.is_none()),
            _ => panic!("expected add command"),
        }
    }

    #[test]
    fn add_path_flag_parses() {
        let cli = Cli::parse_from([
            "helix",
            "add",
            "--path",
            "/tmp/proj",
            "local",
            "--name",
            "qa",
        ]);

        match cli.command {
            Some(Commands::Add {
                path,
                target: Some(AddTarget::Local { name, .. }),
            }) => {
                assert_eq!(path.as_deref(), Some("/tmp/proj"));
                assert_eq!(name, "qa");
            }
            _ => panic!("expected add local command with path"),
        }
    }

    #[test]
    fn root_workspace_command_parses() {
        let cli = Cli::parse_from(["helix", "workspace", "list"]);

        match cli.command {
            Some(Commands::Workspace {
                action: Some(WorkspaceConfigAction::List { .. }),
            }) => {}
            _ => panic!("expected workspace list command"),
        }
    }

    #[test]
    fn root_project_command_parses() {
        let cli = Cli::parse_from(["helix", "project", "show"]);

        match cli.command {
            Some(Commands::Project {
                action: Some(ProjectConfigAction::Show { .. }),
            }) => {}
            _ => panic!("expected project show command"),
        }
    }

    #[test]
    fn root_cluster_command_parses() {
        let cli = Cli::parse_from(["helix", "cluster", "list"]);

        match cli.command {
            Some(Commands::Cluster {
                action: Some(ClusterConfigAction::List { .. }),
            }) => {}
            _ => panic!("expected cluster list command"),
        }
    }

    #[test]
    fn root_cluster_indexes_command_parses() {
        let cli = Cli::parse_from(["helix", "cluster", "indexes", "--cluster-id", "ent_123"]);

        match cli.command {
            Some(Commands::Cluster {
                action:
                    Some(ClusterConfigAction::Indexes {
                        cluster_id,
                        format: _,
                    }),
            }) => assert_eq!(cluster_id.as_deref(), Some("ent_123")),
            _ => panic!("expected cluster indexes command"),
        }
    }

    #[test]
    fn status_accepts_optional_instance() {
        let cli = Cli::parse_from(["helix", "status", "qa"]);

        match cli.command {
            Some(Commands::Status { instance }) => assert_eq!(instance.as_deref(), Some("qa")),
            _ => panic!("expected status command"),
        }
    }

    #[test]
    fn query_accepts_file_input() {
        let cli = Cli::parse_from(["helix", "query", "dev", "--file", "request.json"]);

        match cli.command {
            Some(Commands::Query { file, json, .. }) => {
                assert_eq!(file.as_deref(), Some("request.json"));
                assert!(json.is_none());
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn query_accepts_inline_json_input() {
        let inline_json = r#"{"request_type":"read","query":{"queries":[]}}"#;
        let cli = Cli::parse_from(["helix", "query", "dev", "--json", inline_json]);

        match cli.command {
            Some(Commands::Query { file, json, .. }) => {
                assert!(file.is_none());
                assert_eq!(json.as_deref(), Some(inline_json));
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn query_accepts_dry_run() {
        let cli = Cli::parse_from([
            "helix",
            "query",
            "dev",
            "--json",
            r#"{"request_type":"read","query":{"queries":[]}}"#,
            "--dry-run",
        ]);

        match cli.command {
            Some(Commands::Query {
                dry_run, compact, ..
            }) => {
                assert!(dry_run);
                assert!(!compact);
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn query_rejects_missing_input() {
        assert!(Cli::try_parse_from(["helix", "query", "dev"]).is_err());
    }

    #[test]
    fn query_rejects_file_and_inline_json_together() {
        assert!(
            Cli::try_parse_from([
                "helix",
                "query",
                "dev",
                "--file",
                "request.json",
                "--json",
                "{}",
            ])
            .is_err()
        );
    }

    #[test]
    fn push_accepts_optional_enterprise_instance() {
        let cli = Cli::parse_from(["helix", "push", "production"]);

        match cli.command {
            Some(Commands::Push { instance, dev }) => {
                assert_eq!(instance.as_deref(), Some("production"));
                assert!(!dev);
            }
            _ => panic!("expected push command"),
        }
    }

    #[test]
    fn sync_accepts_yes_for_noninteractive_reconciliation() {
        let cli = Cli::parse_from(["helix", "sync", "production", "--yes"]);

        match cli.command {
            Some(Commands::Sync {
                instance,
                yes,
                dry_run,
            }) => {
                assert_eq!(instance.as_deref(), Some("production"));
                assert!(yes);
                assert!(!dry_run);
            }
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn sync_accepts_dry_run() {
        let cli = Cli::parse_from(["helix", "sync", "production", "--dry-run"]);

        match cli.command {
            Some(Commands::Sync { dry_run, yes, .. }) => {
                assert!(dry_run);
                assert!(!yes);
            }
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn sync_rejects_dry_run_with_yes() {
        assert!(
            Cli::try_parse_from(["helix", "sync", "production", "--dry-run", "--yes"]).is_err()
        );
    }

    #[test]
    fn start_persist_flag_saves_settings() {
        let cli = Cli::parse_from(["helix", "start", "qa", "--persist"]);

        match cli.command {
            Some(Commands::Start { persist, .. }) => assert!(persist),
            _ => panic!("expected start command"),
        }
    }

    #[test]
    fn query_accepts_ts_expression() {
        let cli = Cli::parse_from(["helix", "query", "dev", "-e", "readBatch()"]);

        match cli.command {
            Some(Commands::Query { ts, file, json, .. }) => {
                assert_eq!(ts.as_deref(), Some("readBatch()"));
                assert!(file.is_none());
                assert!(json.is_none());
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn query_accepts_ts_file() {
        let cli = Cli::parse_from(["helix", "query", "dev", "--ts-file", "query.ts"]);

        match cli.command {
            Some(Commands::Query { ts_file, .. }) => {
                assert_eq!(ts_file.as_deref(), Some("query.ts"));
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn query_rejects_json_and_ts_together() {
        assert!(
            Cli::try_parse_from(["helix", "query", "dev", "--json", "{}", "-e", "readBatch()"])
                .is_err()
        );
    }

    #[test]
    fn removed_compile_command_parses_to_hidden_variant() {
        let cli = Cli::parse_from(["helix", "compile"]);
        assert!(matches!(cli.command, Some(Commands::Compile { .. })));
    }

    #[test]
    fn removed_check_command_parses_to_hidden_variant() {
        let cli = Cli::parse_from(["helix", "check"]);
        assert!(matches!(cli.command, Some(Commands::Check { .. })));
    }

    #[test]
    fn removed_deploy_command_parses_to_hidden_variant() {
        let cli = Cli::parse_from(["helix", "deploy"]);
        assert!(matches!(cli.command, Some(Commands::Deploy { .. })));
    }

    #[test]
    fn removed_commands_tolerate_trailing_args() {
        // Agents guess `helix compile <path>` / extra flags; these must still
        // route to the friendly-error handler instead of failing to parse.
        assert!(matches!(
            Cli::parse_from(["helix", "compile", "queries/", "--path", "x"]).command,
            Some(Commands::Compile { .. })
        ));
        assert!(matches!(
            Cli::parse_from(["helix", "check", "src/main.hx"]).command,
            Some(Commands::Check { .. })
        ));
    }

    #[test]
    fn top_level_help_requests_are_recognized() {
        assert!(is_top_level_help_request(["help"]));
        assert!(is_top_level_help_request(["--help"]));
        assert!(is_top_level_help_request(["-h"]));
    }

    #[test]
    fn help_following_a_command_is_left_to_clap() {
        // `helix help <command>` and `helix <command> --help` must NOT be claimed
        // by our top-level renderer — clap shows that command's detailed help.
        assert!(!is_top_level_help_request(["help", "query"]));
        assert!(!is_top_level_help_request(["query", "--help"]));
        assert!(!is_top_level_help_request(["start", "-h"]));
    }

    #[test]
    fn non_help_invocations_are_not_claimed() {
        assert!(!is_top_level_help_request(Vec::<String>::new()));
        assert!(!is_top_level_help_request(["status"]));
        assert!(!is_top_level_help_request(["--version"]));
    }

    #[test]
    fn subcommand_help_flag_is_left_to_clap() {
        // With clap defaults intact, `helix query --help` still triggers clap's
        // per-command help (which surfaces as a DisplayHelp "error" from the
        // parser before exit).
        let err = Cli::try_parse_from(["helix", "query", "--help"])
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
    }

    #[test]
    fn skills_update_defaults_to_global() {
        let cli = Cli::parse_from(["helix", "skills", "update"]);
        match cli.command {
            Some(Commands::Skills {
                action: SkillsAction::Update { project },
            }) => assert!(!project),
            _ => panic!("expected skills update command"),
        }
    }

    #[test]
    fn skills_list_project_flag_parses() {
        let cli = Cli::parse_from(["helix", "skills", "list", "--project"]);
        match cli.command {
            Some(Commands::Skills {
                action: SkillsAction::List { project },
            }) => assert!(project),
            _ => panic!("expected skills list command"),
        }
    }

    #[test]
    fn skills_install_parses() {
        let cli = Cli::parse_from(["helix", "skills", "install"]);
        assert!(matches!(
            cli.command,
            Some(Commands::Skills {
                action: SkillsAction::Install { project: false },
            })
        ));
    }

    #[test]
    fn query_help_is_informative() {
        use clap::CommandFactory;
        let mut cmd = Cli::command();
        let query = cmd
            .get_subcommands_mut()
            .find(|c| c.get_name() == "query")
            .expect("query subcommand should exist");
        // `query` renders the compact (short) help layout for both -h and --help.
        let help = query.render_help().to_string();
        assert!(help.contains("Examples:"), "examples block missing");
        // Options are grouped under scannable headings.
        assert!(help.contains("Input (pick one):"), "input heading missing");
        assert!(help.contains("Connection:"), "connection heading missing");
        assert!(help.contains("Output:"), "output heading missing");
    }
}
