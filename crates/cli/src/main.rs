use clap::builder::styling::{AnsiColor, Color, RgbColor, Style, Styles};
use clap::{ArgGroup, Parser, Subcommand};
use console::style;
use eyre::Result;
use helix_cli::{
    commands, errors, metrics_sender, output, update, AddTarget, AuthAction, CloudApiAction,
    ClusterAction, DatabaseAction, InitTarget, MetricsAction, ProjectAction, S3StorageArgs,
    ServiceCredentialAction, SkillsAction, WorkspaceAction,
};
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
    /// Print machine-readable JSON on stdout; never prompt
    #[arg(long, global = true, conflicts_with_all = ["quiet", "verbose"])]
    json: bool,

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
    /// Initialize a Helix project
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

    /// Add a local or Helix Cloud instance
    Add {
        /// Project directory (defaults to current directory)
        #[arg(short, long, global = true)]
        path: Option<String>,
        #[command(subcommand)]
        target: Option<AddTarget>,
    },

    /// Start a local instance in the background
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
        /// Use on-disk storage backed by a local SeaweedFS container for this run
        #[arg(long, conflicts_with = "storage_uri")]
        disk: bool,
        #[command(flatten)]
        s3: S3StorageArgs,
        #[command(flatten)]
        image: helix_cli::image::ImageArgs,
        /// Persist the resolved port, storage, and image settings back to helix.toml
        #[arg(long)]
        persist: bool,
    },

    /// Stop a background local instance
    Stop {
        /// Instance name to stop
        instance: Option<String>,
    },

    /// Restart a background local instance
    Restart {
        /// Instance name to restart
        instance: Option<String>,
    },

    /// Show local and Helix Cloud instance status
    Status {
        /// Instance name to show, defaults to all instances
        instance: Option<String>,
    },

    /// View logs for a local or Helix Cloud instance
    Logs {
        /// Instance name
        instance: Option<String>,
        /// Stream new log lines (local instances)
        #[arg(long, short = 'f')]
        follow: bool,
        /// Start of the Cloud query-error window (RFC 3339); defaults to an hour before --end
        #[arg(long)]
        start: Option<String>,
        /// End of the Cloud query-error window (RFC 3339); defaults to now
        #[arg(long)]
        end: Option<String>,
    },

    /// Send a query to a running Helix instance
    #[command(group(
        ArgGroup::new("query_input")
            .required(true)
            .args(["file", "body", "ts", "ts_file"])
    ))]
    // Use the compact (short) help layout for both `-h` and `--help`. clap's
    // long-help layout hardcodes a blank line between every option, which is
    // too sparse here, so we render the short layout and supply examples via
    // `after_help` instead of `after_long_help`.
    #[command(disable_help_flag = true)]
    #[command(after_help = r#"Examples:
  helix query --file examples/request.json
  helix query -e 'readBatch().varAs("c", g().nWithLabel("User").count()).returning(["c"])'

Docs: https://docs.helix-db.com/cli/command-reference/query"#)]
    Query {
        /// Print help
        #[arg(short = 'h', long = "help", action = clap::ArgAction::HelpShort)]
        help: Option<bool>,
        /// Instance or typed database; defaults to dev or the sole linked target
        instance: Option<String>,
        /// Query from a JSON request file
        #[arg(
            short,
            long,
            value_name = "REQUEST.json",
            help_heading = "Input (pick one)"
        )]
        file: Option<String>,
        /// Query from an inline JSON request body
        #[arg(long, value_name = "JSON", help_heading = "Input (pick one)")]
        body: Option<String>,
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
        #[arg(long, help_heading = "Connection")]
        warm: bool,
    },

    /// Execute or explain Cypher against a local Helix instance
    Cypher(commands::cypher::Args),

    /// Open an interactive v3 JSON query shell
    Shell {
        /// Instance or typed database; defaults to dev or the sole linked target
        instance: Option<String>,
    },

    /// Log in to Helix Cloud and inspect the session
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },

    /// List and inspect Helix Cloud workspaces
    Workspace {
        #[command(subcommand)]
        action: Option<WorkspaceAction>,
    },

    /// Manage Helix Cloud projects and this directory's link
    Project {
        #[command(subcommand)]
        action: Option<ProjectAction>,
    },

    /// List and inspect Helix Cloud clusters
    Cluster {
        #[command(subcommand)]
        action: Option<ClusterAction>,
    },

    /// Manage Helix Cloud databases and application keys
    Database {
        #[command(subcommand)]
        action: Option<DatabaseAction>,
    },

    /// Manage workspace-owned credentials for headless API and MCP automation
    ServiceCredential {
        #[command(subcommand)]
        action: Option<ServiceCredentialAction>,
    },

    /// Call a Helix Cloud API path with the active session
    Api {
        #[command(subcommand)]
        action: CloudApiAction,
    },

    /// Prune local containers and workspaces
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

    // --- Removed commands ----------------------------------------------------
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
    /// (removed) Cloud deployment is managed by the control plane
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
    errors::CliError::new(format!("`helix {command}` is not a command"))
        .with_hint(
            "Helix validates queries server-side, so there is no compile/check step. \
             Send a query to a running instance with \
             `helix query <instance> --file <request.json>`.",
        )
        .into()
}

/// Build the friendly error shown for the removed deployment command.
fn removed_deploy_command_error() -> eyre::Report {
    errors::CliError::new("`helix deploy` is not a command")
        .with_hint("Cloud database lifecycle is managed through the Helix control plane.")
        .into()
}

fn display_welcome(update_available: Option<String>, skills_update_available: bool) {
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
    println!(
        "  {} {}\n",
        style("Helix DB CLI").bold(),
        style(format!("v{version}")).dim()
    );

    if let Some(latest_version) = update_available {
        println!("  Update available: v{version} -> v{latest_version}");
        println!("  Run 'helix update' to upgrade\n");
    }

    if skills_update_available {
        println!("  Helix skills update available");
        println!("  Run 'helix skills update' to refresh\n");
    }

    print_section("Getting Started");
    print_command("helix chef", "Bootstrap a Helix app with an AI agent", 38);
    print_command("helix init", "Create a new project", 38);
    print_command("helix add", "Add a local or Helix Cloud instance", 38);

    print_section("Local Development");
    print_command(
        "helix start <instance>",
        "Start a local instance in the background",
        38,
    );
    print_command("helix status", "Show local and Cloud instance status", 38);
    print_command(
        "helix logs <instance> -f",
        "Follow logs for an instance",
        38,
    );
    print_command(
        "helix query <instance> --file request.json",
        "Send a query",
        38,
    );

    print_section("Helix Cloud");
    print_command("helix auth login", "Log in to Helix Cloud", 38);
    print_command("helix database list", "List your Cloud databases", 38);

    println!();
    println!("Docs: https://docs.helix-db.com");
    println!("Rust DSL: https://docs.rs/helix-enterprise-ql")
}

fn print_section(title: &str) {
    println!("\n{}\n", style(title).bold());
}

fn print_command(cmd: &str, desc: &str, width: usize) {
    println!(
        "  {} {}",
        style(format!("{cmd:<width$}")).color256(208).bold(),
        style(desc).dim()
    );
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
    const W: usize = 20;
    println!(
        "{} {}\n",
        style("Helix DB CLI").bold(),
        style(format!("v{}", update::current_version())).dim()
    );
    println!("Usage: helix [OPTIONS] <COMMAND>");

    print_section("Getting started");
    print_command(
        "chef",
        "Bootstrap a Helix app with a coding agent (alias: cook)",
        W,
    );
    print_command(
        "init",
        "Scaffold a new project (init local | init cloud)",
        W,
    );
    print_command(
        "add",
        "Add a local or Cloud instance to an existing project",
        W,
    );

    print_section("Local development");
    print_command(
        "start",
        "Start a local instance in the background (alias: run)",
        W,
    );
    print_command("stop", "Stop a background local instance", W);
    print_command("restart", "Restart a background local instance", W);
    print_command("status", "Show local and Cloud instance status", W);
    print_command("logs", "View or follow instance logs", W);
    print_command("query", "Send a query to a local or Cloud instance", W);
    print_command("shell", "Open an interactive JSON query shell", W);
    print_command("prune", "Remove Helix-owned local containers and state", W);
    print_command("delete", "Delete an instance from helix.toml", W);

    print_section("Helix Cloud");
    print_command("auth", "Log in, inspect, or end your session", W);
    print_command("workspace", "List and inspect workspaces", W);
    print_command("project", "Manage projects and this directory's link", W);
    print_command("cluster", "List and inspect clusters", W);
    print_command("database", "Manage databases and application keys", W);
    print_command(
        "service-credential",
        "Manage headless automation credentials",
        W,
    );
    print_command("api", "Call the Helix Cloud API directly", W);

    print_section("CLI");
    print_command("skills", "Install, update, and list Helix agent skills", W);
    print_command("metrics", "Manage telemetry collection", W);
    print_command("update", "Update the CLI to the latest version", W);
    print_command("feedback", "Send feedback to the Helix team", W);
    print_command("help", "Show this help", W);

    print_section("Options");
    print_command(
        "--json",
        "Machine-readable JSON on stdout; never prompts",
        W,
    );
    print_command("--quiet", "Errors and final result only", W);
    print_command(
        "-v, --verbose",
        "Detailed output with timing information",
        W,
    );
    print_command("-h, --help", "Show this help", W);
    print_command("-V, --version", "Show the CLI version", W);

    println!();
    println!("Run 'helix <command> --help' for details on a specific command.");
    println!("Docs: https://docs.helix-db.com");
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    cliclack::set_theme(output::HelixTheme);

    // Render our grouped overview for a bare top-level help request before doing
    // any setup — keeps `helix help` / `helix --help` instant and offline. clap
    // still owns per-command help (`helix query --help`) and the welcome banner
    // on a no-arg invocation.
    if wants_top_level_help() {
        print_help();
        return Ok(());
    }

    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => exit_with_parse_error(error),
    };
    output::OutputMode::from_flags(cli.json, cli.quiet, cli.verbose).set();

    let metrics_sender = metrics_sender::MetricsSender::new();
    metrics_sender.send_cli_install_event_if_first_time();
    // Update notices are chrome; JSON consumers never see them, so skip the
    // network round-trips entirely.
    let (update_available, skills_update_available) = if cli.json {
        (None, false)
    } else {
        (
            update::check_for_updates().await?,
            update::check_skills_update().await,
        )
    };

    let result = match cli.command {
        None if cli.json => output::emit(
            &serde_json::json!({ "version": update::current_version() }),
            |_| Ok(()),
        ),
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
            s3,
            image,
            persist,
        }) => commands::start::run(instance, foreground, port, disk, s3, image, persist).await,
        Some(Commands::Stop { instance }) => commands::stop::run(instance).await,
        Some(Commands::Restart { instance }) => commands::restart::run(instance).await,
        Some(Commands::Status { instance }) => commands::status::run(instance).await,
        Some(Commands::Logs {
            instance,
            follow,
            start,
            end,
        }) => commands::logs::run(instance, follow, start, end).await,
        Some(Commands::Query {
            instance,
            file,
            body,
            ts,
            ts_file,
            warm,
            host,
            port,
            ..
        }) => {
            commands::query::run(
                instance,
                file,
                body,
                ts,
                ts_file,
                commands::query::LocalOverrides { warm, host, port },
            )
            .await
        }
        Some(Commands::Cypher(args)) => commands::cypher::run(args).await,
        Some(Commands::Shell { instance }) => commands::shell::run(instance).await,
        Some(Commands::Auth { action }) => commands::auth::run(action).await,
        Some(Commands::Workspace { action }) => commands::cloud::workspace::run(action).await,
        Some(Commands::Project { action }) => commands::cloud::project::run(action).await,
        Some(Commands::Cluster { action }) => commands::cloud::cluster::run(action).await,
        Some(Commands::Database { action }) => commands::cloud::database::run(action).await,
        Some(Commands::ServiceCredential { action }) => {
            commands::cloud::service_credential::run(action).await
        }
        Some(Commands::Api { action }) => commands::cloud::api::run(action).await,
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

    let _ = metrics_sender.shutdown().await;

    let Err(error) = result else {
        return Ok(());
    };
    output::report_error(&error);
    std::process::exit(1);
}

/// Report a clap parse failure. Help and version requests print normally;
/// real usage errors become `{"error": ...}` on stderr when `--json` was
/// requested, so agents never have to parse clap's human text.
fn exit_with_parse_error(error: clap::Error) -> ! {
    use clap::error::ErrorKind;
    let json_requested = std::env::args().any(|arg| arg == "--json");
    if !json_requested
        || matches!(
            error.kind(),
            ErrorKind::DisplayHelp
                | ErrorKind::DisplayVersion
                | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        )
    {
        error.exit();
    }
    output::OutputMode::Json.set();
    output::report_error(&eyre::Report::new(parse_error_to_cli_error(&error)));
    std::process::exit(2);
}

/// clap's rendered text up to the usage line is the message (its first line
/// minus the `error: ` prefix, plus any indented detail lines); the usage text
/// is replaced by a pointer to `--help`.
fn parse_error_to_cli_error(error: &clap::Error) -> errors::CliError {
    let rendered = error.render().to_string();
    let message = rendered
        .lines()
        .take_while(|line| !line.starts_with("Usage:"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let message = message.trim_start_matches("error: ");
    let message = if message.is_empty() {
        error.kind().to_string()
    } else {
        message.to_owned()
    };
    errors::CliError::new(message).with_hint("run the command with --help for usage")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn json_flag_is_global_and_conflicts_with_verbosity_flags() {
        let cli = Cli::parse_from(["helix", "status", "--json"]);
        assert!(cli.json);
        assert!(Cli::try_parse_from(["helix", "--json", "--quiet", "status"]).is_err());
        assert!(Cli::try_parse_from(["helix", "--json", "-v", "status"]).is_err());
    }

    #[test]
    fn parse_errors_become_cli_errors_without_the_clap_prefix() {
        let Err(error) = Cli::try_parse_from(["helix", "query", "dev"]) else {
            panic!("query without input must fail to parse");
        };
        let converted = parse_error_to_cli_error(&error);
        assert!(
            !converted.message.starts_with("error:"),
            "{}",
            converted.message
        );
        assert!(
            converted.message.contains("required"),
            "{}",
            converted.message
        );
        assert!(converted.hint.is_some());
    }

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
                s3,
                image,
                persist,
            }) => {
                assert_eq!(instance.as_deref(), Some("qa"));
                assert!(!foreground);
                assert!(!detach);
                assert_eq!(port, None);
                assert!(!disk);
                assert!(!s3.has_any());
                assert!(image.image_version.is_none());
                assert!(image.pull.is_none());
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
    fn start_s3_flags_parse() {
        let cli = Cli::parse_from([
            "helix",
            "start",
            "qa",
            "--storage-uri",
            "s3://bucket/prefix",
            "--s3-region",
            "eu-west-2",
            "--s3-endpoint-url",
            "https://s3.example.com",
            "--s3-allow-http",
        ]);

        match cli.command {
            Some(Commands::Start { s3, .. }) => {
                assert_eq!(s3.storage_uri.as_deref(), Some("s3://bucket/prefix"));
                assert_eq!(s3.s3_region.as_deref(), Some("eu-west-2"));
                assert_eq!(
                    s3.s3_endpoint_url.as_deref(),
                    Some("https://s3.example.com")
                );
                assert!(s3.s3_allow_http);
            }
            _ => panic!("expected start command"),
        }
    }

    #[test]
    fn start_rejects_disk_with_storage_uri() {
        assert!(
            Cli::try_parse_from(["helix", "start", "qa", "--disk", "--storage-uri", "s3://b"])
                .is_err()
        );
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
    fn init_local_s3_flags_parse() {
        let cli = Cli::parse_from([
            "helix",
            "init",
            "local",
            "--storage-uri",
            "s3://bucket/prefix",
            "--s3-region",
            "eu-west-2",
        ]);

        match cli.command {
            Some(Commands::Init {
                target: Some(InitTarget::Local { s3, .. }),
                ..
            }) => {
                assert_eq!(s3.storage_uri.as_deref(), Some("s3://bucket/prefix"));
                assert_eq!(s3.s3_region.as_deref(), Some("eu-west-2"));
            }
            _ => panic!("expected init local command"),
        }
    }

    #[test]
    fn init_cloud_with_database_parses() {
        let cli = Cli::parse_from(["helix", "init", "cloud", "--database", "cluster:abc"]);

        match cli.command {
            Some(Commands::Init {
                target: Some(InitTarget::Enterprise { name, database, .. }),
                ..
            }) => {
                assert_eq!(name, "production");
                assert_eq!(database.as_deref(), Some("cluster:abc"));
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
    fn init_cloud_without_database_parses() {
        let cli = Cli::parse_from(["helix", "init", "cloud"]);

        match cli.command {
            Some(Commands::Init {
                target: Some(InitTarget::Enterprise { database, .. }),
                ..
            }) => assert!(database.is_none()),
            _ => panic!("expected init cloud command"),
        }
    }

    #[test]
    fn add_cloud_with_database_parses() {
        let cli = Cli::parse_from([
            "helix",
            "add",
            "cloud",
            "--name",
            "production",
            "--database",
            "tenant:abc",
        ]);

        match cli.command {
            Some(Commands::Add {
                target: Some(AddTarget::Enterprise { name, database, .. }),
                ..
            }) => {
                assert_eq!(name, "production");
                assert_eq!(database.as_deref(), Some("tenant:abc"));
            }
            _ => panic!("expected add cloud command"),
        }
    }

    #[test]
    fn add_cloud_without_database_parses() {
        let cli = Cli::parse_from(["helix", "add", "cloud", "--name", "production"]);

        match cli.command {
            Some(Commands::Add {
                target: Some(AddTarget::Enterprise { database, .. }),
                ..
            }) => assert!(database.is_none()),
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
                target:
                    Some(AddTarget::Local {
                        name, port, disk, ..
                    }),
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
    fn add_local_s3_flags_parse() {
        let cli = Cli::parse_from([
            "helix",
            "add",
            "local",
            "--name",
            "qa",
            "--storage-uri",
            "s3://bucket/prefix",
        ]);

        match cli.command {
            Some(Commands::Add {
                target: Some(AddTarget::Local { name, s3, .. }),
                ..
            }) => {
                assert_eq!(name, "qa");
                assert_eq!(s3.storage_uri.as_deref(), Some("s3://bucket/prefix"));
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
                action: Some(WorkspaceAction::List),
            }) => {}
            _ => panic!("expected workspace list command"),
        }
    }

    #[test]
    fn root_project_command_parses() {
        let cli = Cli::parse_from(["helix", "project", "get"]);

        match cli.command {
            Some(Commands::Project {
                action: Some(ProjectAction::Get { .. }),
            }) => {}
            _ => panic!("expected project get command"),
        }
    }

    #[test]
    fn root_cluster_command_parses() {
        let cli = Cli::parse_from(["helix", "cluster", "list"]);

        match cli.command {
            Some(Commands::Cluster {
                action: Some(ClusterAction::List { .. }),
            }) => {}
            _ => panic!("expected cluster list command"),
        }
    }

    #[test]
    fn root_cluster_indexes_command_parses() {
        let cli = Cli::parse_from(["helix", "cluster", "indexes", "ent_123"]);

        match cli.command {
            Some(Commands::Cluster {
                action: Some(ClusterAction::Indexes { cluster, .. }),
            }) => assert_eq!(cluster.as_deref(), Some("ent_123")),
            _ => panic!("expected cluster indexes command"),
        }
    }

    #[test]
    fn cloud_groups_default_to_listing_without_a_subcommand() {
        for group in [
            "workspace",
            "project",
            "cluster",
            "database",
            "service-credential",
        ] {
            assert!(Cli::try_parse_from(["helix", group]).is_ok(), "{group}");
        }
    }

    #[test]
    fn cloud_resources_are_optional_and_accept_scope_flags() {
        let cli = Cli::parse_from([
            "helix",
            "database",
            "get",
            "--project",
            "api",
            "--workspace",
            "acme",
        ]);
        match cli.command {
            Some(Commands::Database {
                action: Some(DatabaseAction::Get { database, scope }),
            }) => {
                assert!(database.is_none());
                assert_eq!(scope.project.as_deref(), Some("api"));
                assert_eq!(scope.workspace.as_deref(), Some("acme"));
            }
            _ => panic!("expected database get command"),
        }
    }

    #[test]
    fn removed_cloud_flags_are_rejected() {
        for args in [
            vec!["helix", "config", "workspace", "list"],
            vec!["helix", "workspace", "list", "--format", "json"],
            vec!["helix", "project", "list", "--workspace-id", "w"],
            vec!["helix", "cluster", "indexes", "--cluster-id", "c"],
            vec!["helix", "query", "dev", "--file", "r.json", "--compact"],
        ] {
            assert!(Cli::try_parse_from(&args).is_err(), "{args:?}");
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
            Some(Commands::Query { file, body, .. }) => {
                assert_eq!(file.as_deref(), Some("request.json"));
                assert!(body.is_none());
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn query_accepts_inline_json_input() {
        let inline_json = r#"{"request_type":"read","query":{"queries":[]}}"#;
        let cli = Cli::parse_from(["helix", "query", "dev", "--body", inline_json]);

        match cli.command {
            Some(Commands::Query { file, body, .. }) => {
                assert!(file.is_none());
                assert_eq!(body.as_deref(), Some(inline_json));
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
        assert!(Cli::try_parse_from([
            "helix",
            "query",
            "dev",
            "--file",
            "request.json",
            "--body",
            "{}",
        ])
        .is_err());
    }

    #[test]
    fn push_is_removed() {
        assert!(Cli::try_parse_from(["helix", "push", "production"]).is_err());
    }

    #[test]
    fn sync_is_removed() {
        assert!(Cli::try_parse_from(["helix", "sync", "production"]).is_err());
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
            Some(Commands::Query { ts, file, body, .. }) => {
                assert_eq!(ts.as_deref(), Some("readBatch()"));
                assert!(file.is_none());
                assert!(body.is_none());
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
        assert!(Cli::try_parse_from([
            "helix",
            "query",
            "dev",
            "--body",
            "{}",
            "-e",
            "readBatch()"
        ])
        .is_err());
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
        assert!(
            !help.contains("--compact"),
            "--compact was replaced by --json"
        );
    }
}
