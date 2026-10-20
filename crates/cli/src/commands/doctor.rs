//! `helix doctor`: a read-only checkup of how one instance is set up.
//!
//! The CLI checks what it can see itself: its own version and, for a local
//! instance, the container runtime, the container, and the image it runs.
//! It then asks the server for its checks (`GET /v2/diagnostics`): storage
//! durability, the S3 bucket's region and round trip, the disk cache's
//! device and room, memory, index publication, and whether recent queries
//! used indexes. Nothing is changed, and nothing is created or started.
//!
//! The command exits non-zero when a check fails; warnings alone exit zero.
//! `--json` prints the whole report; human output lists every check, with
//! details and fixes for those that are not passing.

use crate::config::{DatabaseReference, InstanceInfo, LocalInstanceConfig};
use crate::errors::CliError;
use crate::local_runtime::LocalRuntime;
use crate::output::{self, Verbosity};
use crate::project::ProjectContext;
use crate::update::{self, LatestRelease};
use console::style;
use eyre::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Longest `GET /healthz` may take.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
/// Longest `GET /v2/diagnostics` may take; the server bounds its own
/// network probes to a few seconds.
const DIAGNOSTICS_TIMEOUT: Duration = Duration::from_secs(30);

/// A check's verdict, as the server reports it. An unknown status from a
/// newer server reads as [`Status::Skip`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Status {
    Pass,
    Warn,
    Fail,
    #[serde(other)]
    Skip,
}

/// One diagnosed aspect of the setup, from the CLI or the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Check {
    id: String,
    category: String,
    status: Status,
    summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fix: Option<String>,
}

/// A verdict the CLI reaches itself; only warnings and failures need a fix.
enum Verdict {
    Pass(Option<String>),
    Warn { detail: String, fix: String },
    Fail { detail: Option<String>, fix: String },
    Skip(String),
}

impl Check {
    /// A CLI check; its category is the ID's first segment.
    fn new(id: &str, summary: impl Into<String>, verdict: Verdict) -> Self {
        let (status, detail, fix) = match verdict {
            Verdict::Pass(detail) => (Status::Pass, detail, None),
            Verdict::Warn { detail, fix } => (Status::Warn, Some(detail), Some(fix)),
            Verdict::Fail { detail, fix } => (Status::Fail, detail, Some(fix)),
            Verdict::Skip(detail) => (Status::Skip, Some(detail), None),
        };
        Self {
            id: id.to_owned(),
            category: id.split('.').next().unwrap_or(id).to_owned(),
            status,
            summary: summary.into(),
            detail,
            fix,
        }
    }
}

/// What the server's `GET /v2/diagnostics` returns.
#[derive(Debug, Deserialize)]
struct ServerReport {
    uptime_secs: u64,
    checks: Vec<Check>,
}

/// Which instance was diagnosed.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Diagnosed {
    Local { name: String, url: String },
    Server { url: String },
    Cloud { name: String, database: String },
}

#[derive(Debug, Default, PartialEq, Eq, Serialize)]
struct Summary {
    pass: usize,
    warn: usize,
    fail: usize,
    skip: usize,
}

#[derive(Debug, Serialize)]
struct Report {
    target: Diagnosed,
    /// Seconds the server has been up, when it reported its own checks.
    #[serde(skip_serializing_if = "Option::is_none")]
    uptime_secs: Option<u64>,
    checks: Vec<Check>,
    summary: Summary,
}

/// Diagnose `instance` (or the default one), or the server at `url`.
pub async fn run(instance: Option<String>, url: Option<String>) -> Result<()> {
    // Resolve before any request, so a bad target fails fast.
    let resolved = resolve(instance, url)?;
    let latest = update::latest_release().await;
    let mut checks = vec![cli_version(update::current_version(), &latest)];
    let (target, uptime_secs) = match resolved {
        Resolved::Url(url) => {
            let uptime = server_checks(&url, None, &mut checks).await;
            (Diagnosed::Server { url }, uptime)
        }
        Resolved::Local {
            name,
            config,
            runtime,
        } => {
            // The running container's port, which `helix start --port` can
            // set without saving it, else the configured one.
            let running = local_checks(&name, &config, &runtime, &mut checks);
            let url = format!("http://localhost:{}", running.unwrap_or(config.port));
            let uptime = match running {
                Some(_) => server_checks(&url, Some(&name), &mut checks).await,
                None => {
                    checks.push(Check::new(
                        "server.reachable",
                        "Server checks need a running instance",
                        Verdict::Skip(format!("{name}'s container is not running.")),
                    ));
                    None
                }
            };
            (Diagnosed::Local { name, url }, uptime)
        }
        Resolved::Cloud { name, database } => {
            checks.push(Check::new(
                "cloud.managed",
                "Helix Cloud manages this database's infrastructure",
                Verdict::Skip(
                    "Storage, caching and regions are set up for you. Query insights and index \
                     recommendations are in the Helix dashboard and the Helix Insights MCP \
                     server."
                        .into(),
                ),
            ));
            (
                Diagnosed::Cloud {
                    name,
                    database: database.to_string(),
                },
                None,
            )
        }
    };
    let summary = checks
        .iter()
        .fold(Summary::default(), |mut summary, check| {
            match check.status {
                Status::Pass => summary.pass += 1,
                Status::Warn => summary.warn += 1,
                Status::Fail => summary.fail += 1,
                Status::Skip => summary.skip += 1,
            }
            summary
        });
    let report = Report {
        target,
        uptime_secs,
        checks,
        summary,
    };
    output::emit(&report, |report| {
        print!("{}", render(report, Verbosity::current()));
        Ok(())
    })?;
    if report.summary.fail == 0 {
        return Ok(());
    }
    Err(CliError::new(format!(
        "{} {} failed",
        report.summary.fail,
        plural(report.summary.fail, "check", "checks")
    ))
    .with_hint("fix the failures above, then run `helix doctor` again")
    .into())
}

/// What to diagnose: a server URL, an instance from helix.toml, or a typed
/// Cloud reference.
enum Resolved {
    Url(String),
    Local {
        name: String,
        config: LocalInstanceConfig,
        runtime: LocalRuntime,
    },
    Cloud {
        name: String,
        database: DatabaseReference,
    },
}

fn resolve(instance: Option<String>, url: Option<String>) -> Result<Resolved> {
    let database = instance
        .as_deref()
        .and_then(|target| target.parse::<DatabaseReference>().ok());
    match (url, database) {
        (Some(url), _) => server_url(&url).map(Resolved::Url),
        (None, Some(database)) => Ok(Resolved::Cloud {
            name: database.to_string(),
            database,
        }),
        (None, None) => {
            let project = ProjectContext::find_and_load(None)?;
            let name = crate::commands::query::resolve_instance_name(&project, instance)?;
            Ok(match project.config.get_instance(&name)? {
                InstanceInfo::Local(config) => Resolved::Local {
                    config: config.clone(),
                    runtime: LocalRuntime::new(&project),
                    name,
                },
                InstanceInfo::Enterprise(config) => Resolved::Cloud {
                    database: config.database.clone(),
                    name,
                },
            })
        }
    }
}

/// `--url` as a base URL without a trailing slash.
fn server_url(url: &str) -> Result<String> {
    let url = url.trim().trim_end_matches('/');
    if url.starts_with("http://") || url.starts_with("https://") {
        return Ok(url.to_owned());
    }
    Err(CliError::new(format!("`{url}` is not a server URL"))
        .with_hint("pass an http:// or https:// URL, e.g. --url http://10.0.1.5:8080")
        .into())
}

fn cli_version(current: &str, latest: &LatestRelease) -> Check {
    match latest {
        LatestRelease::Current => Check::new(
            "cli.version",
            format!("Helix CLI v{current} is up to date"),
            Verdict::Pass(None),
        ),
        LatestRelease::Newer(latest) => Check::new(
            "cli.version",
            format!("Helix CLI v{current} is behind v{latest}"),
            Verdict::Warn {
                detail: "Newer CLIs ship newer server images and checks.".into(),
                fix: "Run `helix update`.".into(),
            },
        ),
        LatestRelease::Disabled => Check::new(
            "cli.version",
            format!("Helix CLI v{current}"),
            Verdict::Skip("Update checks are disabled by HELIX_NO_UPDATE_CHECK.".into()),
        ),
        LatestRelease::Unknown(error) => Check::new(
            "cli.version",
            format!("Helix CLI v{current}"),
            Verdict::Skip(format!("Could not check for a newer release: {error}")),
        ),
    }
}

/// Checks the container runtime, the container, and its image; returns the
/// port a running container serves on, so the server can be asked too.
fn local_checks(
    name: &str,
    config: &LocalInstanceConfig,
    runtime: &LocalRuntime,
    checks: &mut Vec<Check>,
) -> Option<u16> {
    let label = runtime.runtime().label();
    let (running, container) = if LocalRuntime::is_running(runtime.runtime()) {
        checks.push(Check::new(
            "runtime.daemon",
            format!("{label} is running"),
            Verdict::Pass(None),
        ));
        let (container, running) = container_check(name, runtime.status(name), config.port);
        (running, container)
    } else {
        checks.push(Check::new(
            "runtime.daemon",
            format!("{label} is not running"),
            Verdict::Fail {
                detail: Some(format!(
                    "`{} info` did not answer.",
                    runtime.runtime().binary()
                )),
                fix: crate::local_runtime::runtime_unavailable_hint(runtime.runtime()),
            },
        ));
        let container = Check::new(
            "runtime.container",
            format!("{name}'s container"),
            Verdict::Skip(format!("{label} is not running.")),
        );
        (None, container)
    };
    checks.push(container);
    checks.push(image_check(name, config));
    running
}

/// The container's state, and the port it serves on while running: the one
/// it publishes, else `configured`.
fn container_check(
    name: &str,
    status: Result<Option<crate::local_runtime::LocalStatus>>,
    configured: u16,
) -> (Check, Option<u16>) {
    let start = format!("Run `helix start {name}`.");
    match status {
        Ok(Some(status)) if status.status.starts_with("Up") => {
            let port = published_port(&status.ports).unwrap_or(configured);
            (
                Check::new(
                    "runtime.container",
                    format!("Container {} is running", status.container_name),
                    Verdict::Pass(Some(format!("{}, serving on port {port}.", status.status))),
                ),
                Some(port),
            )
        }
        Ok(Some(status)) => (
            Check::new(
                "runtime.container",
                format!("{name} is stopped"),
                Verdict::Fail {
                    detail: Some(format!(
                        "Container {}: {}",
                        status.container_name, status.status
                    )),
                    fix: start,
                },
            ),
            None,
        ),
        Ok(None) => (
            Check::new(
                "runtime.container",
                format!("{name} has not been started"),
                Verdict::Fail {
                    detail: None,
                    fix: start,
                },
            ),
            None,
        ),
        Err(error) => (
            Check::new(
                "runtime.container",
                format!("Could not read {name}'s container"),
                Verdict::Skip(CliError::from_report(&error).message),
            ),
            None,
        ),
    }
}

/// The host port a `ps` Ports column publishes the server's container port
/// on, e.g. `0.0.0.0:7777->8080/tcp, [::]:7777->8080/tcp`.
fn published_port(ports: &str) -> Option<u16> {
    let container = format!("->{}/tcp", crate::local_runtime::CONTAINER_PORT);
    ports.split(", ").find_map(|mapping| {
        mapping
            .strip_suffix(&container)?
            .rsplit_once(':')?
            .1
            .parse()
            .ok()
            .filter(|port| *port != 0)
    })
}

fn image_check(name: &str, config: &LocalInstanceConfig) -> Check {
    let default = crate::config::DEFAULT_LOCAL_IMAGE_TAG;
    let tag = config.tag.to_string();
    let image = config.tag.reference(&config.image);
    if config.image != crate::config::DEFAULT_LOCAL_IMAGE {
        return Check::new(
            "runtime.image",
            format!("{name} runs a custom image"),
            Verdict::Skip(format!(
                "{image} is not checked against this CLI's release."
            )),
        );
    }
    if tag == default {
        return Check::new(
            "runtime.image",
            format!("{name} runs the server image this CLI ships with"),
            Verdict::Pass(Some(image)),
        );
    }
    if tag == "latest" {
        return Check::new(
            "runtime.image",
            format!("{name} tracks the latest server image"),
            Verdict::Pass(Some(format!("{image} is pulled on every start."))),
        );
    }
    let semantic = |tag: &str| {
        tag.strip_prefix('v')
            .filter(|version| version.split('.').all(|part| part.parse::<u64>().is_ok()))
            .map(str::to_owned)
    };
    match (semantic(&tag), semantic(default)) {
        (Some(pinned), Some(shipped)) if update::is_newer_version(&pinned, &shipped) => Check::new(
            "runtime.image",
            format!("{name} pins an older server image"),
            Verdict::Warn {
                detail: format!("{image}; this CLI ships with {default}."),
                fix: format!(
                    "Run `helix stop {name}`, then `helix start {name} --image-version \
                         {default} --persist`."
                ),
            },
        ),
        (Some(_), Some(_)) => Check::new(
            "runtime.image",
            format!("{name} pins server image {tag}"),
            Verdict::Pass(Some(format!("{image}; this CLI ships with {default}."))),
        ),
        (None, _) | (_, None) => Check::new(
            "runtime.image",
            format!("{name} pins server image {tag}"),
            Verdict::Skip(format!("{image} cannot be compared with {default}.")),
        ),
    }
}

/// Asks the server at `url` for its health and checks; returns its uptime
/// when it reported them. `instance` names the target in fixes.
async fn server_checks(url: &str, instance: Option<&str>, checks: &mut Vec<Check>) -> Option<u64> {
    let client = reqwest::Client::new();
    let health = client
        .get(format!("{url}/healthz"))
        .timeout(HEALTH_TIMEOUT)
        .send()
        .await;
    let unreachable_fix = match instance {
        Some(instance) => {
            format!("Check `helix logs {instance}`, then `helix restart {instance}`.")
        }
        None => "Check the URL, and that the server is running and reachable from here.".into(),
    };
    let health = match health {
        Ok(response) if response.status().is_success() => response
            .json::<serde_json::Value>()
            .await
            .unwrap_or_default(),
        Ok(response) => {
            checks.push(Check::new(
                "server.reachable",
                format!("The server at {url} answered HTTP {}", response.status()),
                Verdict::Fail {
                    detail: Some("GET /healthz should always answer 200.".into()),
                    fix: unreachable_fix,
                },
            ));
            return None;
        }
        Err(error) => {
            checks.push(Check::new(
                "server.reachable",
                format!("Cannot reach the server at {url}"),
                Verdict::Fail {
                    detail: Some(error.to_string()),
                    fix: unreachable_fix,
                },
            ));
            return None;
        }
    };
    checks.push(match health["ready"].as_bool() {
        Some(false) => Check::new(
            "server.reachable",
            format!("The server at {url} is not ready"),
            Verdict::Warn {
                detail: format!(
                    "Index runtime: {}.",
                    health["index_runtime"].as_str().unwrap_or("unknown")
                ),
                fix: "Wait for startup to finish; GET /readyz answers 200 once it has.".into(),
            },
        ),
        Some(true) | None => Check::new(
            "server.reachable",
            format!("The server answers at {url}"),
            Verdict::Pass(
                health["mode"]
                    .as_str()
                    .map(|mode| format!("Running as a {mode}.")),
            ),
        ),
    });
    let response = client
        .get(format!("{url}/v2/diagnostics"))
        .timeout(DIAGNOSTICS_TIMEOUT)
        .send()
        .await;
    let report = match response {
        Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => Err(
            "This server predates GET /v2/diagnostics; upgrade its image to check storage, the \
             disk cache, memory and index use."
                .to_owned(),
        ),
        Ok(response) if response.status().is_success() => response
            .json::<ServerReport>()
            .await
            .map_err(|error| format!("Could not read the server's report: {error}")),
        Ok(response) => Err(format!(
            "GET /v2/diagnostics answered HTTP {}.",
            response.status()
        )),
        Err(error) => Err(format!("GET /v2/diagnostics failed: {error}")),
    };
    let report = match report {
        Ok(report) => report,
        Err(detail) => {
            checks.push(Check::new(
                "server.diagnostics",
                "Server checks are unavailable",
                Verdict::Skip(detail),
            ));
            return None;
        }
    };
    checks.extend(report.checks.into_iter().map(|check| Check {
        fix: check.fix.map(|fix| match instance {
            Some(instance) => fix.replace("<instance>", instance),
            None => fix,
        }),
        ..check
    }));
    Some(report.uptime_secs)
}

/// The human report: a header, checks grouped by category, and a summary.
fn render(report: &Report, verbosity: Verbosity) -> String {
    let header = match &report.target {
        Diagnosed::Local { name, url } => format!("{name} (local) · {url}"),
        Diagnosed::Server { url } => url.clone(),
        Diagnosed::Cloud { name, database } if name == database => format!("{name} (cloud)"),
        Diagnosed::Cloud { name, database } => format!("{name} (cloud) · {database}"),
    };
    let uptime = report
        .uptime_secs
        .map(|seconds| format!(" · up {}", uptime(seconds)))
        .unwrap_or_default();
    let mut out = format!(
        "{} {}{}\n",
        style("Helix doctor").bold(),
        header,
        style(uptime).dim()
    );
    let mut categories = Vec::<&str>::new();
    report.checks.iter().for_each(|check| {
        if !categories.contains(&check.category.as_str()) {
            categories.push(&check.category);
        }
    });
    for category in categories {
        let shown = report
            .checks
            .iter()
            .filter(|check| check.category == category)
            .filter(|check| {
                verbosity.show_normal() || matches!(check.status, Status::Warn | Status::Fail)
            })
            .collect::<Vec<_>>();
        if shown.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "\n{}\n",
            style(printable(category_title(category))).bold()
        ));
        for check in shown {
            let symbol = match check.status {
                Status::Pass => style("✓").green(),
                Status::Warn => style("▲").yellow(),
                Status::Fail => style("✗").red(),
                Status::Skip => style("○").dim(),
            };
            out.push_str(&format!("  {symbol} {}\n", printable(&check.summary)));
            let explain = verbosity.show_verbose() || check.status != Status::Pass;
            check
                .detail
                .iter()
                .filter(|_| explain)
                .flat_map(|detail| detail.lines())
                .for_each(|line| out.push_str(&format!("    {}\n", style(printable(line)).dim())));
            check
                .fix
                .iter()
                .flat_map(|fix| fix.lines())
                .enumerate()
                .for_each(|(index, line)| {
                    let lead = if index == 0 { "→ " } else { "  " };
                    out.push_str(&format!("    {}{}\n", style(lead).cyan(), printable(line)));
                });
        }
    }
    let summary = &report.summary;
    let counts = [
        (summary.fail, plural(summary.fail, "failure", "failures")),
        (summary.warn, plural(summary.warn, "warning", "warnings")),
        (summary.pass, "passed"),
        (summary.skip, "skipped"),
    ]
    .into_iter()
    .filter(|(count, _)| *count > 0)
    .map(|(count, label)| format!("{count} {label}"))
    .collect::<Vec<_>>()
    .join(" · ");
    let verdict = match (summary.fail, summary.warn) {
        (0, 0) => style("No problems found").green().bold(),
        (0, _) => style("Setup works, with room to improve").yellow().bold(),
        _ => style("Problems need fixing").red().bold(),
    };
    out.push_str(&format!("\n{verdict} · {counts}\n"));
    out
}

/// `text` with control characters escaped, so names a query client chose
/// cannot drive the terminal that renders them.
fn printable(text: &str) -> String {
    text.chars()
        .fold(String::with_capacity(text.len()), |mut out, character| {
            if character.is_control() {
                out.extend(character.escape_unicode());
            } else {
                out.push(character);
            }
            out
        })
}

fn category_title(category: &str) -> &str {
    match category {
        "cli" => "CLI",
        "runtime" => "Local runtime",
        "server" => "Server",
        "cloud" => "Helix Cloud",
        "storage" => "Storage",
        "cache" => "Disk cache",
        "resources" => "Resources",
        "indexes" => "Indexes",
        "queries" => "Queries",
        other => other,
    }
}

/// Uptime in its coarsest whole unit.
fn uptime(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3_600 => format!("{}m", seconds / 60),
        3_600..86_400 => format!("{}h", seconds / 3_600),
        _ => format!("{}d", seconds / 86_400),
    }
}

const fn plural(count: usize, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 {
        one
    } else {
        many
    }
}

#[cfg(test)]
mod tests;
