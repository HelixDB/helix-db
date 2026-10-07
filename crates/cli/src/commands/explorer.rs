//! `helix explorer`: run the graph Explorer image against a running local
//! instance and open it in a browser.
//!
//! The image serves the UI, `POST /api/query` (forwarded to the instance's
//! `/v2/query`), and `GET /healthz` on container port 3000. The CLI publishes
//! that port on loopback, points `HELIX_URL` at the port the instance publishes
//! on the host, and names the container after the instance's own container.

use crate::config::{DEFAULT_EXPLORER_IMAGE, DEFAULT_EXPLORER_IMAGE_TAG};
use crate::errors::CliError;
use crate::local_runtime::{ExplorerLaunch, LocalRuntime, RunningExplorer};
use crate::output::{self, table, Operation, Step};
use crate::project::ProjectContext;
use crate::{host_actions, port, prompts};
use eyre::Result;
use serde::Serialize;
use serde_json::Value;
use std::time::{Duration, Instant};

/// Host port for the Explorer UI without `--port`. When it is taken, the
/// Explorer uses the next free port instead.
pub const DEFAULT_EXPLORER_PORT: u16 = 6970;
/// Overrides the default Explorer image; `--image` overrides it in turn.
pub const EXPLORER_IMAGE_ENV: &str = "HELIX_EXPLORER_IMAGE";
/// How long a started Explorer has to answer `GET /healthz`.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Failed readiness polls between checks that the container still runs, so
/// an image that exits at once fails in about a second instead of 30.
const LIVENESS_CHECK_EVERY: u32 = 4;

#[derive(clap::Args, Debug)]
#[command(after_help = "Examples:
  helix explorer
  helix explorer dev --no-open
  helix explorer dev --image helix-explorer:local
  helix explorer dev --stop

Docs: https://docs.helix-db.com/cli/command-reference/explorer")]
pub struct Args {
    /// Local instance to explore; defaults to dev or the only local instance
    pub instance: Option<String>,
    /// Host port for the Explorer UI [default: 6970, or the next free port]
    #[arg(long, value_name = "PORT", conflicts_with = "stop")]
    pub port: Option<u16>,
    /// Don't open the Explorer in a browser
    #[arg(long, conflicts_with = "stop")]
    pub no_open: bool,
    /// Explorer image [env: HELIX_EXPLORER_IMAGE] [default: ghcr.io/helixdb/helix-explorer:latest]
    #[arg(long, value_name = "REF", conflicts_with = "stop")]
    pub image: Option<String>,
    /// Stop and remove this instance's Explorer
    #[arg(long)]
    pub stop: bool,
}

/// The `--json` result of opening the Explorer.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    instance: String,
    url: String,
    container: String,
    image: String,
    /// The instance's base URL as the container reaches it.
    helix_url: String,
    /// What `/healthz` reported about the instance: `reachable` or
    /// `unreachable`.
    #[serde(skip_serializing_if = "Option::is_none")]
    helix: Option<String>,
    /// Whether an Explorer that was already running was kept.
    reused: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let cloud = args
        .instance
        .as_deref()
        .filter(|name| project.config.enterprise.contains_key(*name));
    if let Some(name) = cloud {
        return Err(CliError::new(format!("'{name}' is a Helix Cloud instance"))
            .with_hint(
                "`helix explorer` runs against local instances; Cloud databases have the \
                 Explorer in the Helix dashboard",
            )
            .into());
    }
    let instance =
        project.resolve_local_instance(args.instance, "Open the Explorer for which instance?")?;
    let runtime = LocalRuntime::new(&project);
    if args.stop {
        LocalRuntime::check_available(runtime.runtime())?;
        return stop(&runtime, &instance);
    }
    let image = explorer_image(args.image, std::env::var(EXPLORER_IMAGE_ENV).ok())?;
    LocalRuntime::check_available(runtime.runtime())?;

    output::intro(&format!("Opening the Explorer for {instance}"));
    let Some(instance_port) = runtime.instance_port(&instance) else {
        return Err(
            CliError::new(format!("local instance '{instance}' is not running"))
                .with_hint(format!(
                    "start it with `helix start {instance}`, then run `helix explorer {instance}` again"
                ))
                .into(),
        );
    };
    let helix_url = runtime.explorer_helix_url(instance_port);

    let running = runtime.running_explorer(&instance);
    let was_running = running.is_some();
    let reusable = running.filter(|running| {
        let Some(reason) = replacement_reason(running, &image, &helix_url, args.port) else {
            return true;
        };
        output::remark(&format!("Replacing the running Explorer: {reason}"));
        false
    });
    let (port, image, reused) = match reusable {
        Some(running) => {
            output::info(&format!(
                "The Explorer is already running on port {}",
                running.port
            ));
            (running.port, running.image.unwrap_or(image), true)
        }
        None => {
            if was_running {
                // Free the replaced Explorer's port before picking one.
                runtime.remove_explorer(&instance)?;
            }
            let port = launch(&runtime, &instance, &image, &helix_url, args.port).await?;
            (port, image, false)
        }
    };

    let helix = wait_ready(&runtime, &instance, port, &image, &helix_url).await?;
    if helix.as_deref() == Some("unreachable") {
        output::warning(&format!(
            "The Explorer is up but cannot reach {instance} at {helix_url}. Check the instance \
             with `helix status {instance}`."
        ));
    }

    let url = explorer_url(port);
    let container = runtime.explorer_container_name(&instance);
    output::note(
        &format!("{instance} Explorer"),
        &table::key_values(&[
            ("URL", url.clone()),
            ("Instance", format!("http://localhost:{instance_port}")),
            ("Image", image.clone()),
            ("Container", container.clone()),
        ]),
    );
    if !args.no_open && prompts::is_interactive() {
        match host_actions::open_url(&url) {
            Ok(()) => output::info(&format!("Opened {url} in your browser")),
            Err(error) => output::warning(&format!(
                "Could not open a browser ({error}); open {url} yourself"
            )),
        }
    }
    output::outro(&format!("The {instance} Explorer is running at {url}"));
    output::emit(
        &Report {
            instance,
            url,
            container,
            image,
            helix_url,
            helix,
            reused,
        },
        |_| Ok(()),
    )
}

/// The Explorer URL for a host port. The container publishes on loopback
/// only, so the URL names `127.0.0.1` rather than `localhost`, which can
/// resolve to `::1` first.
pub fn explorer_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

/// The image to run: `--image`, else a non-empty `HELIX_EXPLORER_IMAGE`, else
/// the default. A reference that the runtime would read as a flag, or that
/// holds whitespace, is rejected before any container command.
fn explorer_image(flag: Option<String>, env: Option<String>) -> Result<String, CliError> {
    let image = flag
        .or_else(|| env.filter(|image| !image.trim().is_empty()))
        .unwrap_or_else(|| format!("{DEFAULT_EXPLORER_IMAGE}:{DEFAULT_EXPLORER_IMAGE_TAG}"));
    if image.is_empty() || image.starts_with('-') || image.contains(char::is_whitespace) {
        return Err(
            CliError::new(format!("'{image}' is not an image reference")).with_hint(format!(
                "pass a reference such as {DEFAULT_EXPLORER_IMAGE}:{DEFAULT_EXPLORER_IMAGE_TAG} or \
                 helix-explorer:local"
            )),
        );
    }
    Ok(image)
}

/// Why a running Explorer cannot serve this request, or `None` to reuse it.
/// What the runtime did not report is not held against it.
fn replacement_reason(
    running: &RunningExplorer,
    image: &str,
    helix_url: &str,
    explicit_port: Option<u16>,
) -> Option<String> {
    if let Some(stale) = running.helix_url.as_deref().filter(|url| *url != helix_url) {
        return Some(format!(
            "it reads {stale}, but the instance now listens at {helix_url}"
        ));
    }
    // Podman reports short names in full (`localhost/helix-explorer:local`).
    let same_image = |running: &str| {
        running == image
            || running.ends_with(&format!("/{image}"))
            || image.ends_with(&format!("/{running}"))
    };
    if let Some(other) = running.image.as_deref().filter(|other| !same_image(other)) {
        return Some(format!("it runs {other}, not {image}"));
    }
    explicit_port
        .filter(|port| *port != running.port)
        .map(|port| format!("it serves port {}, not {port}", running.port))
}

/// Pick the host port, make the image available, and start the container.
/// Returns the port the runtime reports it published.
async fn launch(
    runtime: &LocalRuntime,
    instance: &str,
    image: &str,
    helix_url: &str,
    explicit_port: Option<u16>,
) -> Result<u16> {
    let port = select_port(explicit_port)?;
    runtime.ensure_image(image).await.map_err(|error| {
        CliError::from_report(&error).with_hint(format!(
            "check the image reference, or run another with --image or {EXPLORER_IMAGE_ENV}"
        ))
    })?;
    runtime.run_explorer(
        instance,
        &ExplorerLaunch {
            image: image.to_owned(),
            port,
            helix_url: helix_url.to_owned(),
        },
    )?;
    runtime
        .explorer_port(instance)
        .ok_or_else(|| exited_error(runtime, port, image, helix_url).into())
}

/// An explicit `--port` is used as given and must be free; without one the
/// default is used, or the next free port when it is taken.
fn select_port(explicit_port: Option<u16>) -> Result<u16> {
    let Some(port) = explicit_port else {
        let (port, moved) = port::ensure_port_available(DEFAULT_EXPLORER_PORT)?;
        if moved {
            output::warning(&format!(
                "Port {DEFAULT_EXPLORER_PORT} is in use, so the Explorer uses port {port}"
            ));
        }
        return Ok(port);
    };
    if !port::is_port_available(port) {
        return Err(CliError::new(format!("port {port} is already in use"))
            .with_hint(format!(
                "pass another with --port, or omit --port to use {DEFAULT_EXPLORER_PORT} or the \
                 next free port"
            ))
            .into());
    }
    Ok(port)
}

/// Poll `/healthz` until the Explorer answers 200 and return its `helix`
/// field. Fails when the container stops or the timeout passes first.
async fn wait_ready(
    runtime: &LocalRuntime,
    instance: &str,
    port: u16,
    image: &str,
    helix_url: &str,
) -> Result<Option<String>> {
    let mut step = Step::with_messages(
        "Waiting for the Explorer",
        "The Explorer did not become ready",
    );
    step.start();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .no_proxy()
        .build()?;
    let url = format!("{}/healthz", explorer_url(port));
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut failures = 0u32;
    loop {
        let healthy = client
            .get(&url)
            .send()
            .await
            .ok()
            .filter(|response| response.status().is_success());
        let Some(response) = healthy else {
            failures += 1;
            if failures.is_multiple_of(LIVENESS_CHECK_EVERY)
                && runtime.explorer_port(instance).is_none()
            {
                step.fail();
                return Err(exited_error(runtime, port, image, helix_url).into());
            }
            if Instant::now() >= deadline {
                step.fail();
                return Err(CliError::new(format!(
                    "the Explorer did not answer {url} within {} s",
                    READY_TIMEOUT.as_secs()
                ))
                .with_hint(format!(
                    "check its logs with `{} logs {}`, or stop it with `helix explorer {instance} --stop`",
                    runtime.runtime().binary(),
                    runtime.explorer_container_name(instance)
                ))
                .into());
            }
            tokio::time::sleep(READY_POLL_INTERVAL).await;
            continue;
        };
        let body = response.json::<Value>().await.ok();
        step.set_completion("Explorer is ready");
        step.done();
        return Ok(body
            .as_ref()
            .and_then(|body| body.get("helix"))
            .and_then(Value::as_str)
            .map(str::to_owned));
    }
}

/// The container removes itself when it stops, logs included, so the hint
/// runs the image attached to show its output.
fn exited_error(runtime: &LocalRuntime, port: u16, image: &str, helix_url: &str) -> CliError {
    CliError::new("the Explorer container stopped before it became ready").with_hint(format!(
        "run it attached to see why: `{} run --rm -p 127.0.0.1:{port}:3000 -e HELIX_URL={helix_url} {image}`",
        runtime.runtime().binary()
    ))
}

fn stop(runtime: &LocalRuntime, instance: &str) -> Result<()> {
    let op = Operation::new("Stopping", &format!("the {instance} Explorer"));
    let was_running = runtime.remove_explorer(instance)?;
    if was_running {
        op.success();
    } else {
        output::outro(&format!("The {instance} Explorer was not running"));
    }
    output::emit(
        &serde_json::json!({
            "instance": instance,
            "container": runtime.explorer_container_name(instance),
            "wasRunning": was_running,
        }),
        |_| Ok(()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running(port: u16, image: Option<&str>, helix_url: Option<&str>) -> RunningExplorer {
        RunningExplorer {
            port,
            image: image.map(str::to_owned),
            helix_url: helix_url.map(str::to_owned),
        }
    }

    #[test]
    fn image_comes_from_the_flag_then_the_environment_then_the_default() {
        assert_eq!(
            explorer_image(Some("a:1".into()), Some("b:2".into())).unwrap(),
            "a:1"
        );
        assert_eq!(explorer_image(None, Some("b:2".into())).unwrap(), "b:2");
        assert_eq!(
            explorer_image(None, None).unwrap(),
            "ghcr.io/helixdb/helix-explorer:latest"
        );
        // An empty variable counts as unset, as shells leave `VAR=` behind.
        assert_eq!(
            explorer_image(None, Some("  ".into())).unwrap(),
            "ghcr.io/helixdb/helix-explorer:latest"
        );
    }

    #[test]
    fn image_references_the_runtime_would_misread_are_rejected() {
        for invalid in ["", "--privileged", "-it", "helix explorer:local", "a:1\n"] {
            let error = explorer_image(Some(invalid.into()), None).unwrap_err();
            assert!(
                error.message.contains("not an image reference"),
                "{invalid:?}"
            );
            assert!(error.hint.is_some());
        }
    }

    #[test]
    fn a_matching_explorer_is_reused() {
        let url = "http://host.docker.internal:6969";
        let image = "ghcr.io/helixdb/helix-explorer:latest";
        assert_eq!(
            replacement_reason(&running(6970, Some(image), Some(url)), image, url, None),
            None
        );
        assert_eq!(
            replacement_reason(
                &running(6970, Some(image), Some(url)),
                image,
                url,
                Some(6970)
            ),
            None
        );
        // Nothing reported means nothing to compare.
        assert_eq!(
            replacement_reason(&running(6971, None, None), image, url, None),
            None
        );
        assert_eq!(
            replacement_reason(
                &running(6970, Some("localhost/helix-explorer:local"), Some(url)),
                "helix-explorer:local",
                url,
                None
            ),
            None
        );
    }

    #[test]
    fn a_stale_or_different_explorer_is_replaced() {
        let url = "http://host.docker.internal:6969";
        let image = "ghcr.io/helixdb/helix-explorer:latest";
        let stale = replacement_reason(
            &running(6970, Some(image), Some("http://host.docker.internal:7000")),
            image,
            url,
            None,
        )
        .unwrap();
        assert!(stale.contains(":7000") && stale.contains(url), "{stale}");
        let other_image = replacement_reason(
            &running(6970, Some(image), Some(url)),
            "helix-explorer:local",
            url,
            None,
        )
        .unwrap();
        assert!(
            other_image.contains("helix-explorer:local"),
            "{other_image}"
        );
        let other_port = replacement_reason(
            &running(6970, Some(image), Some(url)),
            image,
            url,
            Some(7100),
        )
        .unwrap();
        assert!(other_port.contains("7100"), "{other_port}");
    }

    #[test]
    fn explorer_urls_name_the_loopback_address_it_is_published_on() {
        assert_eq!(explorer_url(6970), "http://127.0.0.1:6970");
    }

    #[test]
    fn an_explicit_port_that_is_taken_is_an_error_not_a_silent_move() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let taken = listener.local_addr().unwrap().port();
        let error = select_port(Some(taken)).unwrap_err();
        let error = CliError::from_report(&error);
        assert!(
            error.message.contains(&taken.to_string()),
            "{}",
            error.message
        );
        assert!(error.hint.unwrap().contains("--port"));
    }

    #[test]
    fn exited_error_shows_how_to_run_the_image_attached() {
        let runtime = LocalRuntime::new(&ProjectContext {
            root: std::path::PathBuf::from("/tmp"),
            helix_dir: std::path::PathBuf::from("/tmp/.helix"),
            config: crate::config::HelixConfig::default_config("demo"),
        });
        let hint = exited_error(
            &runtime,
            6970,
            "helix-explorer:local",
            "http://host.docker.internal:6969",
        )
        .hint
        .unwrap();
        assert!(hint.contains(
            "docker run --rm -p 127.0.0.1:6970:3000 -e HELIX_URL=http://host.docker.internal:6969 helix-explorer:local"
        ), "{hint}");
    }
}
