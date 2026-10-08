mod support;

use assert_cmd::assert::Assert;
use serde_json::Value;
use std::path::{Path, PathBuf};
use support::CliFixture;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CONTAINER: &str = "helix-explorer-project-dev";
const EXPLORER: &str = "helix-explorer-project-dev.explorer";
const DEFAULT_IMAGE: &str = "ghcr.io/helixdb/helix-explorer:v0.1.0";
const IDENTITY: &str = "16:explorer-project/dev";

fn stdout_json(assert: Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).expect("stdout should be one JSON value")
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

/// An Explorer `/healthz` that reports the instance as `helix`.
async fn explorer_health(helix: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"ok": true, "helix": helix})),
        )
        .mount(&server)
        .await;
    server
}

/// A project whose `dev` instance the fake runtime reports running on 6969,
/// with any Explorer it starts publishing `explorer_port`.
fn project(fixture: &CliFixture) -> PathBuf {
    let project = fixture.root().join("explorer-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();
    project
}

fn explorer(fixture: &CliFixture, project: &Path, explorer_port: u16) -> assert_cmd::Command {
    let mut command = fixture.command();
    command
        .current_dir(project)
        .env("HELIX_TEST_RUNTIME_PORT_OUTPUT", "0.0.0.0:6969")
        .env(
            "HELIX_TEST_RUNTIME_EXPLORER_PORT_OUTPUT",
            format!("127.0.0.1:{explorer_port}"),
        );
    command
}

fn explorer_runs(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|line| line.starts_with("run ") && line.contains(&format!("--name {EXPLORER} ")))
        .collect()
}

/// Where the fake runtime keeps what `inspect` reports for `container`.
fn container_state(fixture: &CliFixture, container: &str) -> PathBuf {
    fixture.root().join(format!("runtime.log.{container}"))
}

/// The runtime log without Windows line endings.
fn runtime_log(fixture: &CliFixture) -> String {
    fixture.runtime_log().replace('\r', "")
}

/// Whether the runtime was asked to remove `container`.
fn removed(fixture: &CliFixture, container: &str) -> bool {
    runtime_log(fixture)
        .lines()
        .any(|line| line == format!("rm -f {container}"))
}

/// The Explorer URL `helix status` lists for `instance`, if any.
fn status_explorer(fixture: &CliFixture, project: &Path, instance: &str) -> Value {
    let status = stdout_json(
        fixture
            .command()
            .current_dir(project)
            .env("HELIX_TEST_RUNTIME_PORT_OUTPUT", "0.0.0.0:6969")
            .env("HELIX_TEST_RUNTIME_EXPLORER_PORT_OUTPUT", "127.0.0.1:6970")
            .args(["status", instance, "--json"])
            .assert()
            .success(),
    );
    status["instances"][0]["explorer"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explorer_starts_against_the_instance_port_then_reuses_and_stops() {
    let server = explorer_health("reachable").await;
    let explorer_port = server.address().port();
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    let ui_port = support::free_port();

    let started = stdout_json(
        explorer(&fixture, &project, explorer_port)
            .args(["explorer", "dev", "--port"])
            .arg(ui_port.to_string())
            .arg("--json")
            .assert()
            .success(),
    );
    let url = format!("http://127.0.0.1:{explorer_port}");
    assert_eq!(started["instance"], "dev");
    assert_eq!(started["url"], url.as_str());
    assert_eq!(started["container"], EXPLORER);
    assert_eq!(started["image"], DEFAULT_IMAGE);
    assert_eq!(started["helixUrl"], "http://host.docker.internal:6969");
    assert_eq!(started["helix"], "reachable");
    assert_eq!(started["reused"], false);

    let log = fixture.runtime_log().replace('\r', "");
    assert!(log.contains(&format!("port {CONTAINER} 8080/tcp")), "{log}");
    assert_eq!(
        explorer_runs(&log),
        [format!(
            "run -d --rm --name {EXPLORER} --label helixdb.identity={IDENTITY} \
             --label helixdb.role=explorer --add-host host.docker.internal:host-gateway \
             -e HELIX_URL=http://host.docker.internal:6969 -e HELIX_INSTANCE_NAME=dev \
             -p 127.0.0.1:{ui_port}:3000 {DEFAULT_IMAGE}"
        )],
        "{log}"
    );
    // Tests are never interactive, so no browser opens.
    assert!(
        fixture.host_actions().is_empty(),
        "{}",
        fixture.host_actions()
    );

    let reused = stdout_json(
        explorer(&fixture, &project, explorer_port)
            .args(["explorer", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(reused["reused"], true);
    assert_eq!(reused["url"], url.as_str());
    // The image and instance URL come from the running container.
    assert_eq!(reused["image"], DEFAULT_IMAGE);
    assert_eq!(reused["helixUrl"], "http://host.docker.internal:6969");
    assert_eq!(explorer_runs(&fixture.runtime_log()).len(), 1);

    let status = stdout_json(
        explorer(&fixture, &project, explorer_port)
            .env(
                "HELIX_TEST_RUNTIME_PS_OUTPUT",
                format!("{CONTAINER}\tUp 1 minute\t0.0.0.0:6969"),
            )
            .args(["status", "dev", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(status["instances"][0]["explorer"], url.as_str());
    // An Explorer outlives an instance container that is gone.
    let orphaned = stdout_json(
        explorer(&fixture, &project, explorer_port)
            .args(["status", "dev", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(orphaned["instances"][0]["state"], "not created");
    assert_eq!(orphaned["instances"][0]["explorer"], url.as_str());

    let stopped = stdout_json(
        explorer(&fixture, &project, explorer_port)
            .args(["explorer", "--stop", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(stopped["wasRunning"], true);
    assert_eq!(stopped["container"], EXPLORER);
    assert!(fixture.runtime_log().contains(&format!("rm -f {EXPLORER}")));
    let stopped_again = stdout_json(
        explorer(&fixture, &project, explorer_port)
            .args(["explorer", "--stop", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(stopped_again["wasRunning"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_explorer_on_another_port_is_replaced() {
    let server = explorer_health("reachable").await;
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    explorer(&fixture, &project, server.address().port())
        .args(["explorer", "--no-open"])
        .assert()
        .success();

    let other_port = support::free_port();
    let replaced = stderr(
        explorer(&fixture, &project, server.address().port())
            .args(["explorer", "--port"])
            .arg(other_port.to_string())
            .assert()
            .success(),
    );
    assert!(
        replaced.contains(&format!(
            "Replacing the running Explorer: it serves port {}, not {other_port}",
            server.address().port()
        )),
        "{replaced}"
    );
    let log = runtime_log(&fixture);
    let runs = explorer_runs(&log);
    assert_eq!(runs.len(), 2, "{log}");
    assert!(
        runs[1].contains(&format!("-p 127.0.0.1:{other_port}:3000 ")),
        "{log}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_explorer_is_replaced_when_the_image_changes() {
    let server = explorer_health("reachable").await;
    let port = server.address().port();
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    explorer(&fixture, &project, port)
        .args(["explorer", "--no-open"])
        .assert()
        .success();

    let replaced = stderr(
        explorer(&fixture, &project, port)
            .args(["explorer", "--no-open", "--image", "helix-explorer:local"])
            .assert()
            .success(),
    );
    assert!(
        replaced.contains(&format!(
            "Replacing the running Explorer: it runs {DEFAULT_IMAGE}, not helix-explorer:local"
        )),
        "{replaced}"
    );
    let log = runtime_log(&fixture);
    let runs = explorer_runs(&log);
    assert_eq!(runs.len(), 2, "{log}");
    // The replacement keeps the port the running Explorer frees.
    assert!(
        runs[1].contains(&format!("-p 127.0.0.1:{port}:3000 ")),
        "{log}"
    );
    assert!(runs[1].ends_with(" helix-explorer:local"), "{log}");

    // The new container reports the new image, so it is reused from now on.
    let reused = stdout_json(
        explorer(&fixture, &project, port)
            .args(["explorer", "--image", "helix-explorer:local", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(reused["reused"], true);
    assert_eq!(reused["image"], "helix-explorer:local");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebuilt_image_replaces_the_explorer_running_its_old_build() {
    let server = explorer_health("reachable").await;
    let port = server.address().port();
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    explorer(&fixture, &project, port)
        .args(["explorer", "--no-open", "--image", "helix-explorer:local"])
        .assert()
        .success();

    let rebuilt = || {
        let mut command = explorer(&fixture, &project, port);
        command
            .env("HELIX_TEST_RUNTIME_EXPLORER_IMAGE_ID", "sha256:rebuilt")
            .args(["explorer", "--no-open", "--image", "helix-explorer:local"]);
        command
    };
    let replaced = stderr(rebuilt().assert().success());
    assert!(
        replaced.contains("helix-explorer:local has changed since it started"),
        "{replaced}"
    );
    let again = stderr(rebuilt().assert().success());
    assert!(again.contains("The Explorer is already running"), "{again}");
    assert_eq!(explorer_runs(&runtime_log(&fixture)).len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_explorer_is_replaced_when_the_instance_port_changes() {
    let server = explorer_health("reachable").await;
    let port = server.address().port();
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    explorer(&fixture, &project, port)
        .args(["explorer", "--no-open"])
        .assert()
        .success();

    let replaced = stdout_json(
        explorer(&fixture, &project, port)
            .env("HELIX_TEST_RUNTIME_PORT_OUTPUT", "0.0.0.0:7000")
            .args(["explorer", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(replaced["reused"], false);
    assert_eq!(replaced["helixUrl"], "http://host.docker.internal:7000");
    let log = runtime_log(&fixture);
    let runs = explorer_runs(&log);
    assert_eq!(runs.len(), 2, "{log}");
    assert!(
        runs[1].contains(" -e HELIX_URL=http://host.docker.internal:7000 "),
        "{log}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_pull_keeps_the_running_explorer() {
    let server = explorer_health("reachable").await;
    let port = server.address().port();
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    explorer(&fixture, &project, port)
        .args(["explorer", "--no-open"])
        .assert()
        .success();

    let error = stderr(
        explorer(&fixture, &project, port)
            .env("HELIX_TEST_RUNTIME_IMAGE_MISSING", "1")
            .env("HELIX_TEST_RUNTIME_FAIL_IMAGE", "helix-explorer:broken")
            .args(["explorer", "--no-open", "--image", "helix-explorer:broken"])
            .assert()
            .failure(),
    );
    assert!(
        error.contains("failed to pull helix-explorer:broken"),
        "{error}"
    );
    assert!(!removed(&fixture, EXPLORER), "{}", runtime_log(&fixture));
    assert_eq!(explorer_runs(&runtime_log(&fixture)).len(), 1);
    assert_eq!(
        status_explorer(&fixture, &project, "dev"),
        "http://127.0.0.1:6970"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_busy_port_keeps_the_running_explorer() {
    let server = explorer_health("reachable").await;
    let port = server.address().port();
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    explorer(&fixture, &project, port)
        .args(["explorer", "--no-open"])
        .assert()
        .success();

    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy_port = busy.local_addr().unwrap().port();
    let error = stderr(
        explorer(&fixture, &project, port)
            .args(["explorer", "--no-open", "--port"])
            .arg(busy_port.to_string())
            .assert()
            .failure(),
    );
    assert!(
        error.contains(&format!("port {busy_port} is already in use")),
        "{error}"
    );
    assert!(!removed(&fixture, EXPLORER), "{}", runtime_log(&fixture));
    assert_eq!(
        status_explorer(&fixture, &project, "dev"),
        "http://127.0.0.1:6970"
    );
}

/// The port an explicit `--port` names is busy only because the Explorer
/// being replaced holds it, so the replacement takes it over.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replacement_can_take_over_the_port_its_predecessor_holds() {
    let server = explorer_health("reachable").await;
    // The fake reports the Explorer on the health server's port, which the
    // server holds as the running container would.
    let port = server.address().port();
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    explorer(&fixture, &project, port)
        .args(["explorer", "--no-open"])
        .assert()
        .success();

    explorer(&fixture, &project, port)
        .args([
            "explorer",
            "--no-open",
            "--image",
            "helix-explorer:local",
            "--port",
        ])
        .arg(port.to_string())
        .assert()
        .success();
    let log = runtime_log(&fixture);
    let runs = explorer_runs(&log);
    assert_eq!(runs.len(), 2, "{log}");
    assert!(
        runs[1].contains(&format!("-p 127.0.0.1:{port}:3000 ")),
        "{log}"
    );
    // The new image is in place before the old Explorer goes.
    let lines: Vec<_> = log.lines().collect();
    let removal = lines
        .iter()
        .position(|line| *line == format!("rm -f {EXPLORER}"))
        .expect("the old Explorer is removed");
    let inspected = lines
        .iter()
        .rposition(|line| {
            line.starts_with("image inspect ") && line.ends_with(" helix-explorer:local")
        })
        .expect("the new image is checked");
    assert!(inspected < removal, "{log}");
}

#[test]
fn explorer_requires_a_running_instance() {
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    let error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["explorer", "dev"])
            .assert()
            .failure(),
    );
    assert!(
        error.contains("local instance 'dev' is not running"),
        "{error}"
    );
    assert!(error.contains("helix start dev"), "{error}");
    assert!(explorer_runs(&fixture.runtime_log()).is_empty());
}

#[test]
fn explorer_rejects_cloud_instances_and_invalid_images_before_the_runtime() {
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    let config = project.join("helix.toml");
    let toml = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!("{toml}\n[enterprise.production]\ndatabase = \"tenant:t\"\n"),
    )
    .unwrap();
    // `init` probes the runtime; nothing after it may.
    let after_init = fixture.runtime_log();

    let cloud = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["explorer", "production"])
            .assert()
            .failure(),
    );
    assert!(cloud.contains("is a Helix Cloud instance"), "{cloud}");

    let image = stderr(
        fixture
            .command()
            .current_dir(&project)
            .env("HELIX_EXPLORER_IMAGE", "--privileged")
            .args(["explorer", "dev"])
            .assert()
            .failure(),
    );
    assert!(image.contains("not an image reference"), "{image}");
    assert_eq!(fixture.runtime_log(), after_init);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explorer_pulls_a_missing_image_and_honors_the_image_variable() {
    let server = explorer_health("reachable").await;
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    let report = stdout_json(
        explorer(&fixture, &project, server.address().port())
            .env("HELIX_EXPLORER_IMAGE", "helix-explorer:env")
            .env("HELIX_TEST_RUNTIME_IMAGE_MISSING", "1")
            .args(["explorer", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(report["image"], "helix-explorer:env");
    let log = fixture.runtime_log().replace('\r', "");
    assert!(log.contains("pull helix-explorer:env\n"), "{log}");
    let runs = explorer_runs(&log);
    assert!(runs[0].ends_with(" helix-explorer:env"), "{log}");
}

#[test]
fn a_failed_pull_starts_nothing() {
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    let error = stderr(
        explorer(&fixture, &project, 1)
            .env("HELIX_TEST_RUNTIME_IMAGE_MISSING", "1")
            .env("HELIX_TEST_RUNTIME_FAIL_IMAGE", "helix-explorer:local")
            .args(["explorer", "--image", "helix-explorer:local"])
            .assert()
            .failure(),
    );
    assert!(
        error.contains("failed to pull helix-explorer:local"),
        "{error}"
    );
    assert!(error.contains("HELIX_EXPLORER_IMAGE"), "{error}");
    assert!(explorer_runs(&fixture.runtime_log()).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_instance_is_reported_without_failing() {
    let server = explorer_health("unreachable").await;
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    let output = stderr(
        explorer(&fixture, &project, server.address().port())
            .args(["explorer"])
            .assert()
            .success(),
    );
    assert!(
        output.contains("cannot reach dev at http://host.docker.internal:6969"),
        "{output}"
    );
    assert!(
        output.contains(&format!("http://127.0.0.1:{}", server.address().port())),
        "{output}"
    );
}

#[test]
fn an_explorer_that_exits_at_once_fails_fast_with_a_way_to_see_why() {
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    let ui_port = support::free_port();
    // The container starts but publishes nothing, as one that already exited.
    let error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .env("HELIX_TEST_RUNTIME_PORT_OUTPUT", "0.0.0.0:6969")
            .args(["explorer", "--port"])
            .arg(ui_port.to_string())
            .assert()
            .failure(),
    );
    assert!(
        error.contains("the Explorer container stopped before it became ready"),
        "{error}"
    );
    assert!(
        error.contains(&format!("docker run --rm -p 127.0.0.1:{ui_port}:3000")),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_an_instance_stops_its_explorer_too() {
    let server = explorer_health("reachable").await;
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    explorer(&fixture, &project, server.address().port())
        .args(["explorer", "--no-open"])
        .assert()
        .success();

    let stopped = stdout_json(
        fixture
            .command()
            .current_dir(&project)
            .args(["stop", "dev", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(stopped["explorerStopped"], true);
    assert!(fixture.runtime_log().contains(&format!("rm -f {EXPLORER}")));

    let again = stdout_json(
        fixture
            .command()
            .current_dir(&project)
            .args(["stop", "dev", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(again["explorerStopped"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_explorer_removal_warns_without_failing_the_stop() {
    let server = explorer_health("reachable").await;
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    explorer(&fixture, &project, server.address().port())
        .args(["explorer", "--no-open"])
        .assert()
        .success();

    let warning = stderr(
        fixture
            .command()
            .current_dir(&project)
            .env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "inspect")
            .args(["stop", "dev"])
            .assert()
            .success(),
    );
    assert!(
        warning.contains("Could not stop the dev Explorer (")
            && warning.contains("simulated runtime failure"),
        "{warning}"
    );
    assert!(
        warning.contains("retry with `helix explorer dev --stop`"),
        "{warning}"
    );

    let report = stdout_json(
        fixture
            .command()
            .current_dir(&project)
            .env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "inspect")
            .args(["stop", "dev", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(report["explorerStopped"], false);
    let error = report["explorerError"].as_str().unwrap();
    assert!(error.contains("simulated runtime failure"), "{error}");
    assert!(!error.contains('\n'), "{error:?}");
}

/// `helix-<project>-dev-explorer` is the `dev-explorer` instance's own
/// container, never `dev`'s Explorer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_instance_named_like_an_explorer_is_never_touched() {
    let server = explorer_health("reachable").await;
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    let config = project.join("helix.toml");
    let toml = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        format!("{toml}\n[local.dev-explorer]\nport = 7979\n"),
    )
    .unwrap();
    let neighbour = "helix-explorer-project-dev-explorer";

    explorer(&fixture, &project, server.address().port())
        .args(["explorer", "dev", "--no-open"])
        .assert()
        .success();
    explorer(&fixture, &project, server.address().port())
        .args(["explorer", "dev", "--stop"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args(["stop", "dev"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args(["prune", "dev", "--yes"])
        .assert()
        .success();

    let log = runtime_log(&fixture);
    assert!(removed(&fixture, EXPLORER), "{log}");
    assert!(!removed(&fixture, neighbour), "{log}");
    assert!(
        !log.split_whitespace().any(|word| word == neighbour),
        "nothing may name the neighbour's container: {log}"
    );
}

/// A container that holds the Explorer's name without the Explorer's labels
/// is someone else's: it is never reused, listed, or removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_container_at_the_explorer_name_is_left_alone() {
    let server = explorer_health("reachable").await;
    let fixture = CliFixture::new_with_fake_runtime();
    let project = project(&fixture);
    // The instance's identity, but not the Explorer role.
    std::fs::write(
        container_state(&fixture, EXPLORER),
        format!("{IDENTITY}\n\nsha256:other\nnginx:latest\nPATH=/bin\n"),
    )
    .unwrap();

    let stopped = stdout_json(
        explorer(&fixture, &project, server.address().port())
            .args(["explorer", "--stop", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(stopped["wasRunning"], false);
    let instance_stopped = stdout_json(
        fixture
            .command()
            .current_dir(&project)
            .args(["stop", "dev", "--json"])
            .assert()
            .success(),
    );
    assert_eq!(instance_stopped["explorerStopped"], false);
    assert_eq!(status_explorer(&fixture, &project, "dev"), Value::Null);

    let error = stderr(
        explorer(&fixture, &project, server.address().port())
            .args(["explorer", "--no-open"])
            .assert()
            .failure(),
    );
    assert!(
        error.contains(&format!(
            "the container name {EXPLORER} is taken by a container Helix did not start"
        )),
        "{error}"
    );
    assert!(!removed(&fixture, EXPLORER), "{}", runtime_log(&fixture));
    assert!(explorer_runs(&runtime_log(&fixture)).is_empty());
    assert!(container_state(&fixture, EXPLORER).exists());
}
