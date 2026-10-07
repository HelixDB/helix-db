mod support;

use assert_cmd::assert::Assert;
use serde_json::Value;
use std::path::{Path, PathBuf};
use support::CliFixture;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CONTAINER: &str = "helix-explorer-project-dev";
const EXPLORER: &str = "helix-explorer-project-dev-explorer";
const DEFAULT_IMAGE: &str = "ghcr.io/helixdb/helix-explorer:latest";

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
            "run -d --rm --name {EXPLORER} --label helixdb.identity=16:explorer-project/dev \
             --add-host host.docker.internal:host-gateway \
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
        replaced.contains("Replacing the running Explorer"),
        "{replaced}"
    );
    let log = fixture.runtime_log().replace('\r', "");
    let runs = explorer_runs(&log);
    assert_eq!(runs.len(), 2, "{log}");
    assert!(
        runs[1].contains(&format!("-p 127.0.0.1:{other_port}:3000 ")),
        "{log}"
    );
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
