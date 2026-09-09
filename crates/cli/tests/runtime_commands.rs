mod support;

use assert_cmd::assert::Assert;
use std::fs;
use support::CliFixture;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).expect("stdout should be utf8")
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_runtime_commands_cover_resource_reuse_status_cleanup_and_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("disk-command-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();

    fixture
        .command()
        .current_dir(&project)
        .args(["start", "dev"])
        .assert()
        .success();

    let container = "helix-disk-command-project-dev";
    let ps_output = format!(
        "{container}\tUp 1 minute\tlocalhost:{}",
        server.address().port()
    );
    let status = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["status", "dev"])
            .env("HELIX_TEST_RUNTIME_PS_OUTPUT", &ps_output)
            .assert()
            .success(),
    );
    assert!(status.contains("Up 1 minute"));
    assert!(status.contains("storage: disk"));
    assert!(status.contains(&server.address().port().to_string()));

    fixture
        .command()
        .current_dir(&project)
        .args(["logs", "dev", "--follow"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args(["restart", "dev"])
        .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
        .assert()
        .success();

    let stopped = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["stop", "dev"])
            .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
            .assert()
            .success(),
    );
    assert!(stopped.contains("Stopped 'dev' successfully"));
    fixture
        .command()
        .current_dir(&project)
        .args(["prune", "--all", "--yes"])
        .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
        .assert()
        .success();

    let status_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["status", "dev"])
            .env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "ps")
            .assert()
            .failure(),
    );
    assert!(status_error.contains("simulated runtime failure"));

    let log = fixture.runtime_log();
    assert!(log.contains("network create"));
    assert!(log.contains("volume create"));
    assert!(log.contains("minio/mc:latest"));
    assert!(log.contains("logs -f"));
    assert!(log.contains("network inspect"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hash_suffixed_legacy_resources_are_adopted_on_upgrade() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("upgrade-corner-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();

    let instance = "14527b3cbdf37376ceb9eda41d2afac4";
    let config_path = project.join("helix.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("[local.dev]", &format!("[local.{instance}]")),
    )
    .unwrap();

    fixture
        .command()
        .current_dir(&project)
        .env("HELIX_TEST_RUNTIME_VOLUME_MODE", "existing")
        .args(["start", instance])
        .assert()
        .success();

    let legacy = "helix-upgrade-corner-project-14527b3cbdf37376ceb9eda41d2afac4";
    let log = fixture.runtime_log();
    assert!(
        log.contains(&format!("volume inspect {legacy}-minio-data")),
        "expected the legacy volume to be adopted, got: {log}"
    );
    assert!(
        !log.contains("volume create"),
        "an adopted volume must be reopened, not recreated, got: {log}"
    );
    assert!(
        log.contains(&format!("--name {legacy} -p")),
        "expected the legacy container name, got: {log}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_hash_suffixed_names_get_their_own_digest() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("upgrade-corner-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();

    let instance = "14527b3cbdf37376ceb9eda41d2afac4";
    let config_path = project.join("helix.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("[local.dev]", &format!("[local.{instance}]")),
    )
    .unwrap();

    fixture
        .command()
        .current_dir(&project)
        .env("HELIX_TEST_RUNTIME_LABEL_PROBE", "missing")
        .args(["start", instance])
        .assert()
        .success();

    let suffixed =
        "helix-upgrade-corner-project-14527b3cbdf37376ceb9eda41d2afac4-4d82fafccc46ce0a61a48599cc612258";
    let log = fixture.runtime_log();
    assert!(
        log.contains(&format!(
            "volume create --label helixdb.identity=22:upgrade-corner-project/14527b3cbdf37376ceb9eda41d2afac4 {suffixed}-minio-data"
        )),
        "expected a fresh labeled suffixed volume, got: {log}"
    );
    assert!(
        log.contains(&format!("--name {suffixed} -p")),
        "expected a fresh suffixed container, got: {log}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_labeled_resources_are_not_adopted_or_removed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let first = fixture.root().join("a b");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&first)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--no-skills"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&first)
        .args(["start", "dev"])
        .assert()
        .success();

    let second = fixture.root().join("a-b-dev");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&second)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--no-skills"])
        .assert()
        .success();

    let instance = "14527b3cbdf37376ceb9eda41d2afac4";
    let config_path = second.join("helix.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("[local.dev]", &format!("[local.{instance}]")),
    )
    .unwrap();

    fixture
        .command()
        .current_dir(&second)
        .env("HELIX_TEST_RUNTIME_LABEL_PROBE", "3:a b/dev")
        .args(["start", instance])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&second)
        .env("HELIX_TEST_RUNTIME_LABEL_PROBE", "3:a b/dev")
        .args(["stop", instance])
        .assert()
        .success();

    let legacy = "helix-a-b-dev-14527b3cbdf37376ceb9eda41d2afac4";
    let log = fixture.runtime_log();
    assert!(
        log.contains(&format!(
            "--name {legacy}-13e3b00b2c8ffd87792b25c1d1cf2aea -p"
        )),
        "the second identity must use its own suffixed name, got: {log}"
    );
    assert_eq!(
        log.matches(&format!("--name {legacy} -p")).count(),
        1,
        "the legacy run line must belong to the first identity only, got: {log}"
    );
    assert_eq!(
        log.matches(&format!("rm -f {legacy}\n")).count(),
        1,
        "the first identity's resources must not be removed again, got: {log}"
    );
    assert!(
        log.contains(&format!(
            "rm -f {legacy}-13e3b00b2c8ffd87792b25c1d1cf2aea\n"
        )),
        "stopping the second identity must only remove its own resources, got: {log}"
    );
}

#[test]
fn logs_and_status_report_a_missing_runtime_like_stop_does() {
    let fixture = CliFixture::new().with_missing_runtime();
    let project = fixture.root().join("missing-runtime-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    for command in [["logs", "dev"], ["status", "dev"], ["stop", "dev"]] {
        let message = stderr(
            fixture
                .command()
                .current_dir(&project)
                .args(command)
                .assert()
                .failure(),
        );
        assert!(
            message.contains("Docker is not installed"),
            "`helix {}` should name the missing runtime, got: {message}",
            command.join(" ")
        );
        // The number is platform-specific: ENOENT is 2, while Windows reports 3
        // (ERROR_PATH_NOT_FOUND) when the parent directory is absent too. What
        // matters is that the originating error survives as the cause.
        assert!(
            message.contains("os error"),
            "`helix {}` should keep the underlying cause, got: {message}",
            command.join(" ")
        );
    }
}

/// A runtime that is present but refuses to launch is a different failure from
/// one that is not installed, and must not be reported as a missing install.
#[cfg(unix)]
#[test]
fn a_present_but_unspawnable_runtime_keeps_its_command_error() {
    let fixture = CliFixture::new().with_unspawnable_runtime();
    let project = fixture.root().join("unspawnable-runtime-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    for command in [["logs", "dev"], ["status", "dev"]] {
        let message = stderr(
            fixture
                .command()
                .current_dir(&project)
                .args(command)
                .assert()
                .failure(),
        );
        assert!(
            !message.contains("is not installed"),
            "`helix {}` should not blame the install for a permission failure, got: {message}",
            command.join(" ")
        );
    }
}
