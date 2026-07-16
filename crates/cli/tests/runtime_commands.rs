mod support;

use assert_cmd::assert::Assert;
use serde_json::json;
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
    Mock::given(method("POST"))
        .and(path("/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
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
