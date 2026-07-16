mod support;

use assert_cmd::assert::Assert;
use serde_json::json;
use std::fs;
use support::CliFixture;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typescript_runtime_install_covers_npm_success_and_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok":true})))
        .expect(1)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime().with_fake_tools();
    let project = fixture.root().join("typescript-install-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .arg("--no-skills")
        .assert()
        .success();

    let generated = json!({
        "request_type":"read",
        "query":{"queries":[],"returns":[]},
        "parameters":{}
    })
    .to_string();
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "-e", "readBatch()"])
        .env("HELIX_TEST_TOOL_STDOUT", &generated)
        .assert()
        .success();
    assert!(fixture
        .tool_log()
        .contains("npm install --silent --no-audit --no-fund"));
    assert_eq!(
        fs::read_to_string(fixture.cache().join("ts-runtime/.sdk-spec")).unwrap(),
        "^3.0.0"
    );

    fs::remove_dir_all(fixture.cache().join("ts-runtime")).unwrap();
    let install_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "-e", "readBatch()"])
            .env("HELIX_TEST_TOOL_FAIL_COMMAND", "install")
            .env("HELIX_TEST_TOOL_STDERR", "registry unavailable")
            .assert()
            .failure(),
    );
    assert!(install_error.contains("failed to install the TypeScript query runtime"));
    assert!(install_error.contains("registry unavailable"));
}
