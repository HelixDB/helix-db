mod support;

use assert_cmd::assert::Assert;
use serde_json::json;
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
async fn headless_chef_runs_setup_and_surfaces_external_tool_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok":true})))
        .expect(2)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime().with_fake_tools();
    let project = fixture.home().join("my-first-helix-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .arg("--no-skills")
        .assert()
        .success();
    fs::create_dir_all(project.join("web")).unwrap();
    fs::write(project.join("web/package.json"), "{}").unwrap();

    let chef = stdout(
        fixture
            .command()
            .current_dir(fixture.root())
            .arg("chef")
            .env("HELIX_SKIP_CLOUD_AUTH", "1")
            .env("HELIX_TEST_CHEF_PERMISSION_MODE", "full_auto")
            .env(
                "HELIX_TEST_TOOL_STDOUT",
                r#"{"type":"result","is_error":false,"duration_ms":1200,"total_cost_usd":0.125,"result":"Built successfully"}"#,
            )
            .assert()
            .success(),
    );
    assert!(chef.contains("without Helix Cloud auth"));
    assert!(chef.contains("Built successfully"));
    assert!(project.join("HELIX_CHEF_PROMPT.md").exists());
    assert!(project.join("DESIGN.md").exists());
    assert!(project.join("examples/seed.json").exists());
    assert!(project.join("examples/read_users.json").exists());
    let tools = fixture.tool_log();
    assert!(tools.contains("npx -y skills add HelixDB/skills"));
    assert!(tools.contains("npx -y add-mcp"));
    assert!(tools.contains("claude --append-system-prompt-file"));
    assert!(tools.contains("curl -fsSI -m 1 http://localhost:3000"));
    assert!(fixture
        .runtime_log()
        .lines()
        .any(|line| line.starts_with("run ")));
    assert!(fixture.host_actions().contains("http://localhost:3000"));

    let failure = stderr(
        fixture
            .command()
            .current_dir(fixture.root())
            .arg("chef")
            .env("HELIX_SKIP_CLOUD_AUTH", "1")
            .env("HELIX_TEST_TOOL_FAIL_COMMAND", "-y")
            .env("HELIX_TEST_TOOL_STDERR", "skills unavailable")
            .assert()
            .failure(),
    );
    assert!(failure.contains("Installing Helix skills failed"));

    let prompt = fs::read_to_string(project.join("HELIX_CHEF_PROMPT.md")).unwrap();
    assert!(prompt.contains("# HelixDB MVP Builder"));
    assert!(prompt.contains("Personal CRM"));
}
