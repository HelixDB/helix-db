mod support;

use assert_cmd::assert::Assert;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use support::CliFixture;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).expect("stdout should be utf8")
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

#[test]
fn every_command_and_subcommand_renders_help_through_the_binary() {
    const HELP_CASES: &[&[&str]] = &[
        &["init", "--help"],
        &["init", "local", "--help"],
        &["init", "cloud", "--help"],
        &["chef", "--help"],
        &["add", "--help"],
        &["add", "local", "--help"],
        &["add", "cloud", "--help"],
        &["start", "--help"],
        &["stop", "--help"],
        &["restart", "--help"],
        &["status", "--help"],
        &["logs", "--help"],
        &["query", "--help"],
        &["push", "--help"],
        &["auth", "--help"],
        &["auth", "login", "--help"],
        &["auth", "logout", "--help"],
        &["auth", "create-key", "--help"],
        &["config", "--help"],
        &["config", "workspace", "list", "--help"],
        &["config", "project", "show", "--help"],
        &["config", "cluster", "indexes", "--help"],
        &["workspace", "--help"],
        &["workspace", "list", "--help"],
        &["workspace", "show", "--help"],
        &["workspace", "switch", "--help"],
        &["project", "--help"],
        &["project", "list", "--help"],
        &["project", "show", "--help"],
        &["project", "switch", "--help"],
        &["cluster", "--help"],
        &["cluster", "list", "--help"],
        &["cluster", "indexes", "--help"],
        &["sync", "--help"],
        &["prune", "--help"],
        &["delete", "--help"],
        &["skills", "--help"],
        &["skills", "install", "--help"],
        &["skills", "update", "--help"],
        &["skills", "list", "--help"],
        &["metrics", "--help"],
        &["metrics", "full", "--help"],
        &["metrics", "basic", "--help"],
        &["metrics", "off", "--help"],
        &["metrics", "status", "--help"],
        &["update", "--help"],
        &["feedback", "--help"],
        &["run", "--help"],
        &["cook", "--help"],
    ];

    let fixture = CliFixture::new();
    for args in HELP_CASES {
        let output = stdout(
            fixture
                .command()
                .args(args.iter().copied())
                .assert()
                .success(),
        );
        assert!(
            output.contains("Usage:"),
            "expected usage output for {args:?}, got: {output}"
        );
    }
}

#[test]
fn binary_parser_rejects_missing_inputs_and_conflicting_flags() {
    let fixture = CliFixture::new();

    for (args, expected) in [
        (
            &["query", "dev"][..],
            "required arguments were not provided",
        ),
        (
            &["query", "dev", "--json", "{}", "--file", "query.json"][..],
            "cannot be used with",
        ),
        (
            &["logs", "dev", "--start", "2026-01-01T00:00:00Z"][..],
            "--range",
        ),
        (
            &["init", "--skills", "--no-skills"][..],
            "cannot be used with",
        ),
        (
            &["start", "dev", "--disk", "--storage-uri", "s3://bucket"][..],
            "cannot be used with",
        ),
    ] {
        let error = stderr(
            fixture
                .command()
                .args(args.iter().copied())
                .assert()
                .failure(),
        );
        assert!(
            error.contains(expected),
            "expected {expected:?} for {args:?}, got: {error}"
        );
    }
}

#[test]
fn host_actions_auth_and_metrics_are_exercised_through_the_binary() {
    let fixture = CliFixture::new();

    let feedback = stdout(
        fixture
            .command()
            .args(["feedback", "works on Linux, macOS & Windows"])
            .assert()
            .success(),
    );
    assert!(feedback.contains("Opened feedback issue"));

    let updated = stdout(
        fixture
            .command()
            .args(["update", "--force", "--v1"])
            .env("HELIX_TEST_UPDATE_OUTCOME", "updated")
            .assert()
            .success(),
    );
    assert!(updated.contains("CLI updated successfully"));

    let unchanged = stdout(
        fixture
            .command()
            .arg("update")
            .env("HELIX_TEST_UPDATE_OUTCOME", "unchanged")
            .assert()
            .success(),
    );
    assert!(unchanged.contains("already up to date"));

    let update_error = stderr(
        fixture
            .command()
            .arg("update")
            .env("HELIX_TEST_UPDATE_OUTCOME", "error")
            .assert()
            .failure(),
    );
    assert!(update_error.contains("simulated CLI update failure"));

    let actions: Vec<Value> = fixture
        .host_actions()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actions[0]["type"], "open_url");
    assert!(actions[0]["url"]
        .as_str()
        .unwrap()
        .contains("works%20on%20Linux%2C%20macOS%20%26%20Windows"));
    assert_eq!(actions[1], json!({"type":"update","v1":true,"force":true}));
    assert_eq!(
        actions[2],
        json!({"type":"update","v1":false,"force":false})
    );
    assert_eq!(
        actions[3],
        json!({"type":"update","v1":false,"force":false})
    );

    fixture.write_credentials("user-1", "admin-key");
    fixture
        .command()
        .args(["auth", "logout"])
        .assert()
        .success();
    assert!(!fixture.home().join(".helix/credentials").exists());
    let logout_again = stdout(
        fixture
            .command()
            .args(["auth", "logout"])
            .assert()
            .success(),
    );
    assert!(logout_again.contains("Not currently logged in"));

    fixture
        .command()
        .args(["metrics", "basic", "--verbose"])
        .assert()
        .success();
    let basic_status = stdout(
        fixture
            .command()
            .args(["metrics", "status"])
            .assert()
            .success(),
    );
    assert!(basic_status.contains("Basic"));

    fixture
        .command()
        .args(["metrics", "full"])
        .write_stdin("cli@example.com\n")
        .assert()
        .success();
    let metrics = fs::read_to_string(fixture.helix_home().join("metrics.toml")).unwrap();
    assert!(metrics.contains("level = \"full\""));
    assert!(metrics.contains("email = \"cli@example.com\""));

    fixture
        .command()
        .args(["metrics", "off", "--quiet"])
        .assert()
        .success();
    let off_status = stdout(
        fixture
            .command()
            .args(["metrics", "status"])
            .assert()
            .success(),
    );
    assert!(off_status.contains("Off"));
}

#[test]
fn skills_commands_forward_scope_flags_and_surface_tool_failures() {
    let fixture = CliFixture::new().with_fake_tools();

    fixture
        .command()
        .current_dir(fixture.root())
        .args(["skills", "install", "--project"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(fixture.root())
        .args(["skills", "update"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(fixture.root())
        .args(["skills", "list", "--project"])
        .assert()
        .success();

    let log = fixture.tool_log();
    assert!(log.contains("npx skills add HelixDB/skills"));
    assert!(log.contains("npx -y skills add HelixDB/skills --skill * -y -g"));
    assert!(log.contains("npx -y skills list"));

    let error = stderr(
        fixture
            .command()
            .current_dir(fixture.root())
            .args(["skills", "list"])
            .env("HELIX_TEST_TOOL_FAIL_COMMAND", "-y")
            .env("HELIX_TEST_TOOL_STDERR", "npx failed")
            .assert()
            .failure(),
    );
    assert!(error.contains("Listing skills failed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typescript_query_inputs_execute_node_and_send_the_generated_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"count": 3})))
        .expect(2)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime().with_fake_tools();
    let project = fixture.root().join("typescript-query-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .arg("--no-skills")
        .assert()
        .success();

    let sdk = fixture
        .cache()
        .join("ts-runtime/node_modules/@helix-db/helix-db");
    fs::create_dir_all(&sdk).unwrap();
    fs::write(sdk.join("package.json"), "{}").unwrap();
    fs::write(fixture.cache().join("ts-runtime/.sdk-spec"), "^3.0.0").unwrap();

    let generated = json!({
        "request_type": "read",
        "query": {"queries": [], "returns": []},
        "parameters": {}
    })
    .to_string();
    let inline = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "-e", "readBatch()", "--compact"])
            .env("HELIX_TEST_TOOL_STDOUT", &generated)
            .assert()
            .success(),
    );
    assert!(inline.contains("\"count\":3"));

    let ts_file = project.join("query.ts");
    fs::write(&ts_file, "readBatch()").unwrap();
    fixture
        .command()
        .current_dir(&project)
        .args(["query", "dev", "--ts-file"])
        .arg(&ts_file)
        .env("HELIX_TEST_TOOL_STDOUT", &generated)
        .assert()
        .success();

    let log = fixture.tool_log();
    assert_eq!(
        log.lines().filter(|line| line.starts_with("node ")).count(),
        2
    );
    assert!(!log.contains("npm install"));

    let missing_node = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "-e", "readBatch()"])
            .env("HELIX_TEST_TOOL_EXIT_CODE", "1")
            .assert()
            .failure(),
    );
    assert!(missing_node.contains("TypeScript query failed to evaluate"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_start_and_restart_use_the_runtime_and_readiness_contract() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(2)
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("runtime-command-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .arg("--no-skills")
        .assert()
        .success();

    let start = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["start", "dev"])
            .assert()
            .success(),
    );
    assert!(start.contains("URL"));
    assert!(start.contains(&server.address().port().to_string()));

    fixture
        .command()
        .current_dir(&project)
        .args(["restart", "dev"])
        .assert()
        .success();

    let runtime_log = fixture.runtime_log();
    assert!(runtime_log.lines().any(|line| line.starts_with("pull ")));
    assert!(runtime_log.lines().any(|line| line.starts_with("run ")));
    assert!(runtime_log.lines().any(|line| line.starts_with("restart ")));

    let failure = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["start", "dev"])
            .env("HELIX_TEST_RUNTIME_FAIL_COMMAND", "run")
            .assert()
            .failure(),
    );
    assert!(failure.contains("simulated runtime failure"));
}

fn write_enterprise_project(project: &Path, gateway_url: &str) -> (String, String) {
    let queries = project.join("queries");
    fs::create_dir_all(queries.join("src")).unwrap();
    fs::write(
        project.join("helix.toml"),
        format!(
            r#"[project]
name = "enterprise-project"
queries = "queries"
id = "project-1"
workspace_id = "workspace-1"

[enterprise.production]
cluster_id = "cluster-1"
workspace_id = "workspace-1"
project_id = "project-1"
gateway_url = "{gateway_url}"
query_auth_header = "x-api-key"
query_auth_env = "HELIX_API_KEY"
min_instances = 1
max_instances = 1
"#
        ),
    )
    .unwrap();
    let cargo_toml = "[package]\nname = \"queries\"\nversion = \"0.1.0\"\n".to_string();
    let main_rs = "fn main() {}\n".to_string();
    fs::write(queries.join("Cargo.toml"), &cargo_toml).unwrap();
    fs::write(queries.join("src/main.rs"), &main_rs).unwrap();
    fs::write(queries.join("queries.json"), r#"{"queries":[]}"#).unwrap();
    (cargo_toml, main_rs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enterprise_push_and_sync_cover_success_and_service_errors() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new()
        .with_fake_tools()
        .with_http_base(server.uri());
    fixture.write_credentials("user-1", "admin-key");
    let project = fixture.root().join("enterprise-project");
    fs::create_dir_all(&project).unwrap();
    let (cargo_toml, main_rs) = write_enterprise_project(&project, &server.uri());

    Mock::given(method("POST"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/deploy"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"s3_key":"deploy/queries.json"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let push = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["push", "production", "--dev"])
            .assert()
            .success(),
    );
    assert!(push.contains("Enterprise cluster deployed successfully"));
    assert!(push.contains("Uploaded queries.json to deploy/queries.json"));
    let requests = server.received_requests().await.unwrap();
    let deploy_body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(deploy_body["instance_name"], "production");
    assert_eq!(deploy_body["queries_json_size_bytes"], 14);
    assert_eq!(deploy_body["source_files"]["src/main.rs"], main_rs);
    assert!(fixture.tool_log().contains("cargo run --manifest-path"));

    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/deploy"))
        .respond_with(ResponseTemplate::new(500).set_body_string("deploy unavailable"))
        .expect(1)
        .mount(&server)
        .await;
    let push_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["push", "production"])
            .assert()
            .failure(),
    );
    assert!(push_error.contains("Enterprise deploy of 'production' failed"));
    assert!(push_error.contains("500 Internal Server Error"));

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/api/cli/projects/project-1/clusters"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "project_id":"project-1",
            "project_name":"enterprise-project",
            "enterprise":[{
                "cluster_id":"cluster-1",
                "name":"production",
                "project_id":"project-1",
                "gateway_url":server.uri()
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/sync"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "source_files": {
                "Cargo.toml": cargo_toml,
                "src/main.rs": main_rs
            },
            "file_metadata": {}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let sync = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["sync", "production", "--dry-run"])
            .assert()
            .success(),
    );
    assert!(sync.contains("already in sync"));
    assert!(sync.contains("Dry run: no changes were made"));

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/sync"))
        .respond_with(ResponseTemplate::new(503).set_body_string("sync unavailable"))
        .expect(1)
        .mount(&server)
        .await;
    let sync_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["sync", "production", "--dry-run"])
            .assert()
            .failure(),
    );
    assert!(sync_error.contains("Enterprise sync failed"));
    assert!(sync_error.contains("sync unavailable"));
}
