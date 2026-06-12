mod support;

use assert_cmd::assert::Assert;
use serde_json::Value as JsonValue;
use std::fs;
use toml::Value as TomlValue;

use support::{CliFixture, free_port};

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).expect("stdout should be utf8")
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

#[test]
fn top_level_binary_commands_render_help_and_version() {
    let fixture = CliFixture::new();

    let version = stdout(fixture.command().arg("--version").assert().success());
    assert!(version.contains(env!("CARGO_PKG_VERSION")));

    let help = stdout(fixture.command().arg("help").assert().success());
    assert!(help.contains("Usage: helix [OPTIONS] <COMMAND>"));
    assert!(help.contains("Local development"));
    assert!(help.contains("Helix Cloud"));

    let query_help = stdout(
        fixture
            .command()
            .args(["query", "--help"])
            .assert()
            .success(),
    );
    assert!(query_help.contains("Examples:"));
    assert!(query_help.contains("Input (pick one):"));
    assert!(query_help.contains("Connection:"));
}

#[test]
fn removed_commands_return_friendly_errors() {
    let fixture = CliFixture::new();

    let compile = stderr(fixture.command().arg("compile").assert().failure());
    assert!(compile.contains("`helix compile` is not a command"));
    assert!(compile.contains("there is no compile/check step"));

    let check = stderr(
        fixture
            .command()
            .args(["check", "queries/", "--path", "x"])
            .assert()
            .failure(),
    );
    assert!(check.contains("`helix check` is not a command"));
    assert!(check.contains("helix query <instance> --file"));

    let deploy = stderr(fixture.command().arg("deploy").assert().failure());
    assert!(deploy.contains("`helix deploy` is not a command"));
    assert!(deploy.contains("helix push <instance>"));
}

#[test]
fn init_and_add_generate_expected_project_files() {
    let fixture = CliFixture::new();
    let project = fixture.root().join("sample-project");
    let dev_port = free_port();
    let qa_port = free_port();

    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--name", "dev", "--port"])
        .arg(dev_port.to_string())
        .arg("--no-skills")
        .assert()
        .success();

    assert!(project.join("helix.toml").exists());
    assert!(project.join(".helix").is_dir());
    assert!(project.join(".gitignore").exists());
    assert!(project.join("examples/request.json").exists());

    let config_text = fs::read_to_string(project.join("helix.toml")).unwrap();
    let config: TomlValue = toml::from_str(&config_text).unwrap();
    assert_eq!(config["project"]["name"].as_str(), Some("sample-project"));
    assert_eq!(
        config["local"]["dev"]["port"].as_integer(),
        Some(dev_port.into())
    );
    assert_eq!(
        config["local"]["dev"]["image"].as_str(),
        Some("ghcr.io/helixdb/enterprise-dev")
    );
    assert_eq!(config["local"]["dev"]["tag"].as_str(), Some("latest"));

    let gitignore = fs::read_to_string(project.join(".gitignore")).unwrap();
    assert!(gitignore.lines().any(|line| line == ".helix/"));
    assert!(gitignore.lines().any(|line| line == "target/"));
    assert!(gitignore.lines().any(|line| line == "*.log"));

    let request: JsonValue =
        serde_json::from_str(&fs::read_to_string(project.join("examples/request.json")).unwrap())
            .unwrap();
    assert_eq!(request["request_type"].as_str(), Some("read"));
    assert!(request.get("query").is_some());

    fixture
        .command()
        .current_dir(&project)
        .args(["add", "local", "--name", "qa", "--port"])
        .arg(qa_port.to_string())
        .arg("--disk")
        .assert()
        .success();

    let config: TomlValue =
        toml::from_str(&fs::read_to_string(project.join("helix.toml")).unwrap()).unwrap();
    assert_eq!(
        config["local"]["qa"]["port"].as_integer(),
        Some(qa_port.into())
    );
    assert_eq!(config["local"]["qa"]["storage"].as_str(), Some("disk"));
}

#[test]
fn project_and_metrics_commands_use_isolated_state() {
    let fixture = CliFixture::new();
    let project = fixture.root().join("state-project");

    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    let project_json = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["project", "show", "--format", "json"])
            .assert()
            .success(),
    );
    let project_config: JsonValue = serde_json::from_str(&project_json).unwrap();
    assert_eq!(project_config["name"].as_str(), Some("state-project"));

    fixture
        .command()
        .args(["metrics", "off"])
        .assert()
        .success();
    let metrics_status = stdout(
        fixture
            .command()
            .args(["metrics", "status"])
            .assert()
            .success(),
    );
    assert!(metrics_status.contains("Metrics Level"));
    assert!(metrics_status.contains("Off"));
}

#[test]
fn query_preflight_errors_do_not_need_running_runtime() {
    let fixture = CliFixture::new();
    let project = fixture.root().join("query-project");

    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    let invalid_json = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "--json", "{"])
            .assert()
            .failure(),
    );
    assert!(invalid_json.contains("Failed to parse query request JSON"));

    let write_request =
        r#"{"request_type":"write","query":{"queries":[],"returns":[]},"parameters":{}}"#;
    let warm_write = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "--json", write_request, "--warm"])
            .assert()
            .failure(),
    );
    assert!(warm_write.contains("--warm is only valid for read requests"));
}

#[test]
fn cloud_config_smoke_without_credentials() {
    let fixture = CliFixture::new();

    let workspace_show = stdout(
        fixture
            .command()
            .args(["workspace", "show", "--format", "json"])
            .assert()
            .success(),
    );
    let workspace: JsonValue = serde_json::from_str(&workspace_show).unwrap();
    assert!(workspace["workspace_id"].is_null());

    let workspace_list = stderr(
        fixture
            .command()
            .args(["workspace", "list", "--format", "json"])
            .assert()
            .failure(),
    );
    assert!(workspace_list.contains("Authentication required"));
    assert!(workspace_list.contains("helix auth login"));
}
