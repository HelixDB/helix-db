mod support;

use assert_cmd::assert::Assert;
use serde_json::Value as JsonValue;
use std::fs;
use toml::Value as TomlValue;

use support::{free_port, CliFixture};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
        Some("ghcr.io/helixdb/helixdb")
    );
    assert_eq!(config["local"]["dev"]["tag"].as_str(), Some("v0.0.4"));

    let gitignore = fs::read_to_string(project.join(".gitignore")).unwrap();
    assert!(gitignore.lines().any(|line| line == ".helix/"));
    assert!(gitignore.lines().any(|line| line == ".env"));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn documented_quickstart_runs_against_an_isolated_fixture() {
    const QUICKSTART: &str =
        include_str!("../../../docs/database/helix-db/start-here/quickstart.mdx");
    let documented_commands: Vec<&str> = QUICKSTART
        .lines()
        .filter(|line| line.starts_with("helix "))
        .collect();
    assert_eq!(
        documented_commands,
        [
            "helix init local",
            "helix start dev",
            "helix query dev --file examples/request.json",
            "helix stop dev",
        ]
    );
    assert!(QUICKSTART.contains("- A terminal on macOS or Linux, or PowerShell on Windows"));
    assert!(QUICKSTART.contains(
        "irm https://raw.githubusercontent.com/HelixDB/helix-db/main/crates/cli/install.ps1 | iex"
    ));
    assert!(QUICKSTART.contains("container_runtime = \"podman\""));
    for obsolete_claim in [
        "--lang",
        "helix start local",
        "queries.rs",
        "helix dashboard",
    ] {
        assert!(!QUICKSTART.contains(obsolete_claim), "{obsolete_claim}");
    }

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("quickstart-project");
    fs::create_dir_all(&project).unwrap();

    fixture
        .command()
        .current_dir(&project)
        .args(["init", "local"])
        .assert()
        .success();

    for generated_path in [
        "helix.toml",
        ".helix",
        ".gitignore",
        "AGENTS.md",
        "examples/request.json",
    ] {
        assert!(project.join(generated_path).exists(), "{generated_path}");
    }

    let config_path = project.join("helix.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    assert!(config.contains("[local.dev]"));
    assert!(config.contains("port = 6969"));
    fs::write(
        &config_path,
        config.replacen(
            "port = 6969",
            &format!("port = {}", server.address().port()),
            1,
        ),
    )
    .unwrap();

    let request: JsonValue =
        serde_json::from_str(&fs::read_to_string(project.join("examples/request.json")).unwrap())
            .unwrap();
    Mock::given(method("POST"))
        .and(path("/v2/query"))
        .and(body_json(&request))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "node_count": 0
        })))
        .expect(1)
        .mount(&server)
        .await;

    fixture
        .command()
        .current_dir(&project)
        .args(["start", "dev"])
        .assert()
        .success();

    let query = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["query", "dev", "--file", "examples/request.json"])
            .assert()
            .success(),
    );
    assert!(query.contains("\"node_count\": 0"), "{query}");

    fixture
        .command()
        .current_dir(&project)
        .args(["stop", "dev"])
        .env("HELIX_TEST_RUNTIME_RESOURCES_EXIST", "1")
        .assert()
        .success();
}

#[test]
fn s3_local_config_is_written_by_init_add_and_start_persist() {
    let fixture = CliFixture::new_with_fake_runtime();
    let init_project = fixture.root().join("s3-init-project");

    fixture
        .command()
        .args(["init", "--path"])
        .arg(&init_project)
        .args([
            "local",
            "--name",
            "dev",
            "--storage-uri",
            "s3://init-bucket/app-data",
            "--s3-region",
            "eu-west-2",
            "--s3-endpoint-url",
            "https://s3.example.com",
            "--no-skills",
        ])
        .assert()
        .success();

    let config: TomlValue =
        toml::from_str(&fs::read_to_string(init_project.join("helix.toml")).unwrap()).unwrap();
    assert_eq!(config["local"]["dev"]["storage"].as_str(), Some("s3"));
    assert_eq!(
        config["local"]["dev"]["s3"]["bucket"].as_str(),
        Some("init-bucket")
    );
    assert_eq!(
        config["local"]["dev"]["s3"]["prefix"].as_str(),
        Some("app-data/")
    );
    assert_eq!(
        config["local"]["dev"]["s3"]["region"].as_str(),
        Some("eu-west-2")
    );
    assert_eq!(
        config["local"]["dev"]["s3"]["endpoint_url"].as_str(),
        Some("https://s3.example.com")
    );

    fixture
        .command()
        .current_dir(&init_project)
        .args([
            "add",
            "local",
            "--name",
            "qa",
            "--storage-uri",
            "s3://qa-bucket",
        ])
        .assert()
        .success();

    let config: TomlValue =
        toml::from_str(&fs::read_to_string(init_project.join("helix.toml")).unwrap()).unwrap();
    assert_eq!(config["local"]["qa"]["storage"].as_str(), Some("s3"));
    assert_eq!(
        config["local"]["qa"]["s3"]["bucket"].as_str(),
        Some("qa-bucket")
    );
    assert_eq!(config["local"]["qa"]["s3"]["prefix"].as_str(), Some("db/"));

    fixture
        .command()
        .current_dir(&init_project)
        .args([
            "start",
            "dev",
            "--foreground",
            "--storage-uri",
            "s3://start-bucket/new-prefix",
            "--s3-region",
            "us-west-2",
            "--persist",
        ])
        .assert()
        .success();

    let config: TomlValue =
        toml::from_str(&fs::read_to_string(init_project.join("helix.toml")).unwrap()).unwrap();
    assert_eq!(
        config["local"]["dev"]["s3"]["bucket"].as_str(),
        Some("start-bucket")
    );
    assert_eq!(
        config["local"]["dev"]["s3"]["prefix"].as_str(),
        Some("new-prefix/")
    );
    assert_eq!(
        config["local"]["dev"]["s3"]["region"].as_str(),
        Some("us-west-2")
    );
}

#[test]
fn local_runtime_commands_have_cross_platform_no_daemon_smoke_coverage() {
    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("local-command-project");

    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    let status = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["status", "dev"])
            .assert()
            .success(),
    );
    assert!(status.contains("dev (local)"));
    assert!(status.contains("not created"));

    let logs = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["logs", "dev"])
            .assert()
            .success(),
    );
    assert!(logs.contains("fake logs"));

    let stop = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["stop", "dev"])
            .assert()
            .success(),
    );
    assert!(stop.contains("was not running"));

    let prune = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["prune", "dev"])
            .assert()
            .success(),
    );
    assert!(prune.contains("No local runtime resources found"));

    let mut config_text = fs::read_to_string(project.join("helix.toml")).unwrap();
    config_text.push_str(
        r#"

[enterprise.production]
cluster_id = "cluster-test"
gateway_url = "http://127.0.0.1:9999"
"#,
    );
    fs::write(project.join("helix.toml"), config_text).unwrap();

    let start_cloud = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["start", "production"])
            .assert()
            .failure(),
    );
    assert!(start_cloud.contains("'production' is not a local v2 instance"));

    let restart_cloud = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["restart", "production"])
            .assert()
            .failure(),
    );
    assert!(restart_cloud.contains("'production' is not a local v2 instance"));

    fixture
        .command()
        .current_dir(&project)
        .args(["delete", "dev", "--yes"])
        .assert()
        .success();

    let config_text = fs::read_to_string(project.join("helix.toml")).unwrap();
    let config: TomlValue = toml::from_str(&config_text).unwrap();
    assert!(config
        .get("local")
        .and_then(TomlValue::as_table)
        .map(|local| local.is_empty())
        .unwrap_or(true));
    assert!(config["enterprise"]["production"].is_table());
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

#[test]
fn default_instance_and_noninteractive_error_branches_run_through_the_binary() {
    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("default-instance-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    let local_push = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["push", "dev"])
            .assert()
            .failure(),
    );
    assert!(local_push.contains("uses the v2 runtime"));
    let missing_cloud = stderr(
        fixture
            .command()
            .current_dir(&project)
            .arg("push")
            .assert()
            .failure(),
    );
    assert!(missing_cloud.contains("No Enterprise instances found"));

    fixture
        .command()
        .current_dir(&project)
        .args(["start", "--foreground"])
        .assert()
        .success();
    let stop = stdout(
        fixture
            .command()
            .current_dir(&project)
            .arg("stop")
            .assert()
            .success(),
    );
    assert!(stop.contains("was not running"));
    fixture
        .command()
        .current_dir(&project)
        .arg("status")
        .assert()
        .success();

    fs::create_dir_all(project.join(".helix/dev/transient")).unwrap();
    fixture
        .command()
        .current_dir(&project)
        .args(["prune", "dev"])
        .assert()
        .success();
    assert!(!project.join(".helix/dev").exists());
    let missing_prune_target = stderr(
        fixture
            .command()
            .current_dir(&project)
            .arg("prune")
            .assert()
            .failure(),
    );
    assert!(missing_prune_target.contains("Specify a local instance"));
    let unconfirmed_prune = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["prune", "--all"])
            .assert()
            .failure(),
    );
    assert!(unconfirmed_prune.contains("Re-run with --yes"));

    let mut config = fs::read_to_string(project.join("helix.toml")).unwrap();
    config.push_str(
        r#"

[enterprise.production]
cluster_id = "cluster-1"
gateway_url = "https://primary.example.com"

[enterprise.staging]
cluster_id = "cluster-2"
gateway_url = "https://staging.example.com"
"#,
    );
    fs::write(project.join("helix.toml"), config).unwrap();
    let all_status = stdout(
        fixture
            .command()
            .current_dir(&project)
            .arg("status")
            .assert()
            .success(),
    );
    assert!(all_status.contains("production (Enterprise)"));
    assert!(all_status.contains("staging (Enterprise)"));
    let ambiguous_push = stderr(
        fixture
            .command()
            .current_dir(&project)
            .arg("push")
            .assert()
            .failure(),
    );
    assert!(ambiguous_push.contains("Available Enterprise instances"));

    fixture
        .command()
        .current_dir(&project)
        .args(["delete", "dev", "--yes"])
        .assert()
        .success();
    for command in ["start", "stop", "restart"] {
        let error = stderr(
            fixture
                .command()
                .current_dir(&project)
                .arg(command)
                .assert()
                .failure(),
        );
        assert!(error.contains("No local instance specified"), "{error}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_start_reuses_an_existing_volume_without_creating_it() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let fixture = CliFixture::new_with_fake_runtime();
    let project = fixture.root().join("existing-volume-project");
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
        .env("HELIX_TEST_RUNTIME_VOLUME_MODE", "existing")
        .assert()
        .success();

    // an existing volume is inspected and reused, never re-created.
    let log = fixture.runtime_log();
    assert!(log.contains("volume inspect"), "{log}");
    assert!(!log.contains("volume create"), "{log}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_start_tolerates_the_volume_create_race_but_surfaces_real_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    // raced: inspect misses, create loses to another process, start still succeeds.
    let raced = CliFixture::new_with_fake_runtime();
    let raced_project = raced.root().join("raced-volume-project");
    raced
        .command()
        .args(["init", "--path"])
        .arg(&raced_project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();
    raced
        .command()
        .current_dir(&raced_project)
        .args(["start", "dev"])
        .env("HELIX_TEST_RUNTIME_VOLUME_MODE", "raced")
        .assert()
        .success();

    // denied: an unrelated create failure must fail the start with the real error.
    let denied = CliFixture::new_with_fake_runtime();
    let denied_project = denied.root().join("denied-volume-project");
    denied
        .command()
        .args(["init", "--path"])
        .arg(&denied_project)
        .args(["local", "--port"])
        .arg(server.address().port().to_string())
        .args(["--disk", "--no-skills"])
        .assert()
        .success();
    let error = stderr(
        denied
            .command()
            .current_dir(&denied_project)
            .args(["start", "dev"])
            .env("HELIX_TEST_RUNTIME_VOLUME_MODE", "denied")
            .assert()
            .failure(),
    );
    assert!(error.contains("permission denied"), "{error}");
}
