mod support;

use assert_cmd::assert::Assert;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use support::CliFixture;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).expect("stdout should be utf8")
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

fn write_project(project: &Path, gateway_url: &str) {
    let queries = project.join("queries");
    fs::create_dir_all(queries.join("src")).unwrap();
    fs::write(
        project.join("helix.toml"),
        format!(
            r#"[project]
name = "sync-project"
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
    fs::write(
        queries.join("Cargo.toml"),
        "[package]\nname = \"queries\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    fs::write(
        queries.join("src/main.rs"),
        "fn main() { println!(\"local\"); }\n",
    )
    .unwrap();
    fs::write(queries.join("queries.json"), r#"{"queries":[]}"#).unwrap();
}

async fn mount_cluster(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/cli/projects/project-1/clusters"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "project_id":"project-1",
            "project_name":"sync-project",
            "enterprise":[{
                "cluster_id":"cluster-1",
                "name":"production",
                "project_id":"project-1",
                "gateway_url":server.uri()
            }]
        })))
        .expect(1)
        .mount(server)
        .await;
}

async fn mount_sync(server: &MockServer, body: Value) {
    Mock::given(method("GET"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/sync"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(server)
        .await;
}

async fn mount_deploy(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/deploy"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "s3_key":"sync/queries.json"
        })))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_reconciliation_covers_push_pull_divergence_fallback_and_errors() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new()
        .with_fake_tools()
        .with_http_base(server.uri());
    fixture.write_credentials("user-1", "admin-key");
    let project = fixture.root().join("sync-project");
    fs::create_dir_all(&project).unwrap();
    write_project(&project, &server.uri());

    mount_cluster(&server).await;
    mount_sync(&server, json!({})).await;
    mount_deploy(&server).await;
    let local_only = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["sync", "production", "--yes"])
            .assert()
            .success(),
    );
    assert!(local_only.contains("Cloud files to be created"));
    assert!(local_only.contains("Enterprise sync reconciliation applied"));

    server.reset().await;
    mount_cluster(&server).await;
    mount_sync(&server, json!({})).await;
    let validation_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["sync", "production", "--yes"])
            .env("HELIX_TEST_TOOL_FAIL_COMMAND", "run")
            .env("HELIX_TEST_TOOL_STDERR", "compile failed")
            .assert()
            .failure(),
    );
    assert!(validation_error.contains("failed validation"));
    assert!(validation_error.contains("compile failed"));

    fs::remove_file(project.join("queries/Cargo.toml")).unwrap();
    fs::remove_file(project.join("queries/src/main.rs")).unwrap();
    let remote_cargo = "[package]\nname = \"remote\"\nversion = \"0.1.0\"\n";
    let remote_main = "fn main() { println!(\"remote\"); }\n";
    server.reset().await;
    mount_cluster(&server).await;
    mount_sync(
        &server,
        json!({
            "source_files": {
                "Cargo.toml": remote_cargo,
                "src/main.rs": remote_main
            }
        }),
    )
    .await;
    let remote_only = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["sync", "production", "--yes"])
            .assert()
            .success(),
    );
    assert!(remote_only.contains("Local files to be created"));
    assert_eq!(
        fs::read_to_string(project.join("queries/src/main.rs")).unwrap(),
        remote_main
    );

    let newer_remote = "fn main() { println!(\"new remote\"); }\n";
    let future_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 60_000;
    server.reset().await;
    mount_cluster(&server).await;
    mount_sync(
        &server,
        json!({
            "source_files": {
                "Cargo.toml": remote_cargo,
                "src/main.rs": newer_remote
            },
            "file_metadata": {
                "src/main.rs": {"last_modified_ms": future_ms}
            }
        }),
    )
    .await;
    let remote_newer = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["sync", "production", "--yes"])
            .assert()
            .success(),
    );
    assert!(remote_newer.contains("Local files to be changed"));
    assert_eq!(
        fs::read_to_string(project.join("queries/src/main.rs")).unwrap(),
        newer_remote
    );

    let newer_local = "fn main() { println!(\"new local\"); }\n";
    fs::write(project.join("queries/src/main.rs"), newer_local).unwrap();
    server.reset().await;
    mount_cluster(&server).await;
    mount_sync(
        &server,
        json!({
            "source_files": {
                "Cargo.toml": remote_cargo,
                "src/main.rs": newer_remote
            },
            "file_metadata": {
                "src/main.rs": {"last_modified_ms": 1}
            }
        }),
    )
    .await;
    mount_deploy(&server).await;
    let local_newer = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["sync", "production", "--yes"])
            .assert()
            .success(),
    );
    assert!(local_newer.contains("Cloud files to be changed"));
    assert!(local_newer.contains("Enterprise cluster deployed successfully"));

    let tied_local = "fn main() { println!(\"tied local\"); }\n";
    fs::write(project.join("queries/src/main.rs"), tied_local).unwrap();
    let tied_ms = fs::metadata(project.join("queries/src/main.rs"))
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    server.reset().await;
    mount_cluster(&server).await;
    mount_sync(
        &server,
        json!({
            "source_files": {
                "Cargo.toml": remote_cargo,
                "src/main.rs": "fn main() { println!(\"tied remote\"); }\n"
            },
            "file_metadata": {
                "src/main.rs": {"last_modified_ms": tied_ms}
            }
        }),
    )
    .await;
    let tie = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["sync", "production", "--yes"])
            .assert()
            .success(),
    );
    assert!(tie.contains("near-simultaneous"));
    assert!(tie.contains("Left local and cloud changes unchanged"));

    server.reset().await;
    mount_cluster(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/sync"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    let fallback = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["sync", "production", "--dry-run"])
            .assert()
            .success(),
    );
    assert!(fallback.contains("Treating cloud changes as empty"));
    assert!(fallback.contains("would push"));

    for (status, expected) in [(401, "Authentication failed"), (403, "Access denied")] {
        server.reset().await;
        mount_cluster(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/cli/enterprise-clusters/cluster-1/sync"))
            .respond_with(ResponseTemplate::new(status))
            .expect(1)
            .mount(&server)
            .await;
        let error = stderr(
            fixture
                .command()
                .current_dir(&project)
                .args(["sync", "production", "--dry-run"])
                .assert()
                .failure(),
        );
        assert!(error.contains(expected), "{error}");
    }
}
