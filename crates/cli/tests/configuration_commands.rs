mod support;

use assert_cmd::assert::Assert;
use serde_json::{json, Value};
use std::fs;
use support::CliFixture;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn stdout(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stdout.clone()).expect("stdout should be utf8")
}

fn stderr(assert: Assert) -> String {
    String::from_utf8(assert.get_output().stderr.clone()).expect("stderr should be utf8")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configuration_actions_cover_state_human_json_and_selector_errors() {
    let server = MockServer::start().await;
    let fixture = CliFixture::new_with_fake_runtime().with_http_base(server.uri());
    fixture.write_credentials("user-1", "admin-key");
    let project = fixture.root().join("configuration-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&project)
        .args(["local", "--no-skills"])
        .assert()
        .success();

    Mock::given(method("GET"))
        .and(path("/api/cli/workspaces"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id":"ws-1","name":"Acme","url_slug":"acme"},
            {"id":"ws-2","name":"Beta","url_slug":"beta"}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/cli/workspaces/ws-1/projects"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id":"project-1","name":"Graph App"},
            {"id":"project-2","name":"Search App"}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/cli/workspaces/ws-1/clusters"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "enterprise":[{
                "cluster_id":"cluster-1",
                "name":"Primary",
                "project_id":"project-1",
                "project_name":"Graph App",
                "gateway_url":server.uri()
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/cli/projects/project-1/clusters"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "project_id":"project-1",
            "project_name":"Graph App",
            "enterprise":[{
                "cluster_id":"cluster-1",
                "name":"Primary",
                "project_id":"project-1",
                "project_name":"Graph App",
                "gateway_url":server.uri()
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/project"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "cluster_id":"cluster-1",
            "project_id":"project-1",
            "project_name":"Graph App",
            "workspace_id":"ws-1"
        })))
        .mount(&server)
        .await;

    let workspaces = stdout(
        fixture
            .command()
            .args(["config", "workspace", "list"])
            .assert()
            .success(),
    );
    assert!(workspaces.contains("Acme (acme)"));
    fixture
        .command()
        .args(["workspace", "switch", "acme"])
        .assert()
        .success();
    let workspace = stdout(
        fixture
            .command()
            .args(["workspace", "show"])
            .assert()
            .success(),
    );
    assert!(workspace.contains("Selected workspace: ws-1"));
    fixture
        .command()
        .args(["workspace", "switch", "ws-1", "--id"])
        .assert()
        .success();
    let workspace_error = stderr(
        fixture
            .command()
            .args(["workspace", "switch", "missing"])
            .assert()
            .failure(),
    );
    assert!(workspace_error.contains("Workspace 'missing' was not found"));

    let projects = stdout(
        fixture
            .command()
            .args(["project", "list"])
            .assert()
            .success(),
    );
    assert!(projects.contains("Graph App (project-1)"));
    fixture
        .command()
        .current_dir(&project)
        .args(["project", "switch", "Graph App"])
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args(["project", "switch", "project-1", "--id"])
        .assert()
        .success();
    let project_show = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["config", "project", "show"])
            .assert()
            .success(),
    );
    assert!(project_show.contains("Project: Graph App"));
    assert!(project_show.contains("Workspace ID: ws-1"));
    let project_error = stderr(
        fixture
            .command()
            .current_dir(&project)
            .args(["project", "switch", "missing"])
            .assert()
            .failure(),
    );
    assert!(project_error.contains("Project 'missing' was not found"));

    let workspace_clusters = stdout(
        fixture
            .command()
            .args(["cluster", "list", "--workspace-id", "ws-1"])
            .assert()
            .success(),
    );
    assert!(workspace_clusters.contains("Primary (cluster-1)"));
    assert!(workspace_clusters.contains(&server.uri()));
    let project_clusters = stdout(
        fixture
            .command()
            .args([
                "config",
                "cluster",
                "list",
                "--project-id",
                "project-1",
                "--format",
                "json",
            ])
            .assert()
            .success(),
    );
    let project_clusters: Value = serde_json::from_str(&project_clusters).unwrap();
    assert_eq!(project_clusters[0]["cluster_id"], "cluster-1");

    let index_response = json!({
        "mode":"standard",
        "readable_backends":["writer-0","reader-0"],
        "backends":[
            {
                "pod":"writer-0",
                "snapshot":{
                    "node_label_index":true,
                    "edge_label_index":true,
                    "node_equality_indexes":[["User","email"]],
                    "node_range_indexes":[["User","age"]],
                    "node_text_indexes":[["User","bio"]],
                    "edge_vector_indexes":[["Knows","embedding"]]
                }
            },
            {"pod":"reader-0","snapshot":{}}
        ],
        "writer_backend":{"pod":"writer-0"},
        "errors":[{"pod":"reader-1","error":"not ready"}]
    });
    Mock::given(method("GET"))
        .and(path("/api/cli/enterprise-clusters/cluster-1/indexes"))
        .and(header("x-api-key", "admin-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&index_response))
        .mount(&server)
        .await;

    let indexes = stdout(
        fixture
            .command()
            .args(["cluster", "indexes", "--cluster-id", "cluster-1"])
            .assert()
            .success(),
    );
    assert!(indexes.contains("mode: standard"));
    assert!(indexes.contains("Node:  label index enabled"));
    assert!(indexes.contains("Errors:"));
    assert!(indexes.contains("reader-0"));
    let indexes_json = stdout(
        fixture
            .command()
            .args([
                "cluster",
                "indexes",
                "--cluster-id",
                "cluster-1",
                "--format",
                "json",
            ])
            .assert()
            .success(),
    );
    assert_eq!(
        serde_json::from_str::<Value>(&indexes_json).unwrap(),
        index_response
    );

    let cloud_project = fixture.root().join("cloud-project");
    fixture
        .command()
        .args(["init", "--path"])
        .arg(&cloud_project)
        .args([
            "cloud",
            "--name",
            "production",
            "--cluster-id",
            "cluster-1",
            "--gateway-url",
        ])
        .arg(server.uri())
        .arg("--no-skills")
        .assert()
        .success();
    fixture
        .command()
        .current_dir(&project)
        .args([
            "add",
            "cloud",
            "--name",
            "staging",
            "--cluster-id",
            "cluster-1",
            "--gateway-url",
        ])
        .arg(server.uri())
        .assert()
        .success();
    let config = fs::read_to_string(project.join("helix.toml")).unwrap();
    assert!(config.contains("[enterprise.staging]"));

    let resolved_indexes = stdout(
        fixture
            .command()
            .current_dir(&project)
            .args(["cluster", "indexes"])
            .assert()
            .success(),
    );
    assert!(resolved_indexes.contains("Cluster: cluster-1"));

    for args in [
        &["config"][..],
        &["workspace"][..],
        &["project"][..],
        &["cluster"][..],
    ] {
        fixture
            .command()
            .current_dir(&project)
            .args(args.iter().copied())
            .assert()
            .failure();
    }
}
